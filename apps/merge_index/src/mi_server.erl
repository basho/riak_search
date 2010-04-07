%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at

%%   http://www.apache.org/licenses/LICENSE-2.0

%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(mi_server).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").

-export([
    %% GEN SERVER
    init/1, 
    handle_call/3, 
    handle_cast/2, 
    handle_info/2, 
    terminate/2, 
    code_change/3
]).

init([Root, Config]) ->
    %% Create the state...
    State = #state {
        root = Root,
        indexes  = mi_incdex:open(join(Root, "indexes")),
        fields   = mi_incdex:open(join(Root, "fields")),
        terms    = mi_incdex:open(join(Root, "terms")),
        buffer   = mi_buffer:open(join(Root, "buffer")),
        segments = read_segments(Root),
        last_merge = now(),
        merge_pid = undefined,
        config = Config
    },
    
    %% Return.
    {ok, State}.

%% Return a list of segments. We will merge new data into the first
%% segment in the list.
read_segments(Root) ->
    lists:reverse(read_segments(Root, 1)).
read_segments(Root, N) ->
    SegmentFile = join(Root, "segment." ++ integer_to_list(N)),
    case mi_segment:exists(SegmentFile) of
        true -> 
            [mi_segment:open(SegmentFile)|read_segments(Root, N + 1)];
        false ->
            []
    end.

handle_call({index, Index, Field, Term, SubType, SubTerm, Value, Props, NowTime}, _From, State) ->
    %% Calculate the IFT...
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffer=Buffer, segments=Segments } = State,
    {IndexID, NewIndexes} = mi_incdex:lookup(Index, Indexes),
    {FieldID, NewFields} = mi_incdex:lookup(Field, Fields),
    {TermID, NewTerms} = mi_incdex:lookup(Term, Terms),
    IFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, SubTerm),
    
    %% Write to the buffer...
    Timestamp = mi_utils:now_to_timestamp(NowTime),
    NewBuffer = mi_buffer:write(IFT, Value, Props, Timestamp, Buffer),
    
    %% Update the state...
    NewState = State#state { 
        indexes=NewIndexes, 
        fields=NewFields,
        terms=NewTerms,
        buffer = NewBuffer
    },

    %% Possibly dump buffer to a new segment...
    case mi_buffer:size(NewBuffer) > ?ROLLOVERSIZE(State) of
        true ->
            %% Get the new segment name...
            SegNum  = length(Segments) + 1,
            SegFile = join(NewState, "segment." ++ integer_to_list(SegNum)),

            %% Create the new segment...
            mi_buffer:close(NewBuffer),
            NewSegment = mi_segment:from_buffer(SegFile, NewBuffer),
            NewSegments = [NewSegment|Segments],

            %% Clear the buffer...
            NewBuffer1 = mi_buffer:clear(NewBuffer),
            
            %% Update the state...
            NewState1 = NewState#state { buffer=NewBuffer1, segments=NewSegments},
            {reply, ok, NewState1};
        false ->
            {reply, ok, NewState}
    end;

handle_call({info, Index, Field, Term, SubType, SubTerm}, _From, State) ->
    %% Calculate the IFT...
    #state { indexes=Indexes, fields=Fields, terms=Terms } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermID = mi_incdex:lookup_nocreate(Term, Terms),
    IFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, SubTerm),

    %% Look up the offset information...
    F = fun(X, Acc) ->
         mi_segment:info(IFT, X) + Acc
    end,
    Count = lists:foldl(F, 0, State#state.segments),
    {reply, {ok, Count}, State};

handle_call({info_range, Index, Field, StartTerm, EndTerm, Size, SubType, StartSubTerm, EndSubTerm}, _From, State) ->
    %% Get the IDs...
    #state { indexes=Indexes, fields=Fields, terms=Terms, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermIDs = mi_incdex:select(StartTerm, EndTerm, Size, Terms),
    {StartSubTerm1, EndSubTerm1} = normalize_subterm(StartSubTerm, EndSubTerm),

    F = fun({Term, TermID}) ->
        StartIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, StartSubTerm1),
        EndIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, EndSubTerm1),
        Count = lists:sum([mi_segment:info(StartIFT, EndIFT, X) || X <- Segments]),
        {Term, Count}
    end,
    Counts = [F(X) || X <- TermIDs],
    {reply, {ok, Counts}, State};

handle_call({stream, Index, Field, Term, SubType, StartSubTerm, EndSubTerm, Pid, Ref, FilterFun}, _From, State) ->
    %% Get the IDs...
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffer=Buffer, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermID = mi_incdex:lookup_nocreate(Term, Terms),
    {StartSubTerm1, EndSubTerm1} = normalize_subterm(StartSubTerm, EndSubTerm),
    StartIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, StartSubTerm1),
    EndIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, EndSubTerm1),

    %% Get an iterator for each segment...
    spawn_link(fun() -> stream(StartIFT, EndIFT, Pid, Ref, FilterFun, Buffer, Segments) end),
    {reply, ok, State};

handle_call(Request, _From, State) ->
    ?PRINT({unhandled_call, Request}),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    ?PRINT({unhandled_cast, Msg}),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Merge-sort the results from Iterators, and stream to the pid.
stream(StartIFT, EndIFT, Pid, Ref, FilterFun, Buffer, Segments) ->
    %% Put together the group iterator...
    BufferIterator = mi_buffer:iterator(StartIFT, EndIFT, Buffer),
    SegmentIterators = [mi_segment:iterator(StartIFT, EndIFT, X) || X <- Segments],
    GroupIterator = build_group_iterator([BufferIterator|SegmentIterators]),

    %% Start streaming...
    stream_inner(Pid, Ref, undefined, undefined, FilterFun, GroupIterator()),
    ok.
stream_inner(Pid, Ref, LastIFT, LastValue, FilterFun, {{IFT, Value, Props, _}, Iter}) ->
    IsDuplicate = (LastIFT == IFT) andalso (LastValue == Value),
    case (not IsDuplicate) andalso (FilterFun(Value, Props) == true) of
        true ->
            Pid!{result, {Value, Props}, Ref};
        false ->
            skip
    end,
    stream_inner(Pid, Ref, IFT, Value, FilterFun, Iter());
stream_inner(Pid, Ref, _, _, _, eof) ->
    Pid!{result, '$end_of_table', Ref}.

%% Chain a list of iterators into what looks like one single iterator.    
build_group_iterator([Iterator|Iterators]) ->
    GroupIterator = build_group_iterator(Iterators),
    fun() -> group_iterator(Iterator(), GroupIterator()) end;
build_group_iterator([]) ->
    fun() -> eof end.
group_iterator(I1 = {Term1, Iterator1}, I2 = {Term2, Iterator2}) ->
    case compare_fun(Term1, Term2) of
        true ->
            NewIterator = fun() -> group_iterator(Iterator1(), I2) end,
            {Term1, NewIterator};
        false ->
            NewIterator = fun() -> group_iterator(I1, Iterator2()) end,
            {Term2, NewIterator}
    end;
group_iterator(eof, I) -> group_iterator(I);
group_iterator(I, eof) -> group_iterator(I).
group_iterator({Term, Iterator}) ->
    NewIterator = fun() -> group_iterator(Iterator()) end,
    {Term, NewIterator};
group_iterator(eof) ->
    eof.

compare_fun({IFT1, Value1, _, TS1}, {IFT2, Value2, _, TS2}) ->
    (IFT1 < IFT2) orelse
    ((IFT1 == IFT2) andalso (Value1 < Value2)) orelse
    ((IFT1 == IFT2) andalso (Value1 == Value2) andalso (TS1 > TS2)).
    

%% SubTerm range...
normalize_subterm(StartSubTerm, EndSubTerm) ->
    StartSubTerm1 = case StartSubTerm of
        all -> ?MINSUBTERM;
        _   -> StartSubTerm
    end,
    EndSubTerm1 = case EndSubTerm of
        all -> ?MAXSUBTERM;
        _   -> EndSubTerm
    end,
    {StartSubTerm1, EndSubTerm1}.


join(#state { root=Root }, Name) ->
    join(Root, Name);

join(Root, Name) ->            
    filename:join([Root, Name]).
