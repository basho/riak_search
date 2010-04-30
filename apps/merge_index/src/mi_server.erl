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
    filelib:ensure_dir(join(Root, "ignore")),
    {Buffers, Segments, Locks} = read_buf_and_seg(Root, mi_locks:new()),

    %% Create the state...
    State = #state {
        root = Root,
        locks = Locks,
        indexes  = mi_incdex:open(join(Root, "indexes")),
        fields   = mi_incdex:open(join(Root, "fields")),
        terms    = mi_incdex:open(join(Root, "terms")),
        buffers  = Buffers,
        segments = Segments,
        config = Config
    },

    %% Return.
    {ok, State}.

%% Return {Buffers, Segments}, after cleaning up/repairing any partially merged buffers.
read_buf_and_seg(Root, Locks) ->
    %% Get a list of buffers...
    F1 = fun(Filename) -> 
        [_, Num] = string:tokens(Filename, "."),
        {list_to_integer(Num), Filename}
    end,
    BFiles = lists:sort([F1(X) || X <- filelib:wildcard(join(Root, "buffer.*"))]),
    ?PRINT(BFiles),

    %% Get a list of segments...
    F2 = fun(Filename) ->
        F1(filename:rootname(Filename))
    end,
    SFiles = lists:sort([F2(X) || X <- filelib:wildcard(join(Root, "segment.*.data"))]),
    ?PRINT(SFiles),
    read_buf_and_seg_1(Root, Locks, BFiles, SFiles, [], []).

read_buf_and_seg_1(Root, Locks, [], [], Buffers, Segments) ->
    BNum = length(Buffers) + length(Segments) + 1,
    BName = join(Root, "buffer." ++ integer_to_list(BNum)),
    Buffer = mi_buffer:open(BName),
    NewLocks = mi_locks:claim(mi_buffer:filename(Buffer), fun() -> mi_buffer:delete(Buffer) end, Locks),
    {[Buffer|Buffers], Segments, NewLocks};
read_buf_and_seg_1(_Root, Locks, [BFile], [], Buffers, Segments) ->
    %% Reached the last buffer file, open it...
    {_BNum, BName} = BFile,
    Buffer = mi_buffer:open(BName),
    NewLocks = mi_locks:claim(mi_buffer:filename(Buffer), fun() -> mi_buffer:delete(Buffer) end, Locks),
    {[Buffer|Buffers], Segments, NewLocks};
read_buf_and_seg_1(Root, Locks, [BFile|BFiles], [], Buffers, Segments) ->
    %% Convert buffer files to segment files...
    {BNum, BName} = BFile,
    SName = join(Root, "segment." ++ integer_to_list(BNum)),
    Buffer = mi_buffer:open(BName),
    mi_buffer:close_filehandle(Buffer),
    Segment = mi_segment:from_buffer(SName, Buffer),
    mi_buffer:delete(Buffer),
    read_buf_and_seg_1(Root, Locks, BFiles, [], Buffers, [Segment|Segments]);
read_buf_and_seg_1(Root, Locks, [BFile|BFiles], [SFile|SFiles], Buffers, Segments) ->
    {BNum, BName} = BFile,
    {SNum, SName} = SFile,
    if 
        BNum < SNum ->
            %% Should not have this case...
            throw({missing_segment_file, element(2, SFile)});
        BNum == SNum ->
            ?PRINT({converting, BName, to, SName}),
            %% Remove and recreate segment...
            file:delete(SName),
            Buffer = mi_buffer:open(BName),
            mi_buffer:close_filehandle(Buffer),
            Segment = mi_segment:from_buffer(SName, Buffer),
            mi_buffer:delete(Buffer),
            read_buf_and_seg_1(Root, Locks, BFiles, SFiles, Buffers, [Segment|Segments]);
        BNum > SNum ->
            %% Open the segment and loop...
            Segment = mi_segment:open(SName),
            read_buf_and_seg_1(Root, Locks, [BFile|BFiles], SFiles, Buffers, [Segment|Segments])
    end;
read_buf_and_seg_1(_Root, _Locks, [], SFiles, _Buffers, _Segments) ->
    throw({too_many_segment_files, SFiles}).

handle_call({index, Index, Field, Term, SubType, SubTerm, Value, Props, TS}, _From, State) ->
    %% Calculate the IFT...
    #state { root=Root, locks=Locks, indexes=Indexes, fields=Fields, terms=Terms, buffers=[Buffer|Buffers], segments=Segments } = State,
    {IndexID, NewIndexes} = mi_incdex:lookup(Index, Indexes),
    {FieldID, NewFields} = mi_incdex:lookup(Field, Fields),
    {TermID, NewTerms} = mi_incdex:lookup(Term, Terms),
    IFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, SubTerm),

    %% Write to the buffer...
    NewBuffer = mi_buffer:write(IFT, Value, Props, TS, Buffer),

    %% Update the state...
    NewState = State#state {
        indexes=NewIndexes,
        fields=NewFields,
        terms=NewTerms,
        buffers=[NewBuffer|Buffers]
    },

    %% Possibly dump buffer to a new segment...
    case mi_buffer:size(NewBuffer) > ?ROLLOVERSIZE(State) of
        true ->
            %% Start processing the latest buffer.
            Pid = self(),
            mi_buffer:close_filehandle(NewBuffer),
            spawn_link(fun() ->
                Segment = buffer_to_segment(Root, NewBuffer),
                gen_server:call(Pid, {buffer_to_segment, NewBuffer, Segment}, infinity)
            end),
            
            %% Create a new empty buffer...
            BNum = length([NewBuffer|Buffers]) + length(Segments) + 1,
            BName = join(NewState, "buffer." ++ integer_to_list(BNum)),
            EmptyBuffer = mi_buffer:open(BName),
            NewLocks = mi_locks:claim(mi_buffer:filename(EmptyBuffer), fun() -> mi_buffer:delete(EmptyBuffer) end, Locks),
            
            NewState1 = NewState#state {
                locks=NewLocks,
                buffers=[EmptyBuffer|NewState#state.buffers]
            },
            {reply, ok, NewState1};
        false ->
            {reply, ok, NewState}
    end;

handle_call({buffer_to_segment, Buffer, Segment}, _From, State) ->
    #state { locks=Locks, buffers=Buffers, segments=Segments } = State,

    %% Delete the buffer...
    NewLocks = mi_locks:release(mi_buffer:filename(Buffer), Locks),
    
    %% Update state and return...
    NewState = State#state {
        locks=NewLocks,
        buffers=Buffers -- [Buffer],
        segments=[Segment|Segments]
    },
    {reply, ok, NewState};

handle_call({info, Index, Field, Term, SubType, SubTerm}, _From, State) ->
    %% Calculate the IFT...
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermID = mi_incdex:lookup_nocreate(Term, Terms),
    IFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, SubTerm),

    %% Look up the counts in buffers and segments...
    BufferCount = lists:sum([mi_buffer:info(IFT, X) || X <- Buffers]),
    SegmentCount = lists:sum([mi_segment:info(IFT, X) || X <- Segments]),
    Counts = [{Term, BufferCount + SegmentCount}],

    %% Return...
    {reply, {ok, Counts}, State};

handle_call({info_range, Index, Field, StartTerm, EndTerm, Size, SubType, StartSubTerm, EndSubTerm}, _From, State) ->
    %% Get the IDs...
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermIDs = mi_incdex:select(StartTerm, EndTerm, Size, Terms),
    {StartSubTerm1, EndSubTerm1} = normalize_subterm(StartSubTerm, EndSubTerm),

    F = fun({Term, TermID}) ->
        StartIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, StartSubTerm1),
        EndIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, EndSubTerm1),
        BufferCount = lists:sum([mi_buffer:info(StartIFT, EndIFT, X) || X <- Buffers]),
        SegmentCount = lists:sum([mi_segment:info(StartIFT, EndIFT, X) || X <- Segments]),
        {Term, BufferCount + SegmentCount}
    end,
    Counts = [F(X) || X <- TermIDs],
    {reply, {ok, Counts}, State};

handle_call({stream, Index, Field, Term, SubType, StartSubTerm, EndSubTerm, Pid, Ref, FilterFun}, _From, State) ->
    %% Get the IDs...
    #state { locks=Locks, indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermID = mi_incdex:lookup_nocreate(Term, Terms),
    {StartSubTerm1, EndSubTerm1} = normalize_subterm(StartSubTerm, EndSubTerm),
    StartIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, StartSubTerm1),
    EndIFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, EndSubTerm1),

    %% Add locks to all buffers...
    F = fun(Buffer, Acc) ->
        mi_locks:claim(mi_buffer:filename(Buffer), Acc)
    end,
    NewLocks = lists:foldl(F, Locks, Buffers),

    %% Get an iterator for each segment...
    Self = self(),
    spawn_link(fun() -> 
        stream(StartIFT, EndIFT, Pid, Ref, FilterFun, Buffers, Segments),
        gen_server:call(Self, {stream_finished, Buffers}, infinity)
    end),

    %% Reply...
    {reply, ok, State#state { locks=NewLocks }};

handle_call({stream_finished, Buffers}, _From, State) ->
    #state { locks=Locks } = State,

    %% Remove locks from all buffers...
    F = fun(Buffer, Acc) ->
        mi_locks:release(mi_buffer:filename(Buffer), Acc)
    end,
    NewLocks = lists:foldl(F, Locks, Buffers),
    {reply, ok, State#state { locks=NewLocks }};

handle_call({fold, Fun, Acc}, _From, State) ->
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    
    %% Reverse the incdexes. Normally, they map "Value" to ID.  The
    %% invert/1 command returns a gbtree mapping ID to "Value".
    InvertedIndexes = mi_incdex:invert(Indexes),
    InvertedFields = mi_incdex:invert(Fields),
    InvertedTerms = mi_incdex:invert(Terms),

    %% Create the fold function.
    WrappedFun = fun({IFT, Value, Props, TS}, AccIn) ->
        %% Look up the Index, Field, and Term...
        {IndexID, FieldID, TermID, SubType, SubTerm} = mi_utils:ift_unpack(IFT),
        {value, Index} = gb_trees:lookup(IndexID, InvertedIndexes),
        {value, Field} = gb_trees:lookup(FieldID, InvertedFields),
        {value, Term} = gb_trees:lookup(TermID, InvertedTerms),

        %% Call the fold function...
        Fun(Index, Field, Term, SubType, SubTerm, Value, Props, TS, AccIn)
    end,

    %% Fold over each buffer...
    F1 = fun(Buffer, AccIn) ->
        Begin = mi_utils:ift_pack(0, 0, 0, 0, 0),
        End = all,
        BufferIterator = mi_buffer:iterator(Begin, End, Buffer),
        fold(WrappedFun, AccIn, BufferIterator())
    end,
    Acc1 = lists:foldl(F1, Acc, Buffers),

    %% Fold over each segment...
    F2 = fun(Segment, AccIn) ->
        Begin = mi_utils:ift_pack(0, 0, 0, 0, 0),
        End = all,
        SegmentIterator = mi_segment:iterator(Begin, End, Segment),
        fold(WrappedFun, AccIn, SegmentIterator())
    end,
    Acc2 = lists:foldl(F2, Acc1, Segments),

    %% Reply...
    {reply, {ok, Acc2}, State};
    
handle_call(is_empty, _From, State) ->
    #state { terms=Terms } = State,
    IsEmpty = (mi_incdex:size(Terms) == 0),
    {reply, IsEmpty, State};

handle_call(drop, _From, State) ->
    #state { locks=Locks, indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    
    %% Delete files, reset state...
    [mi_buffer:delete(X) || X <- Buffers],
    [mi_segment:delete(X) || X <- Segments],
    BufferFile = join(State, "buffer.1"),
    Buffer = mi_buffer:open(BufferFile),
    NewLocks = mi_locks:claim(mi_buffer:filename(Buffer), fun() -> mi_buffer:delete(Buffer) end, Locks),
    NewState = State#state { 
        locks = NewLocks,
        indexes = mi_incdex:clear(Indexes),
        fields = mi_incdex:clear(Fields),
        terms = mi_incdex:clear(Terms),
        buffers = [Buffer],
        segments = []
    },
    {reply, ok, NewState};

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
stream(StartIFT, EndIFT, Pid, Ref, FilterFun, Buffers, Segments) ->
    %% Put together the group iterator...
    BufferIterators = [mi_buffer:iterator(StartIFT, EndIFT, X) || X <- Buffers],
    SegmentIterators = [mi_segment:iterator(StartIFT, EndIFT, X) || X <- Segments],
    GroupIterator = build_group_iterator(BufferIterators ++ SegmentIterators),

    %% Start streaming...
    stream_inner(Pid, Ref, undefined, undefined, FilterFun, GroupIterator()),
    ok.
stream_inner(Pid, Ref, LastIFT, LastValue, FilterFun, {{IFT, Value, Props, _}, Iter}) ->
    IsDuplicate = (LastIFT == IFT) andalso (LastValue == Value),
    {_IndexID, _FieldID, _TermID, SubType, SubTerm} = mi_utils:ift_unpack(IFT),
    NewProps = [{subterm, {SubType, SubTerm}}|Props],
    case (not IsDuplicate) andalso (FilterFun(Value, NewProps) == true) of
        true ->
            Pid!{result, {Value, NewProps}, Ref};
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


fold(_Fun, Acc, eof) -> 
    Acc;
fold(Fun, Acc, {Term, IteratorFun}) ->
    fold(Fun, Fun(Term, Acc), IteratorFun()).

buffer_to_segment(Root, Buffer) ->
    %% Get the new segment name...
    SegNum  = tl(filename:extension(mi_buffer:filename(Buffer))),
    SegFile = join(Root, "segment." ++ SegNum),
    
    %% Create and return the new segment.
    mi_segment:from_buffer(SegFile, Buffer).


join(#state { root=Root }, Name) ->
    join(Root, Name);

join(Root, Name) ->
    filename:join([Root, Name]).
