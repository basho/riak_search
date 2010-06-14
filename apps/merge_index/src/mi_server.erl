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
-include_lib("kernel/include/file.hrl").

-export([
    %% GEN SERVER
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, { 
    root,
    locks,
    indexes,
    fields,
    terms,
    segments,
    buffers,
    config,
    next_id,
    is_compacting
}).

init([Root, Config]) ->
    %% Get incdex and buffer options...
    TempState = #state { config=Config },
    SyncInterval = ?SYNC_INTERVAL(TempState),
    IncdexOptions = [{write_interval, SyncInterval}],
    BufferOptions = [{write_interval, SyncInterval}],
    
    %% Load from disk...
    filelib:ensure_dir(join(Root, "ignore")),
    io:format("Loading merge_index from '~s'~n", [Root]),
    {NextID, Buffer, Segments} = read_buf_and_seg(Root, BufferOptions),
    io:format("Finished loading merge_index from '~s'~n", [Root]),

    %% Create the state...
    State = #state {
        root     = Root,
        locks    = mi_locks:new(),
        indexes  = mi_incdex:open(join(Root, "indexes"), IncdexOptions),
        fields   = mi_incdex:open(join(Root, "fields"), IncdexOptions),
        terms    = mi_incdex:open(join(Root, "terms"), IncdexOptions),
        buffers  = [Buffer],
        segments = Segments,
        config   = Config,
        next_id  = NextID,
        is_compacting = false
    },

    %% Return.
    {ok, State}.

%% Return {Buffers, Segments}, after cleaning up/repairing any partially merged buffers.
read_buf_and_seg(Root, BufferOptions) ->
    %% Delete any files that have a ".deleted" flag. This means that
    %% the system stopped before proper cleanup.
    io:format("Cleaning up...~n"),
    F1 = fun(Filename) ->
        Basename = filename:basename(Filename, ".deleted"),
        Basename1 = filename:join(Root, Basename ++ ".*"),
        io:format("Deleting '~s'~n", [Basename1]),
        io:format("~p~n", [filelib:wildcard(Basename1)]),
        [ok = file:delete(X) || X <- filelib:wildcard(Basename1)]
    end,
    [F1(X) || X <- filelib:wildcard(join(Root, "*.deleted"))],

    %% Open the segments...
    SegmentFiles = filelib:wildcard(join(Root, "segment.*.data")),
    SegmentFiles1 = [filename:join(Root, filename:basename(X, ".data")) || X <- SegmentFiles],
    Segments = read_segments(SegmentFiles1, []),

    %% Get buffer files, calculate the next_id, load the buffers, turn
    %% any extraneous buffers into segments...
    BufferFiles = filelib:wildcard(join(Root, "buffer.*")),
    BufferFiles1 = lists:sort([{get_id_number(X), X} || X <- BufferFiles]),
    NextID = lists:max([X || {X, _} <- BufferFiles1] ++ [0]) + 1,
    {NextID1, Buffer, Segments1} = read_buffers(Root, BufferOptions, BufferFiles1, NextID, Segments),
    
    %% Return...
    {NextID1, Buffer, Segments1}.

read_segments([], _Segments) -> [];
read_segments([SName|Rest], Segments) ->
    %% Read the segment from disk...
    io:format("Opening segment: '~s'~n", [SName]),
    Segment = mi_segment:open(SName),
    [Segment|read_segments(Rest, Segments)].

read_buffers(Root, BufferOptions, [], NextID, Segments) ->
    %% No latest buffer exists, open a new one...
    BName = join(Root, "buffer." ++ integer_to_list(NextID)),
    io:format("Opening new buffer: '~s'~n", [BName]),
    Buffer = mi_buffer:open(BName, BufferOptions),
    {NextID + 1, Buffer, Segments};

read_buffers(_Root, BufferOptions, [{_BNum, BName}], NextID, Segments) ->
    %% This is the final buffer file... return it as the open buffer...
    io:format("Opening buffer: '~s'~n", [BName]),
    Buffer = mi_buffer:open(BName, BufferOptions),
    {NextID, Buffer, Segments};

read_buffers(Root, BufferOptions, [{BNum, BName}|Rest], NextID, Segments) ->
    %% Multiple buffers exist... convert them into segments...
    io:format("Converting buffer: '~s' to segment.~n", [BName]),
    SName = join(Root, "segment." ++ integer_to_list(BNum)),
    set_deleteme_flag(SName),
    Segment = mi_segment:open(SName),
    Buffer = mi_buffer:open(BName, BufferOptions),
    mi_buffer:close_filehandle(Buffer),
    mi_segment:from_buffer(Buffer, Segment),
    mi_buffer:delete(Buffer),
    clear_deleteme_flag(mi_segment:filename(Segment)),
    
    %% Loop...
    read_buffers(Root, BufferOptions, Rest, NextID, [Segment|Segments]).


handle_call({index, Index, Field, Term, Value, Props, TS}, _From, State) ->
    %% Calculate the IFT...
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=[CurrentBuffer0|Buffers] } = State,
    {IndexID, NewIndexes} = mi_incdex:lookup(Index, Indexes),
    {FieldID, NewFields} = mi_incdex:lookup(Field, Fields),
    {TermID, NewTerms} = mi_incdex:lookup(Term, Terms),
    IFT = mi_utils:ift_pack(IndexID, FieldID, TermID),

    %% Write to the buffer...
    CurrentBuffer = mi_buffer:write(IFT, Value, Props, TS, CurrentBuffer0),

    %% Update the state...
    NewState = State#state {
        indexes=NewIndexes,
        fields=NewFields,
        terms=NewTerms,
        buffers=[CurrentBuffer|Buffers]
    },

    %% Possibly dump buffer to a new segment...
    case mi_buffer:size(CurrentBuffer) > ?ROLLOVER_SIZE(NewState) of
        true ->
            #state { root=Root, next_id=NextID } = NewState,

            %% Start processing the latest buffer.
            mi_buffer:close_filehandle(CurrentBuffer),
            SNum  = get_id_number(mi_buffer:filename(CurrentBuffer)),
            SName = join(Root, "segment." ++ integer_to_list(SNum)),
            set_deleteme_flag(SName),
            Segment = mi_segment:open(SName),
            Pid = self(),
            spawn_link(fun() ->
                mi_segment:from_buffer(CurrentBuffer, Segment),
                gen_server:call(Pid, {buffer_to_segment, CurrentBuffer, Segment}, infinity)
            end),
            
            %% Create a new empty buffer...
            BName = join(NewState, "buffer." ++ integer_to_list(NextID)),
            BufferOptions = [{write_interval, ?SYNC_INTERVAL(State)}],
            NewBuffer = mi_buffer:open(BName, BufferOptions),
            
            NewState1 = NewState#state {
                buffers=[NewBuffer|NewState#state.buffers],
                next_id=NextID + 1
            },
            {reply, ok, NewState1};
        false ->
            {reply, ok, NewState}
    end;

handle_call({buffer_to_segment, Buffer, Segment}, _From, State) ->
    #state { locks=Locks, buffers=Buffers, segments=Segments, is_compacting=IsCompacting } = State,

    %% Clean up by clearing delete flag on the segment, adding delete
    %% flag to the buffer, and telling the system to delete the buffer
    %% as soon as the last lock is released.
    clear_deleteme_flag(mi_segment:filename(Segment)),
    BName = mi_buffer:filename(Buffer),
    set_deleteme_flag(BName),
    NewLocks = mi_locks:when_free(BName, fun() -> mi_buffer:delete(Buffer) end, Locks),

    %% Update state...
    NewSegments = [Segment|Segments],
    NewState = State#state {
        locks=NewLocks,
        buffers=Buffers -- [Buffer],
        segments=NewSegments
    },

    case not IsCompacting andalso length(NewSegments) >= 10 of
        true ->

            %% Get which segments to compact. Do this by getting
            %% filesizes, and then lopping off the four biggest
            %% files. This could be optimized with tuning, but
            %% probably a good enough solution.
            SegmentsToCompact = get_smallest_files(NewSegments, 4),
            
            %% Create the new compaction segment...
            {StartNum, EndNum} = get_id_range(SegmentsToCompact),
            SName = join(State, io_lib:format("segment.~p-~p", [StartNum, EndNum])),
            set_deleteme_flag(SName),
            CompactSegment = mi_segment:open(SName),
            
            %% Spawn a function to merge a bunch of segments into one...
            Pid = self(),
            spawn_link(fun() ->
                StartIFT = mi_utils:ift_pack(0, 0, 0, 0, 0),
                EndIFT = all,
                SegmentIterators = [mi_segment:iterator(StartIFT, EndIFT, X) || X <- SegmentsToCompact],
                GroupIterator = build_group_iterator(SegmentIterators),
                mi_segment:from_iterator(GroupIterator, CompactSegment),
                gen_server:call(Pid, {compacted, CompactSegment, SegmentsToCompact}, infinity)
            end),
            {reply, ok, NewState#state { is_compacting=true }};
        false ->
            {reply, ok, NewState}
    end;

handle_call({compacted, CompactSegment, OldSegments}, _From, State) ->
    #state { locks=Locks, segments=Segments } = State,

    %% Clean up. Remove delete flag on the new segment. Add delete
    %% flags to the old segments. Register to delete the old segments
    %% when the locks are freed.
    clear_deleteme_flag(mi_segment:filename(CompactSegment)),
    [set_deleteme_flag(mi_segment:filename(X)) || X <- OldSegments],
    F = fun(X, Acc) ->
        mi_locks:when_free(mi_segment:filename(X), fun() -> mi_segment:delete(X) end, Acc)
    end,
    NewLocks = lists:foldl(F, Locks, OldSegments),

    %% Update State and return...
    NewState = State#state {
        locks=NewLocks,
        segments=[CompactSegment|(Segments -- OldSegments)],
        is_compacting=false
    },
    {reply, ok, NewState};

handle_call({info, Index, Field, Term}, _From, State) ->
    %% Calculate the IFT...
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermID = mi_incdex:lookup_nocreate(Term, Terms),
    IFT = mi_utils:ift_pack(IndexID, FieldID, TermID),

    %% Look up the counts in buffers and segments...
    BufferCount = lists:sum([mi_buffer:info(IFT, X) || X <- Buffers]),
    SegmentCount = lists:sum([mi_segment:info(IFT, X) || X <- Segments]),
    Counts = [{Term, BufferCount + SegmentCount}],

    %% Return...
    {reply, {ok, Counts}, State};

handle_call({info_range, Index, Field, StartTerm, EndTerm, Size}, _From, State) ->
    %% Get the IDs...
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermIDs = mi_incdex:select(StartTerm, EndTerm, Size, Terms),

    F = fun({Term, TermID}) ->
        StartIFT = mi_utils:ift_pack(IndexID, FieldID, TermID),
        EndIFT = mi_utils:ift_pack(IndexID, FieldID, TermID),
        BufferCount = lists:sum([mi_buffer:info(StartIFT, EndIFT, X) || X <- Buffers]),
        SegmentCount = lists:sum([mi_segment:info(StartIFT, EndIFT, X) || X <- Segments]),
        {Term, BufferCount + SegmentCount}
    end,
    Counts = [F(X) || X <- TermIDs],
    {reply, {ok, Counts}, State};

handle_call({stream, Index, Field, Term, Pid, Ref, FilterFun}, _From, State) ->
    %% Get the IDs...
    #state { locks=Locks, indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermID = mi_incdex:lookup_nocreate(Term, Terms),
    StartIFT = mi_utils:ift_pack(IndexID, FieldID, TermID),
    EndIFT = mi_utils:ift_pack(IndexID, FieldID, TermID),

    %% Add locks to all buffers...
    F1 = fun(Buffer, Acc) ->
        mi_locks:claim(mi_buffer:filename(Buffer), Acc)
    end,
    NewLocks = lists:foldl(F1, Locks, Buffers),

    %% Add locks to all segments...
    F2 = fun(Segment, Acc) ->
        mi_locks:claim(mi_segment:filename(Segment), Acc)
    end,
    NewLocks1 = lists:foldl(F2, NewLocks, Segments),

    %% Spawn a streaming function...
    Self = self(),
    spawn_link(fun() -> 
        F = fun(_IFT, Value, Props, _TS) ->
            case FilterFun(Value, Props) of
                true -> Pid!{result, {Value, Props}, Ref};
                _ -> skip
            end
        end,
        stream(StartIFT, EndIFT, F, Buffers, Segments),
        Pid!{result, '$end_of_table', Ref},
        gen_server:call(Self, {stream_finished, Buffers, Segments}, infinity)
    end),

    %% Reply...
    {reply, ok, State#state { locks=NewLocks1 }};

handle_call({stream_finished, Buffers, Segments}, _From, State) ->
    #state { locks=Locks } = State,

    %% Remove locks from all buffers...
    F1 = fun(Buffer, Acc) ->
        mi_locks:release(mi_buffer:filename(Buffer), Acc)
    end,
    NewLocks = lists:foldl(F1, Locks, Buffers),

    %% Remove locks from all segments...
    F2 = fun(Segment, Acc) ->
        mi_locks:release(mi_segment:filename(Segment), Acc)
    end,
    NewLocks1 = lists:foldl(F2, NewLocks, Segments),

    %% Return...
    {reply, ok, State#state { locks=NewLocks1 }};

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
        {IndexID, FieldID, TermID} = mi_utils:ift_unpack(IFT),
        {value, Index} = gb_trees:lookup(IndexID, InvertedIndexes),
        {value, Field} = gb_trees:lookup(FieldID, InvertedFields),
        {value, Term} = gb_trees:lookup(TermID, InvertedTerms),

        %% Call the fold function...
        Fun(Index, Field, Term, Value, Props, TS, AccIn)
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
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,

    %% Figure out some options...
    SyncInterval = ?SYNC_INTERVAL(State),
    BufferOptions = [{write_interval, SyncInterval}],

    %% Delete files, reset state...
    [mi_buffer:delete(X) || X <- Buffers],
    [mi_segment:delete(X) || X <- Segments],
    BufferFile = join(State, "buffer.1"),
    Buffer = mi_buffer:open(BufferFile, BufferOptions),
    NewState = State#state { 
        locks = mi_locks:new(),
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
stream(StartIFT, EndIFT, F, Buffers, Segments) ->
    %% Put together the group iterator...
    BufferIterators = [mi_buffer:iterator(StartIFT, EndIFT, X) || X <- Buffers],
    SegmentIterators = [mi_segment:iterator(StartIFT, EndIFT, X) || X <- Segments],
    GroupIterator = build_group_iterator(BufferIterators ++ SegmentIterators),

    %% Start streaming...
    stream_inner(F, undefined, undefined, GroupIterator()),
    ok.
stream_inner(F, LastIFT, LastValue, {{IFT, Value, Props, TS}, Iter}) ->
    IsDuplicate = (LastIFT == IFT) andalso (LastValue == Value),
    IsDeleted = (Props == undefined),
    case (not IsDuplicate) andalso (not IsDeleted) of
        true  -> 
            F(IFT, Value, Props, TS);
        false -> 
            skip
    end,
    stream_inner(F, IFT, Value, Iter());
stream_inner(_, _, _, eof) -> ok.

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

%% Return true if the two tuples are in sorted order. 
compare_fun({IFT1, Value1, _, TS1}, {IFT2, Value2, _, TS2}) ->
    (IFT1 < IFT2) orelse
    ((IFT1 == IFT2) andalso (Value1 < Value2)) orelse
    ((IFT1 == IFT2) andalso (Value1 == Value2) andalso (TS1 > TS2)).


%% Return the starting and ending segment number in the list of
%% segments, accounting for compaction segments which have a "1-5"
%% type numbering.
get_id_range(Segments) when is_list(Segments)->
    Numbers = lists:flatten([get_id_number(X) || X <- Segments]),
    {lists:min(Numbers), lists:max(Numbers)}.

%% Return the ID number of a Segment/Buffer/Filename...
%% Files can be named:
%%   - buffer.N
%%   - segment.N
%%   - segment.N.data
%%   - segment.M-N
%%   - segment.M-N.data
get_id_number(Segment) when element(1, Segment) == segment ->
    Filename = mi_segment:filename(Segment),
    get_id_number(Filename);
get_id_number(Buffer) when element(1, Buffer) == buffer ->
    Filename = mi_buffer:filename(Buffer),
    get_id_number(Filename);
get_id_number(Filename) ->
    case string:chr(Filename, $-) == 0 of
        true ->
            %% Handle buffer.N, segment.N, segment.N.data
            case string:tokens(Filename, ".") of
                [_, N]    -> ok;
                [_, N, _] -> ok
            end,
            list_to_integer(N);
        false ->
            %% Handle segment.M-N, segment.M-N.data
            case string:tokens(Filename, ".-") of
                [_, M, N]    -> ok;
                [_, M, N, _] -> ok
            end,
            [list_to_integer(M), list_to_integer(N)]
    end.

set_deleteme_flag(Filename) ->
    file:write_file(Filename ++ ".deleted", "").

clear_deleteme_flag(Filename) ->
    file:delete(Filename ++ ".deleted").

%% Collect the smallest existing files for a merge. Get the file
%% sizes. If the biggest is less than twice the smallest, they are
%% roughly the same size, so merge them all. Otherwise, drop off the
%% first N biggest files, and merge the rest.
get_smallest_files(Segments, N) ->
    F = fun(X) ->
        {ok, FileInfo} = file:read_file_info(mi_segment:data_file(X)),
        {FileInfo#file_info.size, X}
    end,
    SizesAsc = lists:sort([F(X) || X <- Segments]),
    SizesDec = lists:reverse(SizesAsc),
    BiggestSize = element(1, hd(SizesDec)),
    SmallestSize = element(1, hd(SizesAsc)),
    case BiggestSize < SmallestSize * 2 of
        true  -> 
            Segments;
        false ->
            {_, SmallestSegments} = lists:split(N, SizesDec),
            [X || {_, X} <- SmallestSegments]
    end.

fold(_Fun, Acc, eof) -> 
    Acc;
fold(Fun, Acc, {Term, IteratorFun}) ->
    fold(Fun, Fun(Term, Acc), IteratorFun()).

join(#state { root=Root }, Name) ->
    join(Root, Name);

join(Root, Name) ->
    filename:join([Root, Name]).
