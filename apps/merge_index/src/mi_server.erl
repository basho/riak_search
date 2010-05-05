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
    {NextID, Buffers, Segments, Locks} = read_buf_and_seg(Root, BufferOptions, mi_locks:new()),
    io:format("Finished loading merge_index from '~s'~n", [Root]),

    %% Create the state...
    State = #state {
        root     = Root,
        locks    = Locks,
        indexes  = mi_incdex:open(join(Root, "indexes"), IncdexOptions),
        fields   = mi_incdex:open(join(Root, "fields"), IncdexOptions),
        terms    = mi_incdex:open(join(Root, "terms"), IncdexOptions),
        buffers  = Buffers,
        segments = Segments,
        config   = Config,
        next_id  = NextID,
        is_compacting = false
    },

    %% Return.
    {ok, State}.

%% Return {Buffers, Segments}, after cleaning up/repairing any partially merged buffers.
read_buf_and_seg(Root, BufferOptions, Locks) ->
    %% Delete any segment files prefixed with "_", they are temporary...
    [file:delete(X) || X <- filelib:wildcard(join(Root, "_segment.*"))],

    %% Delete any segment files covered by other segment files...
    F1 = fun(X) ->
        [StartNum, EndNum] = get_id_number(X),
        [file:delete(join(Root, io_lib:format("segment.~p.*", [Y]))) || Y <- lists:seq(StartNum, EndNum)]
    end,
    [F1(X) || X <- filelib:wildcard(join(Root, "segment.*-*.data"))],

    %% Get a list of buffers...
    F2 = fun(Filename) -> 
        {get_id_number(Filename), Filename}
    end,
    BFiles = lists:sort([F2(X) || X <- filelib:wildcard(join(Root, "buffer.*"))]),

    %% Get a list of segments...
    F3 = fun(Filename) ->
        Filename1 = filename:rootname(Filename),
        case get_id_number(Filename1) of
            [_, Int] -> {Int, Filename1};
            Int -> {Int, Filename1}
        end
    end,
    SFiles = lists:sort([F3(X) || X <- filelib:wildcard(join(Root, "segment.*.data"))]),

    %% The next_id will be the maximum number, or 1.
    MaxBNum = lists:max([X || {X, _} <- BFiles] ++ [0]),
    MaxSNum = lists:max([X || {X, _} <- SFiles] ++ [0]),
    NextID = lists:max([MaxBNum, MaxSNum]) + 1,

    read_buf_and_seg_1(Root, NextID, BufferOptions, Locks, BFiles, SFiles, [], []).

read_buf_and_seg_1(Root, NextID, BOptions, Locks, [], [], Buffers, Segments) ->
    %% No latest buffer exists, open a new one...
    BName = join(Root, "buffer." ++ integer_to_list(NextID)),
    Buffer = mi_buffer:open(BName, BOptions),
    NewLocks = mi_locks:claim(mi_buffer:filename(Buffer), fun() -> mi_buffer:delete(Buffer) end, Locks),
    {NextID + 1, [Buffer|Buffers], Segments, NewLocks};
read_buf_and_seg_1(_Root, NextID, BOptions, Locks, [BFile], [], Buffers, Segments) ->
    %% Latest buffer exists, open it...
    {_BNum, BName} = BFile,
    io:format("Loading buffer: '~s'~n", [BName]),
    Buffer = mi_buffer:open(BName, BOptions),
    NewLocks = mi_locks:claim(mi_buffer:filename(Buffer), fun() -> mi_buffer:delete(Buffer) end, Locks),
    {NextID, [Buffer|Buffers], Segments, NewLocks};
read_buf_and_seg_1(Root, NextID, BOptions, Locks, [BFile|BFiles], [], Buffers, Segments) ->
    %% More than one buffer exists, convert to a segment file. This is
    %% a repair situation and means that merge_index was stopped
    %% suddenly.
    {BNum, BName} = BFile,
    SName = join(Root, "segment." ++ integer_to_list(BNum)),
    io:format("Converting buffer: '~s'~n", [BName]),
    Buffer = mi_buffer:open(BName, BOptions),
    mi_buffer:close_filehandle(Buffer),
    Segment = mi_segment:from_buffer(SName, Buffer),
    NewLocks = mi_locks:claim(mi_segment:filename(Segment), Locks),
    mi_buffer:delete(Buffer),
    read_buf_and_seg_1(Root, NextID, BOptions, NewLocks, BFiles, [], Buffers, [Segment|Segments]);
read_buf_and_seg_1(Root, NextID, BOptions, Locks, [BFile|BFiles], [SFile|SFiles], Buffers, Segments) ->
    %% Multiple buffers and segment files exist, process them in
    %% order, converting buffers to segment files as necessary. This
    %% is basically a "repair" situation, and means merge_index was stopped suddenly.
    {BNum, BName} = BFile,
    {SNum, SName} = SFile,
    if 
        BNum < SNum ->
            %% Should not have this case...
            throw({missing_segment_file, element(2, SFile)});
        BNum == SNum ->
            %% Remove and recreate segment...
            file:delete(SName),
            io:format("Converting buffer: '~s'~n", [BName]),
            Buffer = mi_buffer:open(BName, BOptions),
            mi_buffer:close_filehandle(Buffer),
            Segment = mi_segment:from_buffer(SName, Buffer),
            NewLocks = mi_locks:claim(mi_segment:filename(Segment), fun() -> mi_segment:delete(Segment) end, Locks),
            mi_buffer:delete(Buffer),
            read_buf_and_seg_1(Root, NextID, BOptions, NewLocks, BFiles, SFiles, Buffers, [Segment|Segments]);
        BNum > SNum ->
            %% Open the segment and loop...
            io:format("Loading segment: '~s'~n", [SName]),
            Segment = mi_segment:open(SName),
            NewLocks = mi_locks:claim(mi_segment:filename(Segment), fun() -> mi_segment:delete(Segment) end, Locks),
            read_buf_and_seg_1(Root, NextID, BOptions, NewLocks, [BFile|BFiles], SFiles, Buffers, [Segment|Segments])
    end;
read_buf_and_seg_1(_Root, _NextID, _BOptions, _Locks, [], SFiles, _Buffers, _Segments) ->
    %% A segment file exists that is a later generation than all the
    %% buffers. This should never happen, and means someone manually
    %% deleted a buffer.
    throw({too_many_segment_files, SFiles}).

handle_call({index, Index, Field, Term, SubType, SubTerm, Value, Props, TS}, _From, State) ->
    %% Calculate the IFT...
    #state { root=Root, locks=Locks, indexes=Indexes, fields=Fields, terms=Terms, buffers=[CurrentBuffer0|Buffers], next_id=NextID } = State,
    {IndexID, NewIndexes} = mi_incdex:lookup(Index, Indexes),
    {FieldID, NewFields} = mi_incdex:lookup(Field, Fields),
    {TermID, NewTerms} = mi_incdex:lookup(Term, Terms),
    IFT = mi_utils:ift_pack(IndexID, FieldID, TermID, SubType, SubTerm),

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
    case mi_buffer:size(CurrentBuffer) > ?ROLLOVER_SIZE(State) of
        true ->
            %% Start processing the latest buffer.
            mi_buffer:close_filehandle(CurrentBuffer),
            SegNum  = tl(filename:extension(mi_buffer:filename(CurrentBuffer))),
            SegFile = join(Root, "segment." ++ SegNum),
            Segment = mi_segment:open(SegFile),
            NewLocks = mi_locks:claim(mi_segment:filename(Segment), fun() -> mi_segment:delete(Segment) end, Locks),
            Pid = self(),
            spawn_link(fun() ->
                mi_segment:from_buffer(CurrentBuffer, Segment),
                gen_server:call(Pid, {buffer_to_segment, CurrentBuffer, Segment}, infinity)
            end),
            
            %% Create a new empty buffer...
            BName = join(NewState, "buffer." ++ integer_to_list(NextID)),
            BufferOptions = [{write_interval, ?SYNC_INTERVAL(State)}],
            NewBuffer = mi_buffer:open(BName, BufferOptions),
            NewLocks1 = mi_locks:claim(mi_buffer:filename(NewBuffer), fun() -> mi_buffer:delete(NewBuffer) end, NewLocks),
            
            NewState1 = NewState#state {
                locks=NewLocks1,
                buffers=[NewBuffer|NewState#state.buffers],
                next_id=NextID + 1
            },
            {reply, ok, NewState1};
        false ->
            {reply, ok, NewState}
    end;

handle_call({buffer_to_segment, Buffer, Segment}, _From, State) ->
    #state { locks=Locks, buffers=Buffers, segments=Segments, is_compacting=IsCompacting } = State,

    %% Delete the buffer...
    NewLocks = mi_locks:release(mi_buffer:filename(Buffer), Locks),
    NewSegments = [Segment|Segments],

    %% Update state...
    NewState = State#state {
        locks=NewLocks,
        buffers=Buffers -- [Buffer],
        segments=NewSegments
    },

    case not IsCompacting andalso length(NewSegments) > 3 of
        true ->
            %% Create the new compaction segment...
            {StartNum, EndNum} = get_id_range(NewSegments),
            SegFile = join(State, io_lib:format("_segment.~p-~p", [StartNum, EndNum])),
            CompactSegment = mi_segment:open(SegFile),
            
            %% Spawn a function to merge a bunch of segments into one...
            Pid = self(),
            spawn_link(fun() ->
                StartIFT = mi_utils:ift_pack(0, 0, 0, 0, 0),
                EndIFT = all,
                SegmentIterators = [mi_segment:iterator(StartIFT, EndIFT, X) || X <- NewSegments],
                GroupIterator = build_group_iterator(SegmentIterators),
                mi_segment:from_iterator(GroupIterator, CompactSegment),
                gen_server:call(Pid, {compacted, CompactSegment, NewSegments}, infinity)
            end),
            {reply, ok, NewState#state { is_compacting=true }};
        false ->
            {reply, ok, NewState}
    end;

handle_call({compacted, CompactSegment, OldSegments}, _From, State) ->
    #state { locks=Locks, segments=Segments } = State,

    %% Rename the segment to indicate that it is done...
    Root = mi_segment:filename(CompactSegment),
    NewRoot = filename:join(filename:dirname(Root), tl(filename:basename(Root))),
    ok = file:rename(mi_segment:data_file(Root), mi_segment:data_file(NewRoot)),
    ok = file:rename(mi_segment:offsets_file(Root), mi_segment:offsets_file(NewRoot)),

    CompactSegment1 = {segment, NewRoot, element(3, CompactSegment)},
    NewLocks = mi_locks:claim(mi_segment:filename(CompactSegment1), fun() -> mi_segment:delete(CompactSegment1) end, Locks),

    %% Release locks on all of the segments...
    F = fun(X, Acc) ->
        mi_locks:release(mi_segment:filename(X), Acc)
    end,
    NewLocks1 = lists:foldl(F, NewLocks, OldSegments),

    %% Update State and return...
    NewState = State#state {
        locks=NewLocks1,
        segments=[CompactSegment1|(Segments -- OldSegments)],
        is_compacting=false
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
        gen_server:call(Self, {stream_finished, Buffers}, infinity)
    end),

    %% Reply...
    {reply, ok, State#state { locks=NewLocks1 }};

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
    #state { indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    
    %% Delete files, reset state...
    [mi_buffer:delete(X) || X <- Buffers],
    [mi_segment:delete(X) || X <- Segments],
    BufferFile = join(State, "buffer.1"),
    Buffer = mi_buffer:open(BufferFile),
    NewLocks = mi_locks:claim(mi_buffer:filename(Buffer), fun() -> mi_buffer:delete(Buffer) end, mi_locks:new()),
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
    {_IndexID, _FieldID, _TermID, SubType, SubTerm} = mi_utils:ift_unpack(IFT),
    NewProps = [{subterm, {SubType, SubTerm}}|Props],
    case (not IsDuplicate) of
        true  -> F(IFT, Value, NewProps, TS);
        false -> skip
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

fold(_Fun, Acc, eof) -> 
    Acc;
fold(Fun, Acc, {Term, IteratorFun}) ->
    fold(Fun, Fun(Term, Acc), IteratorFun()).

join(#state { root=Root }, Name) ->
    join(Root, Name);

join(Root, Name) ->
    filename:join([Root, Name]).
