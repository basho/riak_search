%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

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
    scheduled_compaction,
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
        scheduled_compaction = false,
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
    Segment = mi_segment:open_read(SName),
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
    Buffer = mi_buffer:open(BName, BufferOptions),
    mi_buffer:close_filehandle(Buffer),
    Size = mi_buffer:size(Buffer),
    SegmentWO = mi_segment:open_write(SName, Size, Size),
    mi_segment:from_buffer(Buffer, SegmentWO),
    mi_buffer:delete(Buffer),
    clear_deleteme_flag(mi_segment:filename(SegmentWO)),
    SegmentRO = mi_segment:open_read(SName),
    
    %% Loop...
    read_buffers(Root, BufferOptions, Rest, NextID, [SegmentRO|Segments]).


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

    %% Add a small delay if we are sitting on too many segments. This
    %% provides a sort of automated throttling, giving the compactor
    %% time to merge segments.
    %% TODO - Make this configurable. It would be useful to
    %% turn this off for bulk loading.
    NumSegments = length(State#state.segments),
    case NumSegments > 10 of
        true ->
            Delay = 1 + trunc(NumSegments / 10),
            timer:sleep(Delay);
        false -> 
            ignore
    end,

    %% Possibly dump buffer to a new segment...
    case mi_buffer:filesize(CurrentBuffer) > ?ROLLOVER_SIZE(NewState) of
        true ->
            #state { root=Root, next_id=NextID } = NewState,
            
            %% Close the buffer filehandle. Needs to be done in the owner process.
            mi_buffer:close_filehandle(CurrentBuffer),
            Pid = self(),
            
            spawn_link(fun() ->
                %% Calculate the segment filename, open the segment, and convert.
                SNum  = get_id_number(mi_buffer:filename(CurrentBuffer)),
                SName = join(Root, "segment." ++ integer_to_list(SNum)),
                set_deleteme_flag(SName),
                Size = mi_buffer:size(CurrentBuffer),
                SegmentWO = mi_segment:open_write(SName, Size, Size),
                mi_segment:from_buffer(CurrentBuffer, SegmentWO),
                SegmentRO = mi_segment:open_read(SName),
                gen_server:call(Pid, {buffer_to_segment, CurrentBuffer, SegmentRO}, infinity)
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

handle_call({buffer_to_segment, Buffer, SegmentWO}, _From, State) ->
    #state { locks=Locks, buffers=Buffers, segments=Segments, scheduled_compaction=ScheduledCompaction } = State,

    %% Clean up by clearing delete flag on the segment, adding delete
    %% flag to the buffer, and telling the system to delete the buffer
    %% as soon as the last lock is released.
    clear_deleteme_flag(mi_segment:filename(SegmentWO)),
    BName = mi_buffer:filename(Buffer),
    set_deleteme_flag(BName),
    NewLocks = mi_locks:when_free(BName, fun() -> mi_buffer:delete(Buffer) end, Locks),

    %% Open the segment as read only...
    SegmentRO = mi_segment:open_read(mi_segment:filename(SegmentWO)),

    %% Update state...
    NewSegments = [SegmentRO|Segments],
    NewState1 = State#state {
        locks=NewLocks,
        buffers=Buffers -- [Buffer],
        segments=NewSegments
    },

    %% Give us the opportunity to do a merge...
    case length(get_segments_to_merge(NewSegments)) of
        Num when Num =< 2 orelse ScheduledCompaction == true-> 
            NewState2 = NewState1;
        _ -> 
            mi_scheduler:schedule_compaction(self()),
            NewState2 = NewState1#state { scheduled_compaction=true }
    end,
    
    %% Return.
    {reply, ok, NewState2};

handle_call({start_compaction, CallingPid}, _From, State) 
  when State#state.is_compacting == true orelse length(State#state.segments) =< 5 ->
    %% Don't compact if we are already compacting, or if we have fewer
    %% than four open segments.
    Ref = make_ref(),
    CallingPid ! {compaction_complete, Ref},
    {reply, {ok, Ref}, State#state { scheduled_compaction=false }};

handle_call({start_compaction, CallingPid}, _From, State) ->
    %% Get list of segments to compact. Do this by getting filesizes,
    %% and then lopping off the four biggest files. This could be
    %% optimized with tuning, but probably a good enough solution.
    Segments = State#state.segments,
    SegmentsToCompact = get_segments_to_merge(Segments),
    
    %% Spawn a function to merge a bunch of segments into one...
    Pid = self(),
    CallingRef = make_ref(),
    spawn_link(fun() ->
        %% Create the group iterator...
        process_flag(priority, high),
        SegmentIterators = [mi_segment:iterator(X) || X <- SegmentsToCompact],
        GroupIterator = build_group_iterator(SegmentIterators),

        %% Create the new compaction segment...
        <<MD5:128/integer>> = erlang:md5(term_to_binary({now, make_ref()})),
        SName = join(State, io_lib:format("segment.~.16B", [MD5])),
        set_deleteme_flag(SName),

        MinSize = lists:min([mi_segment:size(X) || X <- SegmentsToCompact]),
        MaxSize = lists:max([mi_segment:size(X) || X <- SegmentsToCompact]) * 2,
        CompactSegment = mi_segment:open_write(SName, MinSize, MaxSize),
        
        %% Run the compaction...
        mi_segment:from_iterator(GroupIterator, CompactSegment),
        gen_server:call(Pid, {compacted, CompactSegment, SegmentsToCompact, CallingPid, CallingRef}, infinity)
    end),
    {reply, {ok, CallingRef}, State#state { is_compacting=true }};

handle_call({compacted, CompactSegmentWO, OldSegments, CallingPid, CallingRef}, _From, State) ->
    #state { locks=Locks, segments=Segments } = State,

    %% Clean up. Remove delete flag on the new segment. Add delete
    %% flags to the old segments. Register to delete the old segments
    %% when the locks are freed.
    clear_deleteme_flag(mi_segment:filename(CompactSegmentWO)),

    %% Open the segment as read only...
    CompactSegmentRO = mi_segment:open_read(mi_segment:filename(CompactSegmentWO)),

    [set_deleteme_flag(mi_segment:filename(X)) || X <- OldSegments],
    F = fun(X, Acc) ->
        mi_locks:when_free(mi_segment:filename(X), fun() -> mi_segment:delete(X) end, Acc)
    end,
    NewLocks = lists:foldl(F, Locks, OldSegments),

    %% Update State and return...
    NewState = State#state {
        locks=NewLocks,
        segments=[CompactSegmentRO|(Segments -- OldSegments)],
        scheduled_compaction=false,
        is_compacting=false
    },

    %% Tell the awaiting process that we've finished compaction.
    CallingPid ! {compaction_complete, CallingRef},
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
    Counts1 = [F(X) || X <- TermIDs],
    Counts2 = [{Term, Count} || {Term, Count} <- Counts1, Count > 0],
    {reply, {ok, Counts2}, State};

handle_call({stream, Index, Field, Term, Pid, Ref, FilterFun}, _From, State) ->
    %% Get the IDs...
    #state { locks=Locks, indexes=Indexes, fields=Fields, terms=Terms, buffers=Buffers, segments=Segments } = State,
    IndexID = mi_incdex:lookup_nocreate(Index, Indexes),
    FieldID = mi_incdex:lookup_nocreate(Field, Fields),
    TermID = mi_incdex:lookup_nocreate(Term, Terms),

    %% TODO - These next two lines are indicitive of vestigal code
    %% from when merge_index used the fated subtype/subterm
    %% structure. We no longer need a StartIFT and EndIFT, we just
    %% need the IFT to stream. As a result, there are simplifications
    %% we could make in mi_buffer and mi_segment. Not making them now
    %% due to time pressure. - RTK
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
                true -> 
                    Pid!{result, {Value, Props}, Ref};
                _ -> 
                    skip
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
        Begin = mi_utils:ift_pack(0, 0, 0),
        End = all,
        BufferIterator = mi_buffer:iterator(Begin, End, Buffer),
        fold(WrappedFun, AccIn, BufferIterator())
    end,
    Acc1 = lists:foldl(F1, Acc, Buffers),

    %% Fold over each segment...
    F2 = fun(Segment, AccIn) ->
        Begin = mi_utils:ift_pack(0, 0, 0),
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

%% Figure out which files to merge. Take the average of file sizes,
%% return anything smaller than the average for merging.
get_segments_to_merge(Segments) ->
    %% Get all segment sizes...
    F1 = fun(X) ->
        {ok, FileInfo} = file:read_file_info(mi_segment:data_file(X)),
        {FileInfo#file_info.size, X}
    end,
    Sizes = [F1(X) || X <- Segments],
    Avg = lists:sum([Size || {Size, _} <- Sizes]) / length(Segments),

    %% Return segments to merge...
    F2 = fun({Size, Segment}, Acc) ->
        case Size < Avg of
            true -> [Segment|Acc];
            false -> Acc
        end
    end,
    lists:foldl(F2, [], Sizes).

fold(_Fun, Acc, eof) -> 
    Acc;
fold(Fun, Acc, {Term, IteratorFun}) ->
    fold(Fun, Fun(Term, Acc), IteratorFun()).

join(#state { root=Root }, Name) ->
    join(Root, Name);

join(Root, Name) ->
    filename:join([Root, Name]).
