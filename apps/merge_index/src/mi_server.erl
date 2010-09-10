%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
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
%% Private spawn exports
-export([
         buffer_converter/2
        ]).

-record(state, { 
    root,
    locks,
    indexes,
    fields,
    terms,
    segments,
    buffers,
    next_id,
    scheduled_compaction,
    is_compacting,
    buffer_converter
}).

-define(RESULTVEC_SIZE, 1000).
-define(DELETEME_FLAG, ".deleted").

init([Root]) ->
    %% Load from disk...
    filelib:ensure_dir(join(Root, "ignore")),
    io:format("Loading merge_index from '~s'~n", [Root]),
    {NextID, Buffer, Segments} = read_buf_and_seg(Root),
    io:format("Finished loading merge_index from '~s'~n", [Root]),

    %% trap exits so compaction and stream/range spawned processes
    %% don't pull down this merge_index if they fail
    process_flag(trap_exit, true),

    %% Use a dedicated worker sub-process to do the 
    Converter = spawn_link(?MODULE, buffer_converter, [self(), Root]),

    %% Create the state...
    State = #state {
        root     = Root,
        locks    = mi_locks:new(),
        buffers  = [Buffer],
        segments = Segments,
        next_id  = NextID,
        scheduled_compaction = false,
        is_compacting = false,
        buffer_converter = Converter
    },

    %% Return.
    {ok, State}.

%% Return {Buffers, Segments}, after cleaning up/repairing any partially merged buffers.
read_buf_and_seg(Root) ->
    %% Delete any files that have a ?DELETEME_FLAG flag. This means that
    %% the system stopped before proper cleanup.
    io:format("Cleaning up...~n"),
    F1 = fun(Filename) ->
        Basename = filename:basename(Filename, ?DELETEME_FLAG),
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
    {NextID1, Buffer, Segments1} = read_buffers(Root, BufferFiles1, NextID, Segments),
    
    %% Return...
    {NextID1, Buffer, Segments1}.

read_segments([], _Segments) -> [];
read_segments([SName|Rest], Segments) ->
    %% Read the segment from disk...
    io:format("Opening segment: '~s'~n", [SName]),
    Segment = mi_segment:open_read(SName),
    [Segment|read_segments(Rest, Segments)].

read_buffers(Root, [], NextID, Segments) ->
    %% No latest buffer exists, open a new one...
    BName = join(Root, "buffer." ++ integer_to_list(NextID)),
    io:format("Opening new buffer: '~s'~n", [BName]),
    Buffer = mi_buffer:new(BName),
    {NextID + 1, Buffer, Segments};

read_buffers(_Root, [{_BNum, BName}], NextID, Segments) ->
    %% This is the final buffer file... return it as the open buffer...
    io:format("Opening buffer: '~s'~n", [BName]),
    Buffer = mi_buffer:new(BName),
    {NextID, Buffer, Segments};

read_buffers(Root, [{BNum, BName}|Rest], NextID, Segments) ->
    %% Multiple buffers exist... convert them into segments...
    io:format("Converting buffer: '~s' to segment.~n", [BName]),
    SName = join(Root, "segment." ++ integer_to_list(BNum)),
    set_deleteme_flag(SName),
    Buffer = mi_buffer:new(BName),
    mi_buffer:close_filehandle(Buffer),
    SegmentWO = mi_segment:open_write(SName),
    mi_segment:from_buffer(Buffer, SegmentWO),
    mi_buffer:delete(Buffer),
    clear_deleteme_flag(mi_segment:filename(SegmentWO)),
    SegmentRO = mi_segment:open_read(SName),
    
    %% Loop...
    read_buffers(Root, Rest, NextID, [SegmentRO|Segments]).


handle_call({index, Postings}, _From, State) ->
    %% Write to the buffer...
    #state { buffers=[CurrentBuffer0|Buffers] } = State,
    %% Index, Field, Term, Value, Props, TS
    CurrentBuffer = mi_buffer:write(Postings, CurrentBuffer0),

    %% Update the state...
    NewState = State#state {buffers = [CurrentBuffer | Buffers]},

    %% Possibly dump buffer to a new segment...
    {ok, RolloverSize} = application:get_env(merge_index, buffer_rollover_size),
    case mi_buffer:filesize(CurrentBuffer) > RolloverSize of
        true ->
            #state { next_id=NextID } = NewState,
            
            %% Close the buffer filehandle. Needs to be done in the owner process.
            mi_buffer:close_filehandle(CurrentBuffer),
            
            State#state.buffer_converter ! {convert, CurrentBuffer},
            
            %% Create a new empty buffer...
            BName = join(NewState, "buffer." ++ integer_to_list(NextID)),
            NewBuffer = mi_buffer:new(BName),
            
            NewState1 = NewState#state {
                buffers=[NewBuffer|NewState#state.buffers],
                next_id=NextID + 1
            },
            {reply, ok, NewState1};
        false ->
            {reply, ok, NewState}
    end;

handle_call(start_compaction, _From, State) 
  when is_tuple(State#state.is_compacting) orelse length(State#state.segments) =< 5 ->
    %% Don't compact if we are already compacting, or if we have fewer
    %% than four open segments.
    {reply, {ok, 0, 0}, State#state { scheduled_compaction=false }};

handle_call(start_compaction, From, State) ->
    %% Get list of segments to compact. Do this by getting filesizes,
    %% and then lopping off files larger than the average. This could be
    %% optimized with tuning, but probably a good enough solution.
    Segments = State#state.segments,
    {ok, MaxSegments} = application:get_env(merge_index, max_compact_segments),
    SegmentsToCompact = case get_segments_to_merge(Segments) of
                            STC when length(STC) > MaxSegments ->
                                lists:sublist(STC, MaxSegments);
                            STC ->
                                STC
                        end,
    BytesToCompact = lists:sum([mi_segment:filesize(X) || X <- SegmentsToCompact]),
    
    %% Spawn a function to merge a bunch of segments into one...
    Pid = self(),
    CompactingPid = spawn_opt(fun() ->
        %% Create the group iterator...
        SegmentIterators = [mi_segment:iterator(X) || X <- SegmentsToCompact],
        GroupIterator = build_iterator_tree(SegmentIterators, fun mi_utils:term_compare_fun/2),

        %% Create the new compaction segment...
        <<MD5:128/integer>> = erlang:md5(term_to_binary({now, make_ref()})),
        SName = join(State, io_lib:format("segment.~.16B", [MD5])),
        set_deleteme_flag(SName),
        CompactSegment = mi_segment:open_write(SName),
        
        %% Run the compaction...
        mi_segment:from_iterator(GroupIterator, CompactSegment),
        gen_server:cast(Pid, {compacted, CompactSegment, SegmentsToCompact, BytesToCompact, From})
    end, [link, {fullsweep_after, 0}]),
    {noreply, State#state { is_compacting={From, CompactingPid} }};

%% Instead of count, return an actual weight. The weight will either
%% be 0 (if the term is in a block with other terms) or the size of
%% the block if the block is devoted to the index alone. This will
%% make query planning go as expected.
handle_call({info, Index, Field, Term}, _From, State) ->
    %% Calculate the IFT...
    #state { segments=Segments } = State,

    %% Look up the weights in segments. 
    SegmentCount = [mi_segment:info(Index, Field, Term, X) || X <- Segments],
    Counts = [{Term, lists:max([0|SegmentCount])}],
    
    %% Return...
    {reply, {ok, Counts}, State};


%% Handle this later. We need a range. Roll through the index to get
%% the size from the first to the last found term in the index. Use
%% this range for query planning. Again, this will make query planning 
%% go as expected.
%% handle_call({info_range, Index, Field, StartTerm, EndTerm, _Size}, _From, State) ->
%%     %% TODO: Why do we need size here?
%%     %% Get the IDs...
%%     #state { buffers=Buffers, segments=Segments } = State,

%%     %% For each term, determine the number of entries
%%     Total = 
%%         lists:sum([mi_buffer:info_range(Index, Field, StartTerm, EndTerm, X) || X <- Buffers]) +
%%         lists:sum([mi_segment:info_range(Index, Field, StartTerm, EndTerm, X) || X <- Segments]),

%%     F = fun({Term, IFT}, Acc) ->
%%                 case Total > 0 of
%%                     true ->
%%                         [{Term, Total} | Acc];
%%                     false ->
%%                         Acc
%%                 end
%%         end,
%%     Counts = lists:reverse(mi_ift_server:fold_ifts(Index, Field, StartTerm, EndTerm, F, [])),
%%     {reply, {ok, Counts}, State};

handle_call({stream, Index, Field, Term, Pid, Ref, FilterFun}, _From, State) ->
    %% Get the IDs...
    #state { locks=Locks, buffers=Buffers, segments=Segments } = State,

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
    %% SLF TODO: How does caller know if worker proc has crashed?
    %%     TODO: If filter fun or other crashes, linked server
    %%           also crashes.
    Self = self(),
    spawn_link(fun() ->
                       F = fun(Results) ->
                                   WrappedFilter = fun({Value, Props}) -> 
                                                           FilterFun(Value, Props) == true
                                                   end,
                                   case lists:filter(WrappedFilter, Results) of
                                       [] -> 
                                           skip;
                                       FilteredResults ->
                                           Pid ! {result_vec, FilteredResults, Ref}
                                   end
                           end,
                       stream(Index, Field, Term, F, Buffers, Segments),
                       Pid ! {result, '$end_of_table', Ref},
                       gen_server:call(Self, {stream_or_range_finished, Buffers, Segments}, infinity)
               end),

    {reply, ok, State#state { locks=NewLocks1 }};
    
handle_call({range, Index, Field, StartTerm, EndTerm, Size, Pid, Ref, FilterFun}, _From, State) ->
    #state { locks=Locks, buffers=Buffers, segments=Segments } = State,

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

    Self = self(),
    spawn_link(fun() ->
                       F = fun(Results) ->
                                   WrappedFilter = fun({Value, Props}) -> 
                                                           FilterFun(Value, Props) == true
                                                   end,
                                   case lists:filter(WrappedFilter, Results) of
                                       [] -> 
                                           skip;
                                       FilteredResults ->
                                           Pid ! {result_vec, FilteredResults, Ref}
                                   end
                           end,
                       range(Index, Field, StartTerm, EndTerm, Size, F, Buffers, Segments),
                       Pid ! {result, '$end_of_table', Ref},
                       gen_server:call(Self, {stream_or_range_finished, Buffers, Segments}, infinity)
               end),

    {reply, ok, State#state { locks=NewLocks1 }};
    
handle_call({stream_or_range_finished, Buffers, Segments}, _From, State) ->
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


handle_call({fold, FoldFun, Acc}, _From, State) ->
    #state { buffers=Buffers, segments=Segments } = State,

    %% Wrap the FoldFun so that we have a chance to do IndexID /
    %% FieldID / TermID lookups
    WrappedFun = fun({Index, Field, Term, Value, Props, TS}, AccIn) ->
        %% Call the fold function...
        FoldFun(Index, Field, Term, Value, Props, TS, AccIn)
    end,

    %% Assemble the group iterator...
    BufferIterators = [mi_buffer:iterator(X) || X <- Buffers],
    SegmentIterators = [mi_segment:iterator(X) || X <- Segments],
    GroupIterator = build_iterator_tree(BufferIterators ++ SegmentIterators, fun mi_utils:term_compare_fun/2),

    %% Fold over everything...
    %% SLF TODO: If filter fun or other crashes, server crashes.
    NewAcc = fold(WrappedFun, Acc, GroupIterator()),

    %% Reply...
    {reply, {ok, NewAcc}, State};
    
handle_call(is_empty, _From, State) ->
    %% Check if we have buffer data...
    case State#state.buffers of
        [] -> 
            HasBufferData = false;
        [Buffer] ->
            HasBufferData = mi_buffer:size(Buffer) > 0;
        _ ->
            HasBufferData = true
    end,

    %% Check if we have segment data.
    HasSegmentData = length(State#state.segments) > 0,

    %% Return.
    IsEmpty = (not HasBufferData) andalso (not HasSegmentData),
    {reply, IsEmpty, State};

handle_call(drop, _From, State) ->
    #state { buffers=Buffers, segments=Segments } = State,

    %% Delete files, reset state...
    [mi_buffer:delete(X) || X <- Buffers],
    [mi_segment:delete(X) || X <- Segments],
    BufferFile = join(State, "buffer.1"),
    Buffer = mi_buffer:new(BufferFile),
    NewState = State#state { locks = mi_locks:new(),
                             buffers = [Buffer],
                             segments = [] },
    {reply, ok, NewState};

handle_call(Request, _From, State) ->
    ?PRINT({unhandled_call, Request}),
    {reply, ok, State}.

handle_cast({compacted, CompactSegmentWO, OldSegments, OldBytes, From}, State) ->
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
    gen_server:reply(From, {ok, length(OldSegments), OldBytes}),
    {noreply, NewState};

handle_cast({buffer_to_segment, Buffer, SegmentWO}, State) ->
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
    SegmentsToMerge = get_segments_to_merge(NewSegments),
    case length(SegmentsToMerge) of
        Num when Num =< 2 orelse ScheduledCompaction == true-> 
            NewState2 = NewState1;
        _ -> 
            mi_scheduler:schedule_compaction(self()),
            NewState2 = NewState1#state { scheduled_compaction=true }
    end,
    {noreply, NewState2};

handle_cast(Msg, State) ->
    ?PRINT({unhandled_cast, Msg}),
    {noreply, State}.

handle_info({'EXIT', CompactingPid, Reason},
            #state{is_compacting={From, CompactingPid}}=State) ->
    %% the spawned compaction process exited
    case Reason of
        normal ->
            %% compaction finished normally: nothing to be done
            %% handle_call({compacted... already sent the reply
            ok;
        _ ->
            %% compaction failed: not too much to worry about
            %% (it should be safe to try again later)
            %% but we need to let the compaction-requester know
            %% that we're not compacting any more
            gen_server:reply(From, {error, Reason})
    end,

    %% clear out compaction flags, so we try again when necessary
    {noreply, State#state{is_compacting=false,
                          scheduled_compaction=false}};
handle_info({'EXIT', ConverterPid, _Reason},
            #state{buffer_converter=ConverterPid,
                   buffers=Buffers,
                   root=Root}=State) ->
    %% the buffer converter died - start a new one
    NewConverter = spawn_link(?MODULE, buffer_converter, [self(), Root]),
    
    %% current buffer is hd(Buffers), so just convert tl(Buffers)
    [ NewConverter ! {convert, B} || B <- tl(Buffers) ],

    {noreply, State#state{buffer_converter = NewConverter}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Merge-sort the results from Iterators, and stream to the pid.
stream(Index, Field, Term, F, Buffers, Segments) ->
    %% Put together the group iterator...
    BufferIterators = [mi_buffer:iterator(Index, Field, Term, X) || X <- Buffers],
    SegmentIterators = [mi_segment:iterator(Index, Field, Term, X) || X <- Segments],
    GroupIterator = build_iterator_tree(BufferIterators ++ SegmentIterators, fun mi_utils:value_compare_fun/2),

    %% Start streaming...
    stream_or_range_inner(F, undefined, GroupIterator(), []),
    ok.

range(Index, Field, StartTerm, EndTerm, Size, F, Buffers, _Segments) ->
    %% Put together the group iterator...
    BufferIterators = lists:flatten([mi_buffer:iterators(Index, Field, StartTerm, EndTerm, Size, X) || X <- Buffers]),
    %% SegmentIterators = lists:flatten([mi_segment:iterators(Index, Field, Term, X) || X <- Segments]),
    SegmentIterators = [],
    GroupIterator = build_iterator_tree(BufferIterators ++ SegmentIterators, fun mi_utils:value_compare_fun/2),

    %% Start rangeing...
    stream_or_range_inner(F, undefined, GroupIterator(), []),
    ok.

stream_or_range_inner(F, LastValue, Iterator, Acc) 
  when length(Acc) > ?RESULTVEC_SIZE ->
    F(lists:reverse(Acc)),
    stream_or_range_inner(F, LastValue, Iterator, []);
stream_or_range_inner(F, LastValue, {{Value, Props, _TS}, Iter}, Acc) ->
    IsDuplicate = (LastValue == Value),
    IsDeleted = (Props == undefined),
    case (not IsDuplicate) andalso (not IsDeleted) of
        true  -> 
            stream_or_range_inner(F, Value, Iter(), [{Value, Props}|Acc]);
        false -> 
            stream_or_range_inner(F, Value, Iter(), Acc)
    end;
stream_or_range_inner(F, _, eof, Acc) -> 
    F(lists:reverse(Acc)),
    ok.

%% BACKHERE - Pass in the compare fun and use a different one for
%% different circumstances. We *don't* want to use the
%% Index/Field/Term when doing a range search, but we do want it for
%% building super-segments as well as sorting a dumped buffer.

%% Chain a list of iterators into what looks like one single iterator.
build_iterator_tree([], _CompareFun) ->
    fun() -> eof end;
build_iterator_tree(Iterators, CompareFun) ->
    case build_iterator_tree_inner(Iterators, CompareFun) of
        [OneIterator] -> OneIterator;
        ManyIterators -> build_iterator_tree(ManyIterators, CompareFun)
    end.
build_iterator_tree_inner([], _) ->
    [];
build_iterator_tree_inner([Iterator], _) ->
    [Iterator];
build_iterator_tree_inner([IteratorA,IteratorB|Rest], CompareFun) ->
    Iterator = fun() -> group_iterator(IteratorA(), IteratorB(), CompareFun) end,
    [Iterator|build_iterator_tree_inner(Rest, CompareFun)].

group_iterator(I1 = {Term1, Iterator1}, I2 = {Term2, Iterator2}, CompareFun) ->
    case CompareFun(Term1, Term2) of
        true ->
            NewIterator = fun() -> group_iterator(Iterator1(), I2, CompareFun) end,
            {Term1, NewIterator};
        false ->
            NewIterator = fun() -> group_iterator(I1, Iterator2(), CompareFun) end,
            {Term2, NewIterator}
    end;
group_iterator(eof, eof, _) -> 
    eof;
group_iterator(eof, Iterator, _) -> 
    Iterator;
group_iterator(Iterator, eof, _) -> 
    Iterator.


buffer_converter(ServerPid, Root) ->
    receive
        {convert, Buffer} ->
            %% Calculate the segment filename, open the segment, and convert.
            SNum  = get_id_number(mi_buffer:filename(Buffer)),
            SName = join(Root, "segment." ++ integer_to_list(SNum)),

            case has_deleteme_flag(SName) of
                true ->
                    %% remove files from a previously-failed conversion
                    file:delete(mi_segment:data_file(SName)),
                    file:delete(mi_segment:offsets_file(SName));
                false ->
                    set_deleteme_flag(SName)
            end,

            SegmentWO = mi_segment:open_write(SName),
            mi_segment:from_buffer(Buffer, SegmentWO),
            gen_server:cast(ServerPid, {buffer_to_segment, Buffer, SegmentWO}),
            ?MODULE:buffer_converter(ServerPid, Root)
    end.

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
    file:write_file(Filename ++ ?DELETEME_FLAG, "").

clear_deleteme_flag(Filename) ->
    file:delete(Filename ++ ?DELETEME_FLAG).

has_deleteme_flag(Filename) ->
    filelib:is_file(Filename ++ ?DELETEME_FLAG).

%% Figure out which files to merge. Take the average of file sizes,
%% return anything smaller than the average for merging.
get_segments_to_merge(Segments) ->
    %% Get all segment sizes...
    F1 = fun(X) ->
        Size = mi_segment:filesize(X),
        {Size, X}
    end,
    SortedSizedSegments = lists:sort([F1(X) || X <- Segments]),
    
    %% Calculate the average...
    Avg = lists:sum([Size || {Size, _} <- SortedSizedSegments]) div length(Segments) + 1024,

    %% Return segments less than average...
    [Segment || {Size, Segment} <- SortedSizedSegments, Size < Avg].

%% get_average_segment_size(Segments) ->
%%     TotalSize = lists:sum([mi_segment:filesize(X) || X <- Segments]),
%%     TotalSize / length(Segments).

fold(_Fun, Acc, eof) -> 
    Acc;
fold(Fun, Acc, {Term, IteratorFun}) ->
    fold(Fun, Fun(Term, Acc), IteratorFun()).

join(#state { root=Root }, Name) ->
    join(Root, Name);

join(Root, Name) ->
    filename:join([Root, Name]).
