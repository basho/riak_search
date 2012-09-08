%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_dir_indexer).

-include_lib("kernel/include/file.hrl").
-include("riak_search.hrl").

-export([
         start_index/3,
         index/3
]).

-record(state, { 
    index, 
    root_dir, 
    files,
    op_fun, 
    status_fun, 
    ref, 
    stat_timer, 
    worker_pids, 
    start_time,
    last_interval_time,
    processed_files=0, 
    processed_bytes=0, 
    interval_files=0,
    interval_bytes=0,
    total_files=0, 
    total_bytes=0 
}).

-record(interval_status, {
    start_time,
    interval,
    processed_files=0, 
    processed_bytes=0, 
    interval_files=0,
    interval_bytes=0,
    total_files=0, 
    total_bytes=0 
}).

%% ===================================================================
%% API functions
%% ===================================================================

start_index(Index, Dir, OpFun) ->
    StatusFun = fun console_status/1,
    MasterPid = spawn(fun() -> start_main_loop(Index, Dir, OpFun, StatusFun) end),
    MasterPid.

index(Index, Dir, OpFun) ->
    StatusFun = fun console_status/1,
    start_main_loop(Index, Dir, OpFun, StatusFun).

start_main_loop(Index, RootDir, OpFun, StatusFun) ->
    %% Read the total number of bytes/files.
    {TotalFiles,TotalBytes,Files} = list_directory(RootDir),

    %% Start up workers.
    Self = self(),
    Ref = make_ref(),
    {ok, NumWorkers} = application:get_env(riak_search, dir_index_workers),
    WorkerPids = [spawn_link(fun() -> start_worker_loop(Index, OpFun, Self, Ref) end) || _ <- lists:seq(1, NumWorkers)],

    %% On intervals, print stats...
    {ok, StatsInterval} = application:get_env(riak_search, dir_index_stats_interval),
    {ok, StatTimer} = timer:send_interval(StatsInterval * 1000, Self, {status, Ref}),

    %% Create the state and loop.
    State = #state {
        index=Index,
        root_dir=RootDir,
        files=Files,
        op_fun=OpFun,
        status_fun=StatusFun,
        ref=Ref,
        stat_timer=StatTimer,
        worker_pids=WorkerPids,
        start_time=os:timestamp(),
        last_interval_time=os:timestamp(),
        total_files=TotalFiles,
        total_bytes=TotalBytes
    },
    print_stats_start(State),
    main_loop(State).

main_loop(State) when State#state.processed_files < State#state.total_files ->
    Ref = State#state.ref,
    receive
        {status, Ref} -> 
            NewState = print_stats(State),
            main_loop(NewState);

        {next_batch, WorkerPid, Ref} when length(State#state.files) == 0 ->
            WorkerPid ! {stop, Ref},
            main_loop(State);

        {next_batch, WorkerPid, Ref} when length(State#state.files) > 0 ->
            {NumBytes, Files, NewState} = split_next_batch(State),
            WorkerPid ! {next_batch_reply, NumBytes, Files, Ref},
            main_loop(NewState);

        {finished_batch, NumFiles, NumBytes, Ref} ->
            NewState = update_stats(NumFiles, NumBytes, State),
            main_loop(NewState);
        
        _Other ->
            %% ?PRINT({unhandled_message, Other, Ref}),
            main_loop(State)
    end;

main_loop(State) ->
    %% Print the final stats.
    print_stats(State),
    print_stats_finish(State),

    %% Cancel the timer...
    StatTimer = State#state.stat_timer,
    timer:cancel(StatTimer), 

    %% Stop all workers...
    WorkerPids = State#state.worker_pids,
    Ref = State#state.ref,
    [X ! {stop, Ref} || X <- WorkerPids],
    ok.

%% @private Call the supplied StatusFunction.    
print_stats_start(State) ->
    StatusFun = State#state.status_fun,
    StatusFun(start).

print_stats_finish(State) ->
    StatusFun = State#state.status_fun,
    StatusFun(finish).

print_stats(State) ->
    %% Call the status function...
    StatusFun = State#state.status_fun,
    Interval = timer:now_diff(os:timestamp(), State#state.last_interval_time) / 1000 / 1000,
    IntervalStatus = #interval_status {
        start_time = State#state.start_time,
        interval = Interval,
        interval_files = State#state.interval_files,
        interval_bytes = State#state.interval_bytes,
        processed_files = State#state.processed_files,
        processed_bytes = State#state.processed_bytes,
        total_files = State#state.total_files,
        total_bytes = State#state.total_bytes
    },
    StatusFun(IntervalStatus),

    %% Update the status function...
    State#state {
        last_interval_time=os:timestamp(),
        interval_files=0, 
        interval_bytes=0
    }.

split_next_batch(State) ->
    %% Split off the next batch of files, limited by number and size...
    Files = State#state.files,
    {ok, FilesLeft} = application:get_env(riak_search, dir_index_batch_size),
    {ok, BytesLeft} = application:get_env(riak_search, dir_index_batch_bytes),
    {Batch, NewFiles} = split_next_batch_inner(random:uniform(FilesLeft), random:uniform(BytesLeft), Files, []),

    %% Return {TotalBytes, Files, NewState}...
    TotalBytes = lists:sum([element(1, X) || X <- Batch]),
    BatchFiles = [element(2, X) || X <- Batch],
    {TotalBytes, BatchFiles, State#state { files=NewFiles}}.
split_next_batch_inner(FilesLeft, _,  Files, Batch) when FilesLeft == 0 ->
    {Batch, Files};
split_next_batch_inner(_, BytesLeft,  Files, Batch) when BytesLeft =< 0 ->
    {Batch, Files};
split_next_batch_inner(_, _, [], Batch) ->
    {Batch, []};
split_next_batch_inner(FilesLeft, BytesLeft, [{Bytes, File}|Rest], Batch) ->
    split_next_batch_inner(FilesLeft - 1, BytesLeft - Bytes, Rest, [{Bytes, File}|Batch]).

start_worker_loop(Index, OpFunction, Parent, Ref) ->
    {ok, Schema} = riak_search_config:get_schema(Index),
    worker_loop(OpFunction, Schema, Parent, Ref).

worker_loop(OpFunction, Schema, Parent, Ref) ->
    Parent ! {next_batch, self(), Ref},
    receive
        {next_batch_reply, TotalBytes, Files, Ref} ->
            try 
                OpFunction(Schema, Files)
            catch Type : Error ->
                ?PRINT({error, Type, Error, erlang:get_stacktrace()}),
                erlang:Type(Error)
            end,
            Parent ! {finished_batch, length(Files), TotalBytes, Ref},
            worker_loop(OpFunction, Schema, Parent, Ref);

        {stop, Ref} ->
            ok
    end.

console_status(start) ->
    io:format("Starting...\n");
console_status(finish) ->
    io:format("Finished.\n");
console_status(Status) ->
    ElapsedSecs = timer:now_diff(os:timestamp(), Status#interval_status.start_time) / 1000 / 1000,
    Interval = Status#interval_status.interval + 1,
    AvgKbSec = (Status#interval_status.interval_bytes / Interval) / 1024,
    TotalBytes = Status#interval_status.total_bytes + 1,
    Percent = (Status#interval_status.processed_bytes / TotalBytes) * 100,
    io:format("Indexer ~p - ~.1f % - ~.1f KB/sec - ~w/~w files - ~w seconds\n",
              [self(), Percent, AvgKbSec, 
               Status#interval_status.processed_files,
               Status#interval_status.total_files,
               trunc(ElapsedSecs)]).

%% @private Update stats when files are processed.
update_stats(NumFiles, NumBytes, State) ->
    State#state {
        interval_files = NumFiles + State#state.interval_files, 
        interval_bytes = NumBytes + State#state.interval_bytes, 
        processed_files = NumFiles + State#state.processed_files, 
        processed_bytes = NumBytes + State#state.processed_bytes
    }.

%% @private Return {NumFiles, NumBytes, Files} where files is a list
%% of {FileSize, FilePath}.
list_directory(Path) ->
    %% Get list of files. If we've been passed a directory, then
    %% recursively get all files in the directory. Otherwise, treat
    %% the path like a wildcard.
    F1 = fun(File, Acc) -> [File|Acc] end,
    case filelib:fold_files(Path, ".*", true, F1, []) of
        [] -> 
            Files = filelib:wildcard(Path);
        Files -> 
            Files
    end,

    %% Ensure we only have regular files, and get the filesizes.
    F2 = fun(File, Acc) ->
        {ok, Info} = file:read_file_info(File),
        case Info#file_info.type of 
            regular ->
                [{Info#file_info.size, File}|Acc];
            _ ->
                Acc
        end
    end,
    Files1 = lists:foldl(F2, [], Files),

    %% Extract the number and bytes, and return...
    Num = length(Files1),
    Bytes = lists:sum([element(1, X) || X <- Files1]),
    {Num, Bytes, lists:reverse(Files1)}.
