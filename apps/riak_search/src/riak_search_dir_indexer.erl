%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_dir_indexer).

-include_lib("kernel/include/file.hrl").
-include("riak_search.hrl").

-export([
    start_index/2
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

start_index(Index, Dir) ->
    OpFun = fun index_fun/3,
    StatusFun = fun console_status/1,
    spawn(fun() -> start_main_loop(Index, Dir, OpFun, StatusFun) end).


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
        start_time=now(),
        last_interval_time=now(),
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
        
        Other ->
            ?PRINT({unhandled_message, Other}),
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
    Interval = timer:now_diff(now(), State#state.last_interval_time) / 1000 / 1000,
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
        last_interval_time=now(),
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
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    worker_loop(OpFunction, Schema, AnalyzerPid, Parent, Ref).

worker_loop(OpFunction, Schema, AnalyzerPid, Parent, Ref) ->
    Parent ! {next_batch, self(), Ref},
    receive
        {next_batch_reply, TotalBytes, Files, Ref} ->
            try 
                OpFunction(Schema, AnalyzerPid, Files)
            catch Type : Error ->
                qilr:close_analyzer(AnalyzerPid),
                ?PRINT({error, Type, Error}),
                erlang:Type(Error)
            end,
            Parent ! {finished_batch, length(Files), TotalBytes, Ref},
            worker_loop(OpFunction, Schema, AnalyzerPid, Parent, Ref);

        {stop, Ref} ->
            qilr:close_analyzer(AnalyzerPid),
            ok
    end.

index_fun(Schema, AnalyzerPid, Files) ->
    F = fun(File) ->
        {ok, Data} = file:read_file(File),
        DocID = riak_search_utils:to_binary(filename:basename(File)),
        Fields = [{Schema:default_field(), Data}],
        riak_indexed_doc:new(Schema:name(), DocID, Fields, [])
    end,
    IdxDocs = [F(X) || X <- Files],
    {ok, Client} = riak_search:local_client(),
    Client:index_docs(IdxDocs, AnalyzerPid),
    ok.

console_status(start) ->
    io:format("Starting indexing...\n");
console_status(finish) ->
    io:format("Finished indexing.\n");
console_status(Status) ->
    ElapsedSecs = timer:now_diff(now(), Status#interval_status.start_time) / 1000 / 1000,
    Interval = Status#interval_status.interval + 1,
    AvgKbSec = (Status#interval_status.interval_bytes / Interval) / 1024,
    TotalBytes = Status#interval_status.total_bytes + 1,
    Percent = (Status#interval_status.processed_bytes / TotalBytes) * 100,
    io:format("Indexer ~p - ~.1f % - ~.1f KB/sec - ~w files - ~w seconds\n",
              [self(), Percent, AvgKbSec, Status#interval_status.processed_files,
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
list_directory(Dir) ->
    %% Recurse through the directory gathering filesize information on
    %% each file.
    F = fun(File, Acc) ->
        {ok, Info} = file:read_file_info(File),
        [{Info#file_info.size, File}|Acc]
    end,
    Files = filelib:fold_files(Dir, ".*", true, F, []),
    Num = length(Files),
    Bytes = lists:sum([element(1, X) || X <- Files]),
    {Num, Bytes, lists:reverse(Files)}.



%% start_index(Index, Dir) ->
%%     start_index(Index, Dir, fun console_status/1).

%% start_index(Index, Dir, StatusFn) ->
%%     %% Spawn master worker; do _not_ link to calling process to avoid
%%     %% issues when running on console
%%     Master = spawn(fun() -> index_master_loop0(Index, Dir, StatusFn) end),
%%     %% eprof:start_profiling([Master]),
%%     Master.

%% index(Index, Dir) ->
%%     index(Index, Dir, fun console_status/1).

%% index(Index, Dir, StatusFun) ->
%%     Master = start_index(Index, Dir, StatusFun),
%%     index_block_loop(Master).

%% index_block_loop(Master) ->
%%     case is_process_alive(Master) of
%%         true -> 
%%             timer:sleep(1000),
%%             index_block_loop(Master);
%%         false -> ok
%%     end.

%% stop_index(MasterPid) ->
%%     Mref = erlang:monitor(process, MasterPid),
%%     MasterPid ! stop,
%%     receive
%%         {'DOWN', Mref, process, MasterPid, _} ->
%%             ok
%%     end.



%% %% ===================================================================
%% %% Internal functions
%% %% ===================================================================

%% console_status(done) ->
%%     io:format("Finished indexing.\n");
%% console_status(Status) ->
%%     ElapsedSecs = timer:now_diff(now(), Status#index_status.start_time) / 1000000,
%%     AvgKbSec = (Status#index_status.processed_bytes / ElapsedSecs) / 1024,
%%     TotalBytes = if Status#index_status.total_bytes == 0 -> 1;
%%                     true                                 -> Status#index_status.total_bytes
%%                  end,
%%     Percent = (Status#index_status.processed_bytes / TotalBytes) * 100,
%%     io:format("Indexer ~p - ~.1f % - ~.1f KB/sec - ~w files - ~w seconds\n",
%%               [self(), Percent, AvgKbSec, Status#index_status.processed_files,
%%                trunc(ElapsedSecs)]).



%% index_master_loop0(Index, Dir, StatusFn) ->
%%     %% Determine basic info about the files we are going to index; use this
%%     %% info later for a meaningful status report
%%     {TotalFiles, TotalBytes} = count_files(list_dir(Dir), 0, 0),

%%     %% Get the number of workers we want to use for indexing
%%     {ok, Workers} = application:get_env(riak_search, dir_index_workers),

%%     %% Spawn a bunch of workers
%%     Self = self(),
%%     WorkerPids = [spawn_link(fun() -> index_worker_loop0(Self, Index) end) || _ <- lists:seq(1,Workers)],

%%     %% Initialize status and start processing files
%%     Status = #index_status {total_files = TotalFiles,
%%                             total_bytes = TotalBytes,
%%                             start_time  = now(),
%%                             last_status_time = now()},
%%     index_master_loop(list_dir(Dir), Status, StatusFn),

%%     %% Stop all workers.
%%     [X!stop || X <- WorkerPids],
%%     StatusFn(done),
%%     ok.

%% index_master_loop([], Status, StatusFn) ->
%%     StatusFn(Status),
%%     stop;
%% index_master_loop([FName | Rest], Status, StatusFn) ->
%%     TimeSinceUpdate = timer:now_diff(now(), Status#index_status.last_status_time) / 1000,
%%     NewStatus = case TimeSinceUpdate > ?STATUS_INTERVAL of
%%                     true ->
%%                         StatusFn(Status),
%%                         Status#index_status { last_status_time = now() };
%%                     false ->
%%                         Status
%%                 end,

%%     receive
%%         {next_file, Worker, BytesProcessed} ->
%%             case filelib:is_dir(FName) of
%%                 false ->
%%                     Worker ! {file, FName},
%%                     NewStatus2 = NewStatus#index_status {
%%                                    processed_files = Status#index_status.processed_files + 1,
%%                                    processed_bytes = Status#index_status.processed_bytes + BytesProcessed
%%                                   },
%%                     index_master_loop(Rest, NewStatus2, StatusFn);

%%                 true ->
%%                     self() ! {next_file, Worker, BytesProcessed},
%%                     index_master_loop(list_dir(FName) ++ Rest, NewStatus, StatusFn);

%%                 {error, Reason} ->
%%                     self() ! {next_file, Worker},
%%                     error_logger:error_msg("Failed to read file ~p: ~p\n", [FName, Reason]),
%%                     index_master_loop(Rest, NewStatus#index_status {
%%                                               processed_files = Status#index_status.processed_files + 1
%%                                              }, StatusFn)
%%             end;
%%         stop ->
%%             StatusFn(NewStatus),
%%             stop
%%     end.

%% index_worker_loop0(QueuePid, Index) ->
%%     {ok, Client} = riak_search:local_client(),
%%     {ok, AnalyzerPid} = qilr:new_analyzer(),
%%     {ok, Schema} = riak_search_config:get_schema(Index),
%%     DefaultField = Schema:default_field(),
%%     try
%%         QueuePid ! {next_file, self(), 0},
%%         index_worker_loop(QueuePid, Client, AnalyzerPid, Index, DefaultField)
%%     catch Error ->
%%             error_logger:error_msg("Indexing error: ~p~n", [Error])
%%     after
%%         qilr:close_analyzer(AnalyzerPid)
%%     end,
%%     ok.

%% index_worker_loop(QueuePid, Client, AnalyzerPid, Index, DefaultField) ->
%%     receive
%%         {file, FName} ->
%%             case file:read_file(FName) of
%%                 {ok, Data} ->
%%                     DocID = riak_search_utils:to_binary(filename:basename(FName)),
%%                     Fields = [{DefaultField, Data}],
%%                     IdxDoc = riak_indexed_doc:new(Index, DocID, Fields, []),
%%                     Client:index_doc(IdxDoc, AnalyzerPid),
%%                     QueuePid ! {next_file, self(), size(Data)};

%%                 {error, Reason} ->
%%                     error_logger:error_msg("Failed to read file ~p: ~p\n", [FName, Reason]),
%%                     QueuePid ! {next_file, self(), 0}
%%             end,
%%             index_worker_loop(QueuePid, Client, AnalyzerPid, Index, DefaultField);
%%         stop ->
%%             stop
%%     after ?WORKER_TIMEOUT ->
%%             stop
%%     end.
