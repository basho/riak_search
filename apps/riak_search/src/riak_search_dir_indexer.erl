%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_dir_indexer).

-include_lib("kernel/include/file.hrl").

-export([start_index/2, start_index/3,
         index/2, index/3,
         stop_index/1]).

-record(index_status, { total_files = 0,
                        total_bytes = 0,
                        processed_files = 0,
                        processed_bytes = 0,
                        start_time,
                        last_status_time }).

-define(STATUS_INTERVAL, 10000).                % Milliseconds between status callbacks
-define(WORKER_TIMEOUT, 2000).

%% ===================================================================
%% API functions
%% ===================================================================

start_index(Index, Dir) ->
    start_index(Index, Dir, fun console_status/1).

start_index(Index, Dir, StatusFn) ->
    %% Spawn master worker; do _not_ link to calling process to avoid
    %% issues when running on console
    Master = spawn(fun() -> index_master_loop0(Index, Dir, StatusFn) end),
    Master.

index(Index, Dir) ->
    index(Index, Dir, fun console_status/1).
    
index(Index, Dir, StatusFun) ->
    Master = start_index(Index, Dir, StatusFun),
    index_block_loop(Master).

index_block_loop(Master) ->
    case is_process_alive(Master) of
        true -> 
            timer:sleep(1000),
            index_block_loop(Master);
        false -> ok
    end.

stop_index(MasterPid) ->
    Mref = erlang:monitor(process, MasterPid),
    MasterPid ! stop,
    receive
        {'DOWN', Mref, process, MasterPid, _} ->
            ok
    end.



%% ===================================================================
%% Internal functions
%% ===================================================================

console_status(done) ->
    io:format("Finished indexing.\n");
console_status(Status) ->
    ElapsedSecs = timer:now_diff(now(), Status#index_status.start_time) / 1000000,
    AvgKbSec = (Status#index_status.processed_bytes / ElapsedSecs) / 1024,
    TotalBytes = if Status#index_status.total_bytes == 0 -> 1;
                    true                                 -> Status#index_status.total_bytes
                 end,
    Percent = (Status#index_status.processed_bytes / TotalBytes) * 100,
    io:format("Indexer ~p - ~.1f % - ~.1f KB/sec - ~w files - ~w seconds\n",
              [self(), Percent, AvgKbSec, Status#index_status.processed_files,
               trunc(ElapsedSecs)]).


list_dir(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    [filename:join(Dir, F) || F <- Files].

count_files([], FileCount, ByteCount) ->
    {FileCount, ByteCount};
count_files([F | Rest], FileCount, ByteCount) ->
    case file:read_file_info(F) of
        {ok, I} ->
            case I#file_info.type of
                directory ->
                    {NewFileCount, NewByteCount} = count_files(list_dir(F), FileCount, ByteCount),
                    count_files(Rest, NewFileCount, NewByteCount);
                regular ->
                    count_files(Rest, FileCount+1, ByteCount + I#file_info.size);
                _ ->
                    count_files(Rest, FileCount, ByteCount)
            end;
        {error, _} ->
            count_files(Rest, FileCount, ByteCount)
    end.

index_master_loop0(Index, Dir, StatusFn) ->
    %% Determine basic info about the files we are going to index; use this
    %% info later for a meaningful status report
    {TotalFiles, TotalBytes} = count_files(list_dir(Dir), 0, 0),

    %% Get the number of workers we want to use for indexing
    {ok, Workers} = application:get_env(riak_search, dir_index_workers),

    %% Spawn a bunch of workers
    Self = self(),
    WorkerPids = [spawn_link(fun() -> index_worker_loop0(Self, Index) end) || _ <- lists:seq(1,Workers)],

    %% Initialize status and start processing files
    Status = #index_status {total_files = TotalFiles,
                            total_bytes = TotalBytes,
                            start_time  = now(),
                            last_status_time = now()},
    index_master_loop(list_dir(Dir), Status, StatusFn),

    %% Stop all workers.
    [X!stop || X <- WorkerPids],
    StatusFn(done),
    ok.

index_master_loop([], Status, StatusFn) ->
    StatusFn(Status),
    stop;
index_master_loop([FName | Rest], Status, StatusFn) ->
    TimeSinceUpdate = timer:now_diff(now(), Status#index_status.last_status_time) / 1000,
    NewStatus = case TimeSinceUpdate > ?STATUS_INTERVAL of
                    true ->
                        StatusFn(Status),
                        Status#index_status { last_status_time = now() };
                    false ->
                        Status
                end,

    receive
        {next_file, Worker, BytesProcessed} ->
            case filelib:is_dir(FName) of
                false ->
                    Worker ! {file, FName},
                    NewStatus2 = NewStatus#index_status {
                                   processed_files = Status#index_status.processed_files + 1,
                                   processed_bytes = Status#index_status.processed_bytes + BytesProcessed
                                  },
                    index_master_loop(Rest, NewStatus2, StatusFn);

                true ->
                    self() ! {next_file, Worker, BytesProcessed},
                    index_master_loop(list_dir(FName) ++ Rest, NewStatus, StatusFn);

                {error, Reason} ->
                    self() ! {next_file, Worker},
                    error_logger:error_msg("Failed to read file ~p: ~p\n", [FName, Reason]),
                    index_master_loop(Rest, NewStatus#index_status {
                                              processed_files = Status#index_status.processed_files + 1
                                             }, StatusFn)
            end;
        stop ->
            StatusFn(NewStatus),
            stop
    end.

index_worker_loop0(QueuePid, Index) ->
    {ok, Client} = riak_search:local_client(),
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    {ok, IndexFsmPid} = Client:get_index_fsm(),
    {ok, Schema} = riak_search_config:get_schema(Index),
    DefaultField = Schema:default_field(),
    try
        QueuePid ! {next_file, self(), 0},
        index_worker_loop(QueuePid, Client, AnalyzerPid, IndexFsmPid, Index, DefaultField)
    catch Error ->
            error_logger:error_msg("Indexing error: ~p~n", [Error])
    after
        Client:stop_index_fsm(IndexFsmPid),
        qilr:close_analyzer(AnalyzerPid)
    end,
    ok.

index_worker_loop(QueuePid, Client, AnalyzerPid, IndexFsmPid, Index, DefaultField) ->
    receive
        {file, FName} ->
            case file:read_file(FName) of
                {ok, Data} ->
                    DocID = filename:basename(FName),
                    Fields = [{DefaultField, binary_to_list(Data)}],
                    IdxDoc = riak_indexed_doc:new(DocID, Fields, [], Index),
                    Client:index_doc(IdxDoc, AnalyzerPid, IndexFsmPid),
                    QueuePid ! {next_file, self(), size(Data)};

                {error, Reason} ->
                    error_logger:error_msg("Failed to read file ~p: ~p\n", [FName, Reason]),
                    QueuePid ! {next_file, self(), 0}
            end,
            index_worker_loop(QueuePid, Client, AnalyzerPid, IndexFsmPid, Index, DefaultField);
        stop ->
            stop
    after ?WORKER_TIMEOUT ->
            stop
    end.
