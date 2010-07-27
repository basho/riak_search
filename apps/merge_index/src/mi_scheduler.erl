%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_scheduler).

%% API
-export([
    start_link/0,
    start/0,
    schedule_compaction/2
]).

-include("merge_index.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { queue,
                 worker,
                 worker_ready = false}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

schedule_compaction(AvgSize, Pid) ->
    gen_server:call(?MODULE, {schedule_compaction, AvgSize, Pid}).
    

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits of the actual worker process
    process_flag(trap_exit, true),

    %% Use a dedicated worker sub-process to do the actual merging. The
    %% process may ignore messages for a long while during the compaction
    %% and we want to ensure that our message queue doesn't fill up with
    %% a bunch of dup requests for the same directory.
    Self = self(),
    WorkerPid = spawn_link(fun() -> worker_loop(Self) end),
    {ok, #state{ queue = [],
                 worker = WorkerPid }}.

handle_call({schedule_compaction, AvgSize, Pid}, _From, #state { queue = Q } = State) ->
    %% Run the smaller/quicker compactions first. Goal is to keep
    %% total number of files low.
    Q1 = lists:keydelete(Pid, 2, Q),
    Q2 = lists:sort([{AvgSize, Pid}|Q1]),
    case State#state.worker_ready of
        true -> 
            {_, Pid} = hd(Q2),
            State#state.worker!{compaction, Pid},
            {reply, ok, State#state { queue=tl(Q2), worker_ready=false }};
        false ->
            {reply, ok, State#state { queue=Q2 }}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(worker_ready, #state { queue = Q } = State) ->
    case Q == [] of
        true ->
            {noreply, State#state { worker_ready = true }};
        false ->
            {_, Pid} = hd(Q),
            NewQ = tl(Q),
            State#state.worker ! {compaction, Pid},
            {noreply, State#state { queue = NewQ, worker_ready = false }}
    end;
handle_info({'EXIT', Pid, Reason}, #state { worker = Pid } = State) ->
    error_logger:error_msg("Compaction worker PID exited: ~p\n", [Reason]),
    {stop, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal worker
%% ====================================================================

worker_loop(Parent) ->
    Parent ! worker_ready,
    receive
        {compaction, Pid} ->
            Start = now(),
            Result = merge_index:compact(Pid),
            ElapsedSecs = timer:now_diff(now(), Start) / 1000000,
            case Result of
                {ok, OldSegments, OldBytes} ->
                    error_logger:info_msg(
                      "Pid ~p compacted ~p segments for ~p bytes in ~p seconds, ~.2f MB/sec\n",
                      [Pid, OldSegments, OldBytes, ElapsedSecs, OldBytes/ElapsedSecs/(1024*1024)]);
                {Error, Reason} when Error == error; Error == 'EXIT' ->
                    error_logger:error_msg("Failed to compact ~p: ~p\n",
                                           [Pid, Reason])
            end,
            worker_loop(Parent)
    end.

