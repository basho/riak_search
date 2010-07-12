-module(qilr_sup).

-behaviour(supervisor).

-include("qilr.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-ifndef(TEST).
init([]) ->
    AnalyzerMonitor = {qilr_analyzer_monitor, {qilr_analyzer_monitor, start_link, []},
                       permanent, 2000, worker, [qilr_analyzer_monitor]},

    AnalyzerSup = {qilr_analyzer_sup, {qilr_analyzer_sup, start_link, []},
                       permanent, infinity, supervisor, [qilr_analyzer_sup]},

    PoolCountFun = fun() -> app_helper:get_env(qilr, backend_conn_count, 10) end,

    ConnPool = {?CONN_POOL, {riak_sock_pool, start_link, [?CONN_POOL, {qilr_analyzer_sup, qilr_analyzer}, PoolCountFun]},
                permanent, 5000, worker, [riak_sock_pool]},


    {ok, {{one_for_all, 100, 10}, [AnalyzerMonitor, AnalyzerSup, ConnPool]}}.
-endif.
-ifdef(TEST).
init([]) ->
    {ok, {{one_for_all, 100, 10}, []}}.
-endif.
