%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(raptor_sup).

-behaviour(supervisor).

-include("raptor.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 10,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    ConnSup = {raptor_conn_sup, {raptor_conn_sup, start_link, []},
               permanent, infinity, supervisor, [raptor_conn_sup]},
    Monitor = {raptor_monitor, {raptor_monitor, start_link, []},
               permanent, 5000, worker, [raptor_monitor]},

    PoolCountFun = fun() -> app_helper:get_env(raptor, backend_conn_count, 10) end,

    ConnPool = {?CONN_POOL, {riak_sock_pool, start_link, [?CONN_POOL, {raptor_conn_sup, raptor_conn}, PoolCountFun]},
                permanent, 5000, worker, [riak_sock_pool]},

    {ok, {SupFlags, [Monitor, ConnSup, ConnPool]}}.
