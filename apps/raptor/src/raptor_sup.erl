-module(raptor_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 10,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    ConnMgr = {raptor_conn_mgr, {raptor_conn_mgr, start_link, []},
               permanent, 2000, worker, [raptor_conn_mgr]},

    {ok, {SupFlags, [ConnMgr]}}.
