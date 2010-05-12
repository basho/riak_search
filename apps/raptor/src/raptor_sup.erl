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

    ConnSup = {raptor_conn_sup, {raptor_conn_sup, start_link, []},
               permanent, infinity, supervisor, [raptor_conn_sup]},

    {ok, {SupFlags, [ConnSup]}}.
