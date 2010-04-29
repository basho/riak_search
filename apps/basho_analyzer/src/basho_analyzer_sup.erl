-module(basho_analyzer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    AnalyzerMonitor = {basho_analyzer_monitor, {basho_analyzer_monitor, start_link, []},
                       permanent, 60000, worker, [basho_analyzer_monitor]},

    Analyzer = {basho_analyzer, {basho_analyzer, start_link, []},
                permanent, 2000, worker, [basho_analyzer]},


    {ok, {{one_for_one, 10, 10}, [AnalyzerMonitor, Analyzer]}}.
