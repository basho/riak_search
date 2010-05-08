-module(qilr_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    AnalyzerMonitor = {qilr_analyzer_monitor, {qilr_analyzer_monitor, start_link, []},
                       permanent, 60000, worker, [qilr_analyzer_monitor]},

    Analyzer = {qilr_analyzer, {qilr_analyzer, start_link, []},
                permanent, 2000, worker, [qilr_analyzer]},


    {ok, {{one_for_all, 100, 10}, [AnalyzerMonitor, Analyzer]}}.
