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

    AnalyzerSup = {qilr_analyzer_sup, {qilr_analyzer_sup, start_link, []},
                   permanent, infinity, supervisor, [qilr_analyzer_sup]},


    {ok, {{one_for_all, 100, 10}, [AnalyzerMonitor, AnalyzerSup]}}.
