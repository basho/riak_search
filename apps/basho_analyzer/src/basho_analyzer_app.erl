-module(basho_analyzer_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    basho_analyzer_sup:start_link().

stop(_State) ->
    ok.
