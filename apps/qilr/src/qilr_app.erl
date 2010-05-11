-module(qilr_app).

-behaviour(application).

-export([start/0, start/2, stop/1]).

start() ->
   application:start(qilr).

start(_StartType, _StartArgs) ->
    qilr_sup:start_link().

stop(_State) ->
    ok.
