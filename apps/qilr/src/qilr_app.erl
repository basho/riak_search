%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(qilr_app).

-behaviour(application).

-export([start/0, start/2, stop/1]).

start() ->
   application:start(qilr).

start(_StartType, _StartArgs) ->
    case app_helper:get_env(riak_search, enabled, false) of
        true -> qilr_sup:start_link();
        false -> {ok, self()}
    end.

stop(_State) ->
    ok.
