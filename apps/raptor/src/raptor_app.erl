%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(raptor_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

start() ->
    application:start(raptor).

start(_StartType, _StartArgs) ->
    raptor_sup:start_link().

stop(_State) ->
    ok.
