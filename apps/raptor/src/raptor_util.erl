%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(raptor_util).

-export([get_env/3]).

get_env(AppName, Key, Default) ->
    case application:get_env(AppName, Key) of
        undefined ->
            Default;
        {ok, Value} ->
            Value
    end.
