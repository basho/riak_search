-module(raptor_util).

-export([get_env/3]).

get_env(AppName, Key, Default) ->
    case application:get_env(AppName, Key) of
        undefined ->
            Default;
        {ok, Value} ->
            Value
    end.
