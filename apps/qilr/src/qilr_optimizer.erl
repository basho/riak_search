-module(qilr_optimizer).

-export([optimize/2]).

optimize({ok, [H|T]}, Opts) when not(is_list(T)) ->
    DefaultBool = proplists:get_value(default_bool, Opts, lor),
    {ok, [{DefaultBool, H ++ [T]}]};
optimize({ok, [{Type, _, _}=H|T]}, Opts) ->
    DefaultBool = proplists:get_value(default_bool, Opts, lor),
    case needs_implicit_bool(Type, T) of
        true ->
            {ok, [{DefaultBool, [H|T]}]};
        false ->
            {ok, [H|T]}
    end;
optimize(Query, _Opts) ->
    Query.

needs_implicit_bool(term, T) when length(T) > 0 ->
    true;
needs_implicit_bool(group, T) when length(T) > 0 ->
    true;
needs_implicit_bool(lnot, T) when length(T) > 0 ->
    true;
needs_implicit_bool(lor, T) when length(T) > 0 ->
    true;
needs_implicit_bool(land, T) when length(T) > 0 ->
    true;
needs_implicit_bool(_, _) ->
    false.
