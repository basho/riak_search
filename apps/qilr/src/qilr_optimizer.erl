-module(qilr_optimizer).

-export([optimize/2]).

-include_lib("eunit/include/eunit.hrl").

optimize({ok, Query}, Opts) when not is_list(Query) ->
    optimize({ok, [Query]}, Opts);
optimize({ok, Query0}, Opts) ->
    Query1 = process_terms(Query0, Opts),
    {ok, default_bool(Query1, Opts)}.

%% Internal functions
process_terms(Query, _Opts) ->
    analyze_terms(Query, []).

-ifdef(TEST).
analyze_terms(Query, _Acc) ->
    Query.
-else.
analyze_terms(Query, _Acc) ->
    Query.
-endif.

default_bool([H|T], Opts) when not(is_list(T)) ->
    DefaultBool = proplists:get_value(default_bool, Opts, lor),
    [{DefaultBool, H ++ [T]}];
default_bool([{Type, _, _}=H|T], Opts) ->
    DefaultBool = proplists:get_value(default_bool, Opts, lor),
    case needs_implicit_bool(Type, T) of
        true ->
            [{DefaultBool, [H|T]}];
        false ->
            [H|T]
    end;
default_bool(Query, _Opts) ->
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
