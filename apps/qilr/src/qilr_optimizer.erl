-module(qilr_optimizer).

-export([optimize/1]).

optimize({ok, [H|T]}) when not(is_list(T)) ->
    {ok, [{lor, H ++ [T]}]};
optimize({ok, [{Type, _, _}=H|T]}) ->
  case needs_implicit_or(Type, T) of
      true ->
          {ok, [{lor, [H|T]}]};
      false ->
          {ok, [H|T]}
  end;
optimize(Query) ->
  Query.

needs_implicit_or(term, T) when length(T) > 0 ->
    true;
needs_implicit_or(group, T) when length(T) > 0 ->
    true;
needs_implicit_or(lnot, T) when length(T) > 0 ->
    true;
needs_implicit_or(lor, T) when length(T) > 0 ->
    true;
needs_implicit_or(land, T) when length(T) > 0 ->
    true;
needs_implicit_or(_, _) ->
    false.
