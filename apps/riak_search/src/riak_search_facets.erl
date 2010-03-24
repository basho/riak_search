-module(riak_search_facets).
-export([passes_facets/2]).
-include("riak_search.hrl").

passes_facets(Props, Facets) when is_list(Facets) ->
    F = fun(X) -> passes_facets(Props, X) end,
    lists:all(F, Facets);

passes_facets(Props, Facet) when is_record(Facet, land) ->
    F = fun(X) -> passes_facets(Props, X) end,
    lists:all(F, Facet#land.ops);

passes_facets(Props, Facet) when is_record(Facet, lor) ->
    F = fun(X) -> passes_facets(Props, X) end,
    lists:any(F, Facet#lor.ops);

passes_facets(Props, Facet) when is_record(Facet, lnot) ->
    not passes_facets(Props, Facet#lnot.ops);

passes_facets(Props, Facet) when is_record(Facet, term) ->
    [_, Field, Value] = string:tokens(Facet#term.string, "."),
    proplists:get_value(Field, Props) == Value.

