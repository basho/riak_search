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

passes_facets(Props, Facet) when is_record(Facet, inclusive_range) ->
    Start = hd(Facet#inclusive_range.start_op),
    End = hd(Facet#inclusive_range.end_op),
    {_Index, StartField, StartValue} = Start#term.q,
    {_Index, StartField, EndValue} = End#term.q,
    Value = proplists:get_value(StartField, Props),
    StartValue =< Value andalso Value =< EndValue;

passes_facets(Props, Facet) when is_record(Facet, exclusive_range) ->
    Start = hd(Facet#exclusive_range.start_op),
    End = hd(Facet#exclusive_range.end_op),
    {_Index, StartField, StartValue} = Start#term.q,
    {_Index, StartField, EndValue} = End#term.q,
    Value = proplists:get_value(StartField, Props),
    StartValue < Value andalso Value < EndValue;

passes_facets(Props, Facet) when is_record(Facet, term) ->
    {_Index, Field, FacetValue} = Facet#term.q,
    PropValue = proplists:get_value(Field, Props),

    IsWildcardAll = ?IS_TERM_WILDCARD_ALL(Facet),
    IsWildcardOne = ?IS_TERM_WILDCARD_ONE(Facet),
    PrefixMatch = (PropValue /= undefined) andalso (string:str(PropValue, FacetValue) == 1),
    LengthDiff = case PropValue /= undefined of
        true -> length(PropValue) - length(FacetValue);
        false -> undefined
    end,

    if 
        PrefixMatch andalso LengthDiff == 0 andalso not IsWildcardOne ->
            true;
        PrefixMatch andalso LengthDiff == 1 andalso IsWildcardOne ->
            true;
        PrefixMatch andalso IsWildcardAll ->
            true;
        true ->
            false
    end.    

    
