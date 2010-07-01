-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-define(IS_TERM_PROHIBITED(Op), lists:member(prohibited, Op#term.options)).
-define(IS_TERM_REQUIRED(Op), lists:member(required, Op#term.options)).
-define(IS_TERM_FACET(Op), lists:member(facet, Op#term.options)).
-define(IS_TERM_PROXIMITY(Op), (proplists:get_value(proximity, Op#term.options) /= undefined)).
-define(IS_TERM_WILDCARD_ALL(Op), lists:member({wildcard, all}, Op#term.options)).
-define(IS_TERM_WILDCARD_ONE(Op), lists:member({wildcard, one}, Op#term.options)).

%% Pre-plan Operators...

%% Q will be normalized to {"index", "field", "term"} in
%% riak_search_preplan:normalize_term/2
-record(term,             {q, options=[]}).

%% Mockterm is used for QC unit tests to test middle logic. Doesn't
%% hit a backing store.
-record(mockterm,         {results=[]}).

%% #lnot's are collapsed down to the #term level in
%% riak_search_preplan:pass5/2.
-record(lnot,             {ops}).

-record(land,             {ops}).
-record(phrase,           {phrase, base_query}).
-record(lor,              {ops}).
-record(group,            {ops}).
-record(field,            {field, ops}).
-record(inclusive_range,  {start_op, end_op}).
-record(exclusive_range,  {start_op, end_op}).
-record(node,             {ops, node}).
-record(proximity,        {ops, proximity}).

-record(riak_idx_doc, {id,
                       index="search",
                       fields=[],
                       props=[]}).

-record(riak_search_ref, {id,
                          termcount,
                          inputcount,
                          querynorm}).

-record(riak_search_field, {name,
                            type,
                            required=false,
                            dynamic=false,
                            facet=false}).
