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
-record(term,             {q, options}).
-record(lnot,             {ops}).
-record(land,             {ops}).
-record(lor,              {ops}).
-record(group,            {ops}).
-record(field,            {field, ops}).
-record(inclusive_range,  {start_op, end_op}).
-record(exclusive_range,  {start_op, end_op}).
-record(node,             {ops, node}).
-record(proximity,        {ops, proximity}).

-record(riak_indexed_doc, {id,
                           index="search",
                           fields=[],
                           props=[]}).
