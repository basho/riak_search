-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-define(IS_TERM_PROHIBITED(Op), lists:member(prohibited, Op#term.options)).
-define(IS_TERM_REQUIRED(Op), lists:member(required, Op#term.options)).
-define(IS_TERM_FACET(Op), lists:member(facet, Op#term.options)).

%% Pre-plan Operators...
-record(term,             {string, options}). 
-record(lnot,             {ops}).
-record(land,             {ops}).
-record(lor,              {ops}).
-record(group,            {ops}).
-record(field,            {field, ops}).

%% Not yet implemented...
-record(inclusive_range,  {start_op, end_op}).
-record(exclusive_range,  {start_op, end_op}).
-record(node,             {node, ops}).
