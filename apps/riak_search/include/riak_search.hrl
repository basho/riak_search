-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n ~p~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-define(IS_PROHIBITED(Op), lists:member(prohibited, Op#term.flags)).
-define(IS_REQUIRED(Op), lists:member(required, Op#term.flags)).

%% Pre-plan Operators...
-record(term,             {string, flags}). 
-record(lnot,             {ops}).
-record(land,             {ops}).
-record(lor,              {ops}).
-record(group,            {ops}).
-record(field,            {field, ops}).
-record(inclusive_range,  {start_op, end_op}).
-record(exclusive_range,  {start_op, end_op}).
-record(node,             {node, ops}).
