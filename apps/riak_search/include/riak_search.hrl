-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n ~p~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-define(IS_PROHIBITED(Flags), lists:member(prohibited, Flags)).
-define(IS_REQUIRED(Flags), lists:member(required, Flags)).

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
