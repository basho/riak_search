-module(riak_search_op_lnot).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#lnot { ops=F(Op#lnot.ops) }.

chain_op(Op, OutputPid, OutputRef) ->
    [riak_search_op:chain_op(X, OutputPid, OutputRef) || X <- Op#lnot.ops],
    {ok, length(Op#lnot.ops)}.
