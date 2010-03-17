-module(riak_search_op_lor).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#lor { ops=F(Op#lor.ops) }.

chain_op(Op, OutputPid, OutputRef) ->
    NewOp = #land { ops=Op#lor.ops },
    riak_search_op_land:chain_op(NewOp, OutputPid, OutputRef, 'lor').
