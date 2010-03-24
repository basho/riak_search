-module(riak_search_op_node).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#node { ops=F(Op#node.ops) }.

chain_op(Op, OutputPid, OutputRef) ->
    Node = Op#node.node,
    Ops = Op#node.ops,
    rpc:call(Node, riak_search_op, chain_op, [Ops, OutputPid, OutputRef]).

