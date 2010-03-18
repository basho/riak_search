-module(riak_search_op_field).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#field { ops=F(Op#field.ops) }.

chain_op(Op, _OutputPid, _OutputRef) ->
    throw({should_not_get_here, Op}).
