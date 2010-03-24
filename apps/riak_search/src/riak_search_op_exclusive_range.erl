-module(riak_search_op_exclusive_range).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#exclusive_range { 
        start_op=F(Op#exclusive_range.start_op),
        end_op=F(Op#exclusive_range.end_op)
    }.

chain_op(Op, _OutputPid, _OutputRef) ->
    throw({should_not_get_here, Op}).
