-module(riak_search_op_inclusive_range).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#inclusive_range { 
        start_op=F(Op#inclusive_range.start_op),
        end_op=F(Op#inclusive_range.end_op)
    }.

chain_op(Op, _OutputPid, _OutputRef) ->
    throw({should_not_get_here, Op}).
