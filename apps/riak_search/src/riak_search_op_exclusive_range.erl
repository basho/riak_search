%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_exclusive_range).
-export([
         preplan_op/2,
         chain_op/4
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#exclusive_range { 
        start_op=F(Op#exclusive_range.start_op),
        end_op=F(Op#exclusive_range.end_op)
    }.

chain_op(Op, _OutputPid, _OutputRef, _QueryProps) ->
    throw({should_not_get_here, Op}).
