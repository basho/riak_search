%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_lnot).
-export([
         preplan_op/2,
         chain_op/4
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#lnot { ops=F(Op#lnot.ops) }.

chain_op(Op, OutputPid, OutputRef, QueryProps) ->
    [riak_search_op:chain_op(X, OutputPid, OutputRef, QueryProps) || X <- Op#lnot.ops],
    {ok, length(Op#lnot.ops)}.
