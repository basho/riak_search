%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_negation).
-export([
         extract_scoring_props/1,
         preplan/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

extract_scoring_props(Op) ->
    riak_search_op:extract_scoring_props(Op#negation.op).

preplan(Op, State) ->
    ChildOp = riak_search_op:preplan(Op#negation.op, State),
    Op#negation { op=ChildOp }.

chain_op(Op, OutputPid, OutputRef, State) ->
    %% Higher order operations (specifically #intersection and #union)
    %% look for the presence of the #negation operator during merge
    %% joins.  No actual work is done here.
    riak_search_op:chain_op(Op#negation.op, OutputPid, OutputRef, State).
