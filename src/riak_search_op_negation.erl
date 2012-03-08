%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_negation).
-export([
         extract_scoring_props/1,
         frequency/1,
         is_negation/1,
         preplan/2,
         chain_op/4,
         chain_op/5
        ]).

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

extract_scoring_props(Op) ->
    riak_search_op:extract_scoring_props(Op#negation.op).

-spec frequency(term()) -> {Frequency::non_neg_integer(), term()}.
frequency(Op) ->
    {Frequency, _} = riak_search_op:frequency(Op#negation.op),
    {Frequency, Op}.

is_negation(#negation{}) -> true;
is_negation(_) -> false.

preplan(Op, State) ->
    ChildOp = riak_search_op:preplan(Op#negation.op, State),
    Op#negation { op=ChildOp }.

chain_op(Op, OutputPid, OutputRef, State) ->
    %% Higher order operations (specifically #intersection and #union)
    %% look for the presence of the #negation operator during merge
    %% joins.  No actual work is done here.
    riak_search_op:chain_op(Op#negation.op, OutputPid, OutputRef, State).

chain_op(Op, OutputPid, OutputRef, CandidateSet, State) ->
    %% Higher order operations (specifically #intersection and #union)
    %% look for the presence of the #negation operator during merge
    %% joins.  No actual work is done here.
    riak_search_op:chain_op(Op#negation.op, OutputPid, OutputRef, CandidateSet,
                            State).
