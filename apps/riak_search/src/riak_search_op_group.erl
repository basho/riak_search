%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_group).
-export([
         preplan/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

preplan(Op, State) -> 
    case Op#group.ops of
        [SingleOp] ->
            %% If there is only one op, no need to wrap it...
            NewOp = SingleOp;
        OpList ->
            %% Otherwise, look at schema to figure out if our default combine
            %% operation is AND or OR, and call intersection or union
            %% appropriately.
            Index = State#search_state.index,
            {ok, Schema} = riak_search_config:get_schema(Index),
            case Schema:default_op() of
                'and' ->
                    NewOp = #intersection { id=Op#group.id, ops=OpList };
                'or' ->
                    %% In this case, check if there are any
                    %% negated/prohibited terms. If so, then rewrite
                    %% the query so that the negated terms subtract
                    %% from the overall results.
                    F = fun(X) -> is_record(X, negation) end,
                    {NegatedOps, NormalOps} = lists:partition(F, Op#group.ops),
                    NegatedOps1 = [X#negation.op || X <- NegatedOps],

                    NewOp = #intersection { id=Op#group.id, ops=[
                        #union { id=Op#group.id, ops=NormalOps },
                        #negation { id=Op#group.id, op=#union { id=Op#group.id, ops=NegatedOps1 } }
                    ]}
            end
    end,
    riak_search_op:preplan(NewOp, State).

chain_op(Op, OutputPid, OutputRef, State) ->
    case Op#group.ops of
        [SingleOp] ->
            %% If there is only one op, no need to wrap it...
            NewOp = SingleOp;
        OpList ->
            %% Otherwise, look at schema to figure out if our default combine
            %% operation is AND or OR, and call intersection or union
            %% appropriately.
            Index = State#search_state.index,
            {ok, Schema} = riak_search_config:get_schema(Index),
            case Schema:default_op() of
                'and' ->
                    NewOp = #intersection { id=Op#group.id, ops=OpList };
                'or' ->
                    %% In this case, check if there are any
                    %% negated/prohibited terms. If so, then rewrite
                    %% the query so that the negated terms subtract
                    %% from the overall results.
                    F = fun(X) -> is_record(X, negation) end,
                    {NegatedOps, NormalOps} = lists:partition(F, Op#group.ops),
                    NegatedOps1 = [X#negation.op || X <- NegatedOps],

                    NewOp = #intersection { id=Op#group.id, ops=[
                        #union { id=Op#group.id, ops=NormalOps },
                        #negation { id=Op#group.id, op=#union { id=Op#group.id, ops=NegatedOps1 } }
                    ]}
            end
    end,

    %% Call the new operation...
    riak_search_op:chain_op(NewOp, OutputPid, OutputRef, State).
