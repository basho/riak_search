%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_node).
-export([
         extract_scoring_props/1,
         preplan/2,
         chain_op/4
        ]).
-include("riak_search.hrl").

extract_scoring_props(Op) ->
    riak_search_op:extract_scoring_props(Op#node.ops).

preplan(Op, _State) ->
    ChildOps = Op#node.ops,
    Node = get_target_node(ChildOps),
    Op#node { node=Node, ops=ChildOps }.

chain_op(Op, OutputPid, OutputRef, State) ->
    Node = Op#node.node,
    Ops = Op#node.ops,
    rpc:call(Node, riak_search_op, chain_op, [Ops, OutputPid, OutputRef, State]).


%% Crawl through the ops list looking for #term {} records. When we
%% find one, extract the weights from {Node, Weight}. Return the node
%% with the greatest weight.
get_target_node(Ops) ->
    NodeWeights = get_term_weights(Ops),
    F = fun({Node, Weight}, Acc) ->
                case gb_trees:lookup(Node, Acc) of
                    {value, OldWeight} ->
                        gb_trees:update(Node, Weight + OldWeight, Acc);
                    none ->
                        gb_trees:insert(Node, Weight, Acc)
                end
        end,
    NodeWeights1 = lists:foldl(F, gb_trees:empty(), NodeWeights),
    NodeWeights2 = [{node(), 0}|gb_trees:to_list(NodeWeights1)],

    %% Sort in descending order by count...
    F1 = fun({_, Weight1}, {_, Weight2}) ->
                Weight1 >= Weight2
        end,
    NodeWeights3 = lists:sort(F1, NodeWeights2),

    %% Take nodes while they are at least 80% of heaviest weight and
    %% then choose one randomly.
    {_, Heavy} = hd(NodeWeights3),
    F2 = fun({_, Weight}) ->
        case Heavy of
            0 ->
                true;
            _ ->
                (Weight / Heavy) > 0.8
         end
    end,
    NodeWeights4 = lists:takewhile(F2, NodeWeights3),
    {Node, _} = riak_search_utils:choose(NodeWeights4),
    Node.

get_term_weights(Ops) ->
    lists:flatten(get_term_weights_1(Ops)).
get_term_weights_1(Ops) when is_list(Ops) ->
    [get_term_weights_1(X) || X <- Ops];
get_term_weights_1(#term { weights=Weights }) ->
    Weights;
get_term_weights_1(Op) when is_tuple(Op) ->
    get_term_weights_1(tuple_to_list(Op));
get_term_weights_1(_) ->
    [].
