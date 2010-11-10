%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op).
-export([
         preplan/2,
         chain_op/4,
         op_to_module/1,
         props_by_tag/2,
         get_target_node/1
        ]).
-include("riak_search.hrl").


%% Calls preplan on all operations, which returns a dictionary of
%% properties. This allows operations to exchange information with
%% other operations upstream or downstream.
preplan(OpList, State) when is_list(OpList) ->
    F = fun(Op, Acc) ->
                preplan(Op, State) ++ Acc
        end,
    lists:foldl(F, [], OpList);
preplan(Op, State) when is_tuple(Op) ->
    Module = op_to_module(Op),
    Module:preplan(Op, State).

%% Kick off execution of the query graph.
chain_op(OpList, OutputPid, Ref, SearchState) when is_list(OpList)->
    [chain_op(Op, OutputPid, Ref, SearchState) || Op <- OpList],
    {ok, length(OpList)};

chain_op(Op, OutputPid, Ref, SearchState) ->
    F = fun() ->
                Module = op_to_module(Op),
                Module:chain_op(Op, OutputPid, Ref, SearchState)
        end,
    spawn_link(F),
    {ok, 1}.

op_to_module(Op) ->
    ModuleString = "riak_search_op_" ++ atom_to_list(element(1, Op)),
    Module = list_to_atom(ModuleString),
    case code:ensure_loaded(Module) of
	{module, Module} -> 
            Module;
	{error, _}       -> 
            ?PRINT({unknown_op, Op}),
            throw({unknown_op, Op})
    end.

%% Given a list of Props, filter to only get the props that match a
%% given tag.
props_by_tag(Tag, Props) ->
    [Prop || {{Tag1, _}, Prop} <- Props, Tag == Tag1].

%% Given a list of Props, extract the properties created by #term and
%% use it to figure out the best place to run the intersection in
%% order to minimize data sent across the wire.
get_target_node(Props) ->
    %% Group the counts by node...
    NodeWeights = props_by_tag(node_weight, Props),
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

