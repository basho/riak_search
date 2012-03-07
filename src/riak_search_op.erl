%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op).
-export([
         extract_scoring_props/1,
         frequency/1,
         preplan/1,
         preplan/2,
         chain_op/4,
         chain_op/5,
         op_to_module/1
        ]).
-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

extract_scoring_props(Ops) when is_list(Ops) ->
    [extract_scoring_props(Op) || Op <- Ops];
extract_scoring_props(Op) when is_tuple(Op) ->
    Mod = riak_search_op:op_to_module(Op),
    Mod:extract_scoring_props(Op).

%% @doc Return the `Frequency' of the search term in the index along
%% with its corresponding `Op'.
-spec frequency(term()) -> {Frequency::non_neg_integer(), Op::term()}.
frequency(Op) ->
    Mod = riak_search_op:op_to_module(Op),
    Mod:frequency(Op).

preplan(Op) ->
    preplan(Op, #search_state {}).

preplan(#scope { ops=[#negation { }]}, _State) ->
    throw({error, single_negated_term});
preplan(OpList, State) when is_list(OpList) ->
    [preplan(X, State) || X <- OpList];
preplan(Op, State) when is_tuple(Op) ->
    Module = riak_search_op:op_to_module(Op),
    Module:preplan(Op, State).

%% Kick off execution of the query graph.
chain_op(OpList, OutputPid, Ref, SearchState) when is_list(OpList)->
    erlang:link(SearchState#search_state.parent),
    [chain_op(Op, OutputPid, Ref, SearchState) || Op <- OpList],
    {ok, length(OpList)};

chain_op(Op, OutputPid, Ref, SearchState) ->
    F = fun() ->
                erlang:link(SearchState#search_state.parent),
                Module = op_to_module(Op),
                Module:chain_op(Op, OutputPid, Ref, SearchState)
        end,
    erlang:spawn_link(F),
    {ok, 1}.

%% TODO: Integrate this properly, i.e. don't use copy/paste, one-off
%% function here.
chain_op(OpList, OutputPid, Ref, CandidateSet, SearchState) when is_list(OpList)->
    erlang:link(SearchState#search_state.parent),
    [chain_op(Op, OutputPid, Ref, CandidateSet, SearchState) || Op <- OpList],
    {ok, length(OpList)};

chain_op(Op, OutputPid, Ref, CandidateSet, SearchState) ->
    F = fun() ->
                erlang:link(SearchState#search_state.parent),
                Module = op_to_module(Op),
                %% NOTE: chain_op/5 must be supported by any op that
                %% can be a child of intersection: term, range,
                %% negation, proximity, union, and scope
                %%
                %% TODO: actually, need to specially handle negation
                Module:chain_op(Op, OutputPid, Ref, CandidateSet, SearchState)
        end,
    erlang:spawn_link(F),
    {ok, 1}.

op_to_module(Op) ->
    ModuleString = "riak_search_op_" ++ atom_to_list(element(1, Op)),
    list_to_atom(ModuleString).
