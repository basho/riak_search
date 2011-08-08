%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op).
-export([
         preplan/1,
         preplan/2,
         chain_op/4,
         chain_op/5,
         op_to_module/1
        ]).
-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

preplan(#scope { ops=[#negation { }]}) ->
    throw({error, single_negated_term});
preplan(Op) ->
    preplan(Op, #search_state {}).
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

chain_op(OpList, OutputPid, Ref, SearchState, DocIds) when is_list(OpList)->
    erlang:link(SearchState#search_state.parent),
    [chain_op(Op, OutputPid, Ref, SearchState, DocIds) || Op <- OpList],
    {ok, length(OpList)};

chain_op(Op, OutputPid, Ref, SearchState, DocIds) ->
    F = fun() ->
                erlang:link(SearchState#search_state.parent),
                Module = op_to_module(Op),
                Module:chain_op(Op, OutputPid, Ref, SearchState, DocIds)
        end,
    erlang:spawn_link(F),
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
