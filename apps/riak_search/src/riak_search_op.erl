%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op).
-export([
         preplan/2,
         chain_op/4,
         op_to_module/1
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
