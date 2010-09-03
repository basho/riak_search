%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op).
-export([
         preplan_op/2,
         chain_op/4
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    case op_to_module(Op) of
        undefined ->
            ?PRINT({unknown_op, Op}),
            throw({unknown_op, Op});
        Module ->
            Module:preplan_op(Op, F)
    end.


chain_op(OpList, OutputPid, Ref, QueryProps) when is_list(OpList)->
    [chain_op(Op, OutputPid, Ref, QueryProps) || Op <- OpList],
    {ok, length(OpList)};

chain_op(Op, OutputPid, Ref, QueryProps) ->
    spawn_link(fun() ->
        case op_to_module(Op) of
            undefined ->
                ?PRINT({unknown_op, Op}),
                throw({unknown_op, Op});
            Module ->
                Module:chain_op(Op, OutputPid, Ref, QueryProps),
                {ok, 1}
        end end),
    {ok, 1}.

op_to_module(Op) ->
    ModuleString = "riak_search_op_" ++ atom_to_list(element(1, Op)),
    Module = list_to_atom(ModuleString),
    case code:ensure_loaded(Module) of
	{module, Module} -> Module;
	{error, _}       -> undefined
    end.
