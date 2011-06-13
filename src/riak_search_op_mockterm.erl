%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_mockterm).
-export([
         preplan/2,
         chain_op/4
        ]).

-include("riak_search.hrl").

preplan(Op, _State) -> 
    Op.

chain_op(Op, OutputPid, OutputRef, State) ->
    F = fun() ->
                erlang:link(State#search_state.parent),
                send_results(Op, OutputPid, OutputRef, State) 
        end,
    erlang:spawn_link(F),
    {ok, 1}.

send_results(Op, OutputPid, OutputRef, _State) ->
    F = fun(X) ->
        OutputPid!{results, X, OutputRef}
    end,
    [F(X) || X <- Op#mockterm.results],
    OutputPid!{disconnect, OutputRef}.
