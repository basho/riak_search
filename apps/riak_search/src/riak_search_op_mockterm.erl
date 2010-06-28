-module(riak_search_op_mockterm).
-export([
         preplan_op/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef, QueryProps) ->
    spawn_link(fun() -> send_results(Op, OutputPid, OutputRef, QueryProps) end),
    {ok, 1}.

send_results(Op, OutputPid, OutputRef, _QueryProps) ->
    F = fun(X) ->
        OutputPid!{results, X, OutputRef}
    end,
    [F(X) || X <- Op#mockterm.results],
    OutputPid!{disconnect, OutputRef}.
