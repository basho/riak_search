-module(riak_search_op_term).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

preplan_op(Op, _F) ->
    Op.

chain_op(Op, OutputPid, OutputRef) ->
    String = Op#term.string,
    spawn(fun() -> send_results(String, OutputPid, OutputRef) end),
    {ok, 1}.

send_results(String, OutputPid, OutputRef) ->
    Term1 = lists:nth(3, string:tokens(String, ".")),
    Term2 = lists:sort(Term1),
    [OutputPid!{results, [X], OutputRef} || X <- Term2],
    OutputPid!{disconnect, OutputRef}.
