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
    DEBUG = false,
    case DEBUG of
        true ->
            spawn(fun() -> send_results(String, OutputPid, OutputRef) end);
        false ->
            spawn(fun() -> start_loop(String, OutputPid, OutputRef) end)
    end,
    {ok, 1}.

send_results(String, OutputPid, OutputRef) ->
    Term1 = lists:nth(3, string:tokens(String, ".")),
    Term2 = lists:sort(Term1),
    [OutputPid!{results, [X], OutputRef} || X <- Term2],
    OutputPid!{disconnect, OutputRef}.

start_loop(String, OutputPid, OutputRef) ->
    Term1 = lists:nth(3, string:tokens(String, ".")),
    Ref = make_ref(),
    riak_search_file_index:stream(string:to_lower(Term1), self(), Ref),
    loop(Ref, OutputPid, OutputRef).

loop(Ref, OutputPid, OutputRef) ->
    receive 
        {result, '$end_of_table', Ref} ->
            OutputPid!{disconnect, OutputRef};

        {result, {Key, Props}, Ref} ->
            OutputPid!{results, [Key], OutputRef},
            loop(Ref, OutputPid, OutputRef)
    end.
