-module(riak_search_op_term).
-export([
         preplan_op/2,
         chain_op/3
        ]).

-include("riak_search.hrl").

preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef) ->
    Q = Op#term.q,
    Facets = proplists:get_all_values(facets, Op#term.options),
    spawn_link(fun() -> start_loop(Q, Facets, OutputPid, OutputRef) end),
    {ok, 1}.

start_loop(Q, Facets, OutputPid, OutputRef) ->
    {Index, Field, Term} = Q,

    %% Stream the results...
    Fun = fun(_Value, Props) ->
        riak_search_facets:passes_facets(Props, Facets)
    end,
    {ok, Ref} = riak_search:stream(Index, Field, Term, Fun),

    %% Gather the results...
    loop(Ref, OutputPid, OutputRef).

loop(Ref, OutputPid, OutputRef) ->
    receive 
        {result, '$end_of_table', Ref} ->
            OutputPid!{disconnect, OutputRef};

        {result, {Key, Props}, Ref} ->
            OutputPid!{results, [{Key, Props}], OutputRef},
            loop(Ref, OutputPid, OutputRef)
    end.
