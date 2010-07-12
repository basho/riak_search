-module(riak_search_ets_backend).
-behavior(riak_search_backend).

-export([start/2,
         stop/1,
         index/6,
         multi_index/2,
         delete_entry/5,
         stream/6,
         multi_stream/4,
         info/5,
         info_range/7,
         catalog_query/3,
         fold/3,
         is_empty/1,
         drop/1]).
-export([stream_results/3]).

-include("riak_search.hrl").

-record(state, {partition, table}).

start(Partition, _Config) ->
    Table = ets:new(list_to_atom(integer_to_list(Partition)),
                    [protected, bag]),
    {ok, #state{partition=Partition, table=Table}}.

stop(State) ->
    maybe_delete(State).

index(Index, Field, Term, DocId, Props, State) ->
    multi_index([{Index, Field, Term, DocId, Props}], State),
    noreply.
%%% TODO: why can't I {reply, ok} here? (cargo-cult raptor_backend)


multi_index(IFTVPList, State) ->
    %% TODO: look for timestamp before clobbering
    ets:insert(State#state.table,
               [ {{b(I), b(F), b(T)}, {V, P}}
                 || {I, F, T, V, P} <- IFTVPList ]),
    {reply, {indexed, node()}, State}.
%%% TODO: why can't I {reply, ok} here? (cargo-cult raptor_backend)


delete_entry(Index, Field, Term, DocId, State) ->
    ets:match_delete(State#state.table,
                     {{b(Index), b(Field), b(Term)}, {DocId, '_'}}),
    noreply.
%%% TODO: why can't I {reply, ok} here? (cargo-cult raptor_backend)

info(Index, Field, Term, Sender, State) ->
    Count = ets:select_count(State#state.table,
                             [{{{b(Index), b(Field), b(Term)}, '_'},
                               [],[true]}]),
    riak_search_backend:info_response(Sender, [{Term, node(), Count}]),
    noreply.

info_range(Index, Field, StartTerm, EndTerm, _Size, Sender, State) ->
    Terms = find_terms_in_range(b(Index), b(Field),
                                b(StartTerm), b(EndTerm),
                                State),
    spawn(
      fun() ->
              R = [{T, node(),
                    ets:select_count(State#state.table,
                                     [{{{b(Index), b(Field), T}, '_'},
                                       [], [true]}])}
                   || T <- Terms],
              riak_search_backend:info_response(Sender, R)
      end),
    noreply.

find_terms_in_range(I, F, ST, ET, State) ->
    find_terms_in_range(I, F, ST, ET,
                        State#state.table,
                        ets:first(State#state.table)).

find_terms_in_range(_, _, _, _, _,'$end_of_table') ->
    [];
find_terms_in_range(I, F, ST, ET, Table, {I, F, T})
  when T >= ST, T =< ET ->
    [T|find_terms_in_range(I, F, ST, ET, Table,
                           ets:next(Table, {I, F, T}))];
find_terms_in_range(I, F, ST, ET, Table, K) ->
    find_terms_in_range(I, F, ST, ET, Table, ets:next(Table, K)).

-define(STREAM_SIZE, 100).

stream(Index, Field, Term, FilterFun, Sender, State) ->
    multi_stream([{term, {Index, Field, Term}, []}],
                 FilterFun, Sender, State).

multi_stream(IFTList, FilterFun, Sender, State) ->
    spawn(riak_search_ets_backend, stream_results,
          [Sender,
           FilterFun,
           ets:select(State#state.table,
                      [{{{b(I), b(F), b(T)}, '$1'}, [], ['$1']}
                       || {term, {I, F, T}, _P} <- IFTList],
                      ?STREAM_SIZE)]),
    noreply.

stream_results(Sender, FilterFun, {Results0, Continuation}) ->
    case lists:filter(fun({V,P}) -> FilterFun(V, P) end, Results0) of
        [] ->
            ok;
        Results ->
            riak_search_backend:stream_response_results(Sender, Results)
    end,
    stream_results(Sender, FilterFun, ets:select(Continuation));
stream_results(Sender, _, '$end_of_table') ->
    riak_search_backend:stream_response_done(Sender).

%% TODO: hack up this function or change the implementation of
%%       riak_search:do_catalog_query/3, such that catalog_queries
%%       are performed on all vnodes, not just one per node
catalog_query(CatalogQuery, Sender, State) ->
    {Index, Field, StartTerm, EndTerm} = parse_catalog_query(CatalogQuery),
    Terms = find_terms_in_range(Index, Field, StartTerm, EndTerm, State),
    I = binary_to_list(Index),
    F = binary_to_list(Field),
    [riak_search_backend:catalog_query_response(
       Sender, State#state.partition, I, F, T,
       [{json_props, []}, {node, node()}])
     || T <- Terms],
    riak_search_backend:catalog_query_done(Sender),
    noreply.

parse_catalog_query(CatalogQuery) ->
    RE = <<"index:(.*) AND field:(.*) AND term:\\[(.*) TO (.*)\\]">>,
    {match, [I, F, ST, ET]} = re:run(CatalogQuery, RE,
                                     [{capture, [1, 2, 3, 4], binary}]),
    {I, F, ST, ET}.
                            


%%% TODO: test fold/3
fold(FoldFun, Acc, State) ->
    ets:foldl(FoldFun, Acc, State#state.table),
    {reply, ok}.

is_empty(State) ->
    0 == ets:info(State#state.table, size).

drop(State) ->
    maybe_delete(State).

maybe_delete(State) ->
    case lists:member(State#state.table, ets:all()) of
        true ->
            ets:delete(State#state.table),
            ok;
        false ->
            ok
    end.

b(Binary) when is_binary(Binary) -> Binary;
b(List) when is_list(List) -> iolist_to_binary(List).
