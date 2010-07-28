%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_raptor_backend).
-author("John Muellerleile <johnm@basho.com>").
-behavior(riak_search_backend).

-export([start/2,stop/1,index_if_newer/7,
         multi_index/2,delete_entry/6,multi_delete/2,
         stream/6,multi_stream/4,
         info/5,info_range/7,catalog_query/3,fold/3,is_empty/1,drop/1]).
-export([toggle_raptor_debug/0, shutdown_raptor/0]).
-export([sync/0, poke/1, raptor_status/0, get_entry_keyclock/5]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_fold/0, test_is_empty/0, test_drop/0]).
-endif.

-include_lib("raptor/include/raptor.hrl").
-include("riak_search.hrl").

% @type state() = term().
-record(state, {partition}).

-define(MAX_HANDOFF_STREAMS,  10).
-define(FOLD_TIMEOUT,         30000).
-define(INFO_TIMEOUT,          5000).
-define(INFO_RANGE_TIMEOUT,    5000).
-define(STREAM_TIMEOUT,        5000).
-define(CATALOG_QUERY_TIMEOUT, 5000).
-define(RESULT_VEC_SZ, 1000).


%% ===================================================================
%% Search Backend API
%% ===================================================================

%% @spec start(Partition :: integer(), Config :: proplist()) ->
%%          {ok, state()} | {{error, Reason :: term()}, state()}
%% @doc Start this backend.
start(Partition, _Config) ->
    {ok, #state { partition=Partition }}.
        

%% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(_State) ->
    ok.

index_if_newer(Index, Field, Term, DocId, Props, KeyClock, State) ->
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    try
        {ok, indexed} = raptor_conn:index_if_newer(Conn,
                                          to_binary(Index),
                                          to_binary(Field),
                                          to_binary(Term),
                                          to_binary(DocId),
                                          to_binary(State#state.partition),
                                          term_to_binary(Props),
                                          KeyClock)
    after
        riak_sock_pool:checkin(?CONN_POOL, Conn)
    end,
    noreply.

multi_index(IFTVPKList, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    try
        %% Index for raptor is fire and forget - no point checking
        %% return value
        [{ok, _} = raptor_conn:index(Conn,
                           to_binary(Index),
                           to_binary(Field),
                           to_binary(Term),
                           to_binary(Value),
                           Partition,
                           term_to_binary(Props),
                           KeyClock) || 
            {Index, Field, Term, Value, Props, KeyClock} <- IFTVPKList]
    after
        riak_sock_pool:checkin(?CONN_POOL, Conn)
    end,
    {reply, {indexed, State#state.partition}, State}.

delete_entry(Index, Field, Term, DocId, _KeyClock, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    try
        raptor_conn:delete_entry(Conn,
                                 to_binary(Index),
                                 to_binary(Field),
                                 to_binary(Term),
                                 to_binary(DocId),
                                 Partition)
    after
        riak_sock_pool:checkin(?CONN_POOL, Conn)
    end,
    noreply.

multi_delete(IFTVKList, State) ->
    [delete_entry(I, F, T, V, K, State) || {I, F, T, V, K} <- IFTVKList],
    noreply.

info(Index, Field, Term, Sender, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:info(
                                               Conn,
                                               to_binary(Index),
                                               to_binary(Field),
                                               to_binary(Term),
                                               Partition),
                           receive_info_results(StreamRef, Sender, Conn)
                       after
                           riak_sock_pool:checkin(?CONN_POOL, Conn)
                       end
    end),
    noreply.

info_range(Index, Field, StartTerm, EndTerm, _Size, Sender, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:info_range(
                                               Conn,
                                               to_binary(Index),
                                               to_binary(Field),
                                               to_binary(StartTerm),
                                               to_binary(EndTerm),
                                               Partition),
                           receive_info_range_results(StreamRef, Sender, Conn)
                       after
                           riak_sock_pool:checkin(?CONN_POOL, Conn)
                       end
    end),
    noreply.

stream(Index, Field, Term, FilterFun, Sender, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:stream(
                                               Conn,
                                               to_binary(Index),
                                               to_binary(Field),
                                               to_binary(Term),
                                               Partition),
                           receive_stream_results(StreamRef, Sender, FilterFun, Conn)
                       after
                           riak_sock_pool:checkin(?CONN_POOL, Conn)
                       end
               end),
    noreply.

multi_stream(IFTList, FilterFun, Sender, _State) ->
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),

    %% Encode ~ delimited index/field/term list delimited by ` chars
    Terms1 = lists:map(fun({term, {I, F, T}, _Props}) ->
                               string:join([I, F, T], "~")
                       end, IFTList),
    TermArg = string:join(Terms1, "`"),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:multi_stream(
                                               Conn,
                                               list_to_binary(TermArg)),
                           receive_stream_results(StreamRef, Sender, FilterFun, Conn)
                       after
                           riak_sock_pool:checkin(?CONN_POOL, Conn)
                       end
               end),
    noreply.

catalog_query(CatalogQuery, Sender, _State) ->
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:catalog_query(
                                               Conn,
                                               CatalogQuery),
                           receive_catalog_query_results(StreamRef, Sender, Conn)
                       after
                           riak_sock_pool:checkin(?CONN_POOL, Conn)
                       end
               end),
    noreply.

fold(Folder, Acc, State) ->
    sync(),
    {ok, Pid} = riak_search_raptor_backend_folder:start_link(),
    {ok, Result} = riak_search_raptor_backend_folder:fold(Pid, State#state.partition, Folder, Acc),
    {reply, Result, State}.

is_empty(State) ->
    Partition = to_binary(State#state.partition),
    raptor_command(<<"partition_count">>, Partition, <<"">>, <<"">>) == "0".

drop(State) ->
    Partition = to_binary(State#state.partition),
    raptor_command(<<"drop_partition">>, Partition, <<"">>, <<"">>),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

raptor_command(Command, Arg1, Arg2, Arg3) ->
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    try
        {ok, _StreamRef} = raptor_conn:command(Conn, Command, Arg1, Arg2, Arg3),
        receive
            {command, _ReqId, Response} ->
                Response
        end
    after
        riak_sock_pool:checkin(?CONN_POOL, Conn)
    end.

receive_stream_results(StreamRef, Sender, FilterFun, Conn) ->
    receive_stream_results(StreamRef, Sender, FilterFun, Conn, []).

receive_stream_results(StreamRef, Sender, FilterFun, Conn, Acc0) ->
    case length(Acc0) > ?RESULT_VEC_SZ of
        true ->
            riak_search_backend:stream_response_results(
              Sender, lists:reverse(Acc0)),
            Acc = [];
        false ->
            Acc = Acc0
    end,
    receive
        {stream, StreamRef, timeout} ->
            case length(Acc) > 0 of
                true ->
                    riak_search_backend:stream_response_results(
                      Sender, lists:reverse(Acc));
                false -> skip
            end,
            riak_search_backend:stream_response_done(Sender),
            exit(Conn, kill),
            error_logger:warning_msg("Stream result Raptor socket timeout\n");
        {stream, StreamRef, "$end_of_table", _, _} ->
            case length(Acc) > 0 of
                true ->
                    riak_search_backend:stream_response_results(
                      Sender, lists:reverse(Acc));
                
                false -> skip
            end,
            riak_search_backend:stream_response_done(Sender);
        {stream, StreamRef, Value, Props, _KeyClock} ->
            case Props of
                <<"">> ->
                    Props2 = [];
                _ ->
                    Props2 = binary_to_term(Props)
            end,
            case FilterFun(Value, Props2) of
                true ->
                    Acc2 = [{Value, Props2}|Acc];
                    %OutputPid ! {result, {Value, Props2}, OutputRef};
                _ ->
                    Acc2 = Acc,
                    skip
            end,
            receive_stream_results(StreamRef, Sender, FilterFun, Conn, Acc2);
        Msg ->
            riak_search_backend:stream_response_done(Sender),
            exit(Conn, kill),
            throw({unexpected_msg, Msg})
    after
        ?STREAM_TIMEOUT ->
            riak_search_backend:stream_response_done(Sender),
            exit(Conn, kill),
            error_logger:warning_msg("Stream result Raptor conn timeout\n")
    end,
    ok.

receive_info_range_results(StreamRef, Sender, Conn) ->
    receive_info_range_results(StreamRef, Sender, Conn, []).

receive_info_range_results(StreamRef, Sender, Conn, Results) ->
    receive
        {info, StreamRef, timeout} ->
            riak_search_backend:info_response(Sender, lists:reverse(Results)),
            exit(Conn, kill),
            error_logger:warning_msg("Info range result Raptor socket timeout\n");

        {info, StreamRef, "$end_of_info", 0} ->
            riak_search_backend:info_response(Sender, lists:reverse(Results));
        
        {info, StreamRef, Term, Count} ->
            receive_info_range_results(StreamRef, Sender, Conn,
                [{Term, node(), Count}|Results]);
        Msg ->
            riak_search_backend:info_response(Sender, lists:reverse(Results)),
            exit(Conn, kill),
            throw({unexpected_msg, Msg})
    after
        ?INFO_RANGE_TIMEOUT ->
            riak_search_backend:info_response(Sender, lists:reverse(Results)),
            exit(Conn, kill),
            error_logger:warning_msg("Info range result Raptor conn timeout\n")
    end,
    ok.

receive_info_results(StreamRef, Sender, Conn) ->
    receive
        {info, StreamRef, timeout} ->
            riak_search_backend:info_response(Sender, []),
            exit(Conn, kill),
            error_logger:warning_msg("Info result Raptor socket timeout\n"),
            ok;
        {info, StreamRef, "$end_of_info", _Count} ->
            ok;
        {info, StreamRef, Term, Count} ->
            riak_search_backend:info_response(Sender, [{Term, node(), Count}]),
            receive_info_results(StreamRef, Sender, Conn);
        Msg ->
            riak_search_backend:info_response(Sender, []),
            exit(Conn, kill),
            throw({unexpected_msg, Msg})
    after
        ?INFO_TIMEOUT ->           
            riak_search_backend:info_response(Sender, []),
            exit(Conn, kill),
            error_logger:warning_msg("Info result Raptor conn timeout\n")
    end,
    ok.

receive_catalog_query_results(StreamRef, Sender, Conn) ->
    receive
        {catalog_query, _ReqId, timeout} ->
            riak_search_backend:catalog_query_done(Sender),
            exit(Conn, kill),
            error_logger:warning_msg("Catalog query result Raptor socket timeout\n");
        {catalog_query, _ReqId, "$end_of_results", _, _, _, _} ->
            riak_search_backend:catalog_query_done(Sender);
        {catalog_query, StreamRef, Partition, Index,
                        Field, Term, JSONProps} ->
            riak_search_backend:catalog_query_response(Sender, Partition, Index,
                                                       Field, Term, 
                                                       [{json_props, JSONProps},
                                                        {node, node()}]),
            receive_catalog_query_results(StreamRef, Sender, Conn);
        Msg ->
            riak_search_backend:catalog_query_done(Sender),
            exit(Conn, kill),
            throw({unexpected_msg, Msg})
    after
        ?CATALOG_QUERY_TIMEOUT ->   
            riak_search_backend:catalog_query_done(Sender),
            exit(Conn, kill),
            error_logger:warning_msg("Catalog query result Raptor conn timeout\n")
    end,
    ok.

%%%

to_binary(A) when is_atom(A) -> to_binary(atom_to_list(A));
to_binary(B) when is_binary(B) -> B;
to_binary(I) when is_integer(I) -> to_binary(integer_to_list(I));
to_binary(L) when is_list(L) -> list_to_binary(L).

poke(Command) ->
    raptor_command(Command, <<"">>, <<"">>, <<"">>).

sync() ->
    poke("sync").

toggle_raptor_debug() ->
    poke("toggle_debug").

shutdown_raptor() ->
    error_logger:info_msg("issuing raptor engine shutdown~n"),
    poke("shutdown").

raptor_status() ->
    Status = string:tokens(poke("status"), "`"),
    error_logger:info_msg("Raptor Status ~p~n", [Status]),
    Status.

get_entry_keyclock(Partition, Index, Field, Term, DocId) ->
    IFT = string:join([Index, Field, Term], "~"),
    raptor_command("get_entry_keyclock", IFT, Partition, DocId).

%%%

-ifdef(TEST).
%% test fold
test_fold() ->
    Fun0 = fun(_BKey, _Obj, Acc) ->
        Acc
    end,
    State = #state { partition=0 },
    spawn(fun() ->
        fold(State, Fun0, []) end).

test_is_empty() ->
    State = #state { partition=0 },
    is_empty(State).

test_drop() ->
    State = #state { partition=0 },
    drop(State).

conn_pool_pid() ->
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    ?assertEqual(0, riak_sock_pool:current_count(?CONN_POOL)),
    riak_sock_pool:checkin(?CONN_POOL, Conn),
    Conn.

info_socket_timeout_test() ->
    run_timeout_test(
      fun(State) ->
              Index = <<"index">>,
              Field = <<"field">>,
              Term = <<"term">>,
              Ref = make_ref(),
              noreply = info(Index, Field, Term, {raw, Ref, self()}, State),
              receive
                  {Ref, []} ->
                      ok
              after
                  100 ->
                      ?assert(false)
              end
      end).

info_range_socket_timeout_test() ->
    run_timeout_test(
      fun(State) ->
              Index = <<"index">>,
              Field = <<"field">>,
              StartTerm = <<"startterm">>,
              EndTerm = <<"endterm">>,
              Size = undefined,
              Ref = make_ref(),
              noreply = info_range(Index, Field, StartTerm, EndTerm, Size,
                                   {raw, Ref, self()}, State),
              receive
                  {Ref, []} ->
                      ok
              after
                  100 ->
                      ?assert(false)
              end
      end).
             
stream_socket_timeout_test() ->
    run_timeout_test(
      fun(State) ->
              Index = <<"index">>,
              Field = <<"field">>,
              Term = <<"term">>,
              FilterFun = fun(_,_) -> true end,
              Ref = make_ref(),
              noreply = stream(Index, Field, Term, FilterFun,
                                   {raw, Ref, self()}, State),
              receive
                  Msg ->
                      ?assertMatch({Ref, done}, Msg)
              after
                  100 ->
                      ?assert(false)
              end
      end).
              
multi_stream_socket_timeout_test() ->
    run_timeout_test(
      fun(State) ->
              Index = "index",
              Field = "field",
              Term =  "term",
              FilterFun = fun(_,_) -> true end,
              Ref = make_ref(),
              noreply = multi_stream([{term, {Index, Field, Term},[]}], FilterFun,
                                   {raw, Ref, self()}, State),
              receive
                  Msg ->
                      ?assertMatch({Ref, done}, Msg)
              after
                  100 ->
                      ?assert(false)
              end
      end).

              
catalog_query_socket_timeout_test() ->
    run_timeout_test(
      fun(State) ->
              CatalogQuery = "query",
              Ref = make_ref(),
              noreply = catalog_query(CatalogQuery, 
                                      {raw, Ref, self()}, State),
              receive
                  Msg ->
                      ?assertMatch({Ref, done}, Msg)
              after
                  100 ->
                      ?assert(false)
              end
      end).
 
           
run_timeout_test(TestFun) ->                  
    {ok, Sup} = riak_sock_pool:start_link(?CONN_POOL, 
                                          {mock_raptor_conn, mock_raptor_conn},
                                          fun() -> 1 end),
    try
        Partition = 0,
        Config = [],
        {ok, State} = start(Partition, Config),
        Pid1 = conn_pool_pid(),
        TestFun(State),
        Pid2 = conn_pool_pid(),
        ?assertEqual(false, is_process_alive(Pid1)),
        ?assert(Pid1 =/= Pid2)
    after
        unlink(Sup),
        exit(Sup, kill)
    end.
    

-endif.
