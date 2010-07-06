%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_search_raptor_backend).
-author("John Muellerleile <johnm@basho.com>").
-behavior(riak_search_backend).

-export([start/2,stop/1,index/6,multi_index/2,delete_entry/5,
         stream/6,multi_stream/4,
         info/5,info_range/7,catalog_query/3,fold/3,is_empty/1,drop/1]).
-export([toggle_raptor_debug/0, shutdown_raptor/0]).
-export([sync/0, poke/1, raptor_status/0]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_fold/0, test_is_empty/0, test_drop/0]).
-endif.

-include_lib("raptor/include/raptor.hrl").
-include("riak_search.hrl").

% @type state() = term().
-record(state, {partition}).

-define(FOLD_TIMEOUT, 30000).
-define(MAX_HANDOFF_STREAMS, 50).
-define(INFO_TIMEOUT, 5000).
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

index(Index, Field, Term, Value, Props, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    try
        raptor_conn:index(Conn,
                          to_binary(Index),
                          to_binary(Field),
                          to_binary(Term),
                          to_binary(Value),
                          Partition,
                          term_to_binary(Props))
    after
        raptor_conn_pool:checkin(Conn)
    end,
    noreply.

multi_index(IFTVPList, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    try
        %% Index for raptor is fire and forget - no point checking
        %% return value
        [raptor_conn:index(Conn,
                           to_binary(Index),
                           to_binary(Field),
                           to_binary(Term),
                           to_binary(Value),
                           Partition,
                           term_to_binary(Props)) || 
            {Index, Field, Term, Value, Props} <- IFTVPList]
    after
        raptor_conn_pool:checkin(Conn)
    end,
    {reply, {indexed, node()}, State}.

delete_entry(Index, Field, Term, DocId, State) ->
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
                           receive_info_results(StreamRef, Sender)
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
                           receive_info_range_results(StreamRef, Sender)
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
                           receive_stream_results(StreamRef, Sender, FilterFun)
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
                           receive_stream_results(StreamRef, Sender, FilterFun)
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
                           receive_catalog_query_results(StreamRef, Sender)
                       after
                           riak_sock_pool:checkin(?CONN_POOL, Conn)
                       end
               end),
    noreply.

fold(Folder, Acc, State) ->
%% spawn a process to kick off the catalog listing
%%   for the this partition, e.g.,
    %% io:format("fold(~p, ~p, ~p)~n", [State, Folder, Acc]),
    %% io:format("fold/sync...~n"),
    sync(),
    %% io:format("fold/sync complete.~n"),
    Partition = integer_to_list(State#state.partition),
    {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
    Me = self(),
    FoldCatRef = {fold_catalog, erlang:make_ref()},
    CatalogResultsPid = spawn_link(fun() ->
                                           fold_catalog_process(FoldCatRef, Me, 
                                                                false, 0, 0, false) end),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:catalog_query(
                                               Conn,
                                               ["partition_id:\"", Partition , "\""]),
                           Sender = {raw, FoldCatRef, CatalogResultsPid},
                           receive_catalog_query_results(StreamRef, Sender)
                       after
                           riak_sock_pool:checkin(?CONN_POOL, Conn)
                       end end),
    FinalAcc = receive_fold_results(Folder, Acc),
    {reply, FinalAcc, State}.

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

receive_stream_results(StreamRef, Sender, FilterFun) ->
    receive_stream_results(StreamRef, Sender, FilterFun, []).

receive_stream_results(StreamRef, Sender, FilterFun, Acc0) ->
    case length(Acc0) > ?RESULT_VEC_SZ of
        true ->
            riak_search_backend:stream_response_results(Sender, Acc0),
            Acc = [];
        false ->
            Acc = Acc0
    end,
    receive
        {stream, StreamRef, timeout} ->
            case length(Acc) > 0 of
                true ->
                    riak_search_backend:stream_response_results(Sender, Acc);
                
                false -> skip
            end,
            riak_search_backend:stream_response_done(Sender);
        {stream, StreamRef, "$end_of_table", _} ->
            case length(Acc) > 0 of
                true ->
                    riak_search_backend:stream_response_results(Sender, Acc);
                
                false -> skip
            end,
            riak_search_backend:stream_response_done(Sender);
        {stream, StreamRef, Value, Props} ->
            case Props of
                <<"">> ->
                    Props2 = [];
                _ ->
                    Props2 = binary_to_term(Props)
            end,
            case FilterFun(Value, Props2) of
                true ->
                    Acc2 = Acc ++ [{Value, Props2}];
                    %OutputPid ! {result, {Value, Props2}, OutputRef};
                _ ->
                    Acc2 = Acc,
                    skip
            end,
            receive_stream_results(StreamRef, Sender, FilterFun, Acc2);
        _Msg ->
            %% TODO: Should this throw - must be an error
            %% io:format("receive_stream_results(~p, ~p, ~p) -> ~p~n",
            %%     [StreamRef, Sender, FilterFun, _Msg]),
            riak_search_backend:stream_response_done(Sender)
    end,
    ok.

receive_info_range_results(StreamRef, Sender) ->
    receive_info_range_results(StreamRef, Sender, []).

receive_info_range_results(StreamRef, Sender, Results) ->
    receive
        {info, StreamRef, timeout} ->
            riak_search_backend:info_response(Sender, Results);
        {info, StreamRef, "$end_of_info", 0} ->
            riak_search_backend:info_response(Sender, Results);
        
        %% TODO: Replace this with a [New | Acc] and lists:reverse
        {info, StreamRef, Term, Count} ->
            receive_info_range_results(StreamRef, Sender,
                Results ++ [{Term, node(), Count}])

    end,
    ok.

receive_info_results(StreamRef, Sender) ->
    receive
        {info, StreamRef, timeout} ->
            ok;
        {info, StreamRef, "$end_of_info", _Count} ->
            ok;
        {info, StreamRef, Term, Count} ->
            riak_search_backend:info_response(Sender, [{Term, node(), Count}]),
            receive_info_results(StreamRef, Sender)
    after
        ?INFO_TIMEOUT ->
            ok
    end,
    ok.

receive_catalog_query_results(StreamRef, Sender) ->
    receive
        {catalog_query, _ReqId, timeout} ->
            riak_search_backend:catalog_query_done(Sender);
        {catalog_query, _ReqId, "$end_of_results", _, _, _, _} ->
            riak_search_backend:catalog_query_done(Sender);
        {catalog_query, StreamRef, Partition, Index,
                        Field, Term, JSONProps} ->
            riak_search_backend:catalog_query_response(Sender, Partition, Index,
                                                       Field, Term, 
                                                       [{json_props, JSONProps},
                                                        {node, node()}]),
            receive_catalog_query_results(StreamRef, Sender)
    end,
    ok.

%% receive catalog entries for current partition & kick
%%   off a stream process for each one in parallel
fold_catalog_process(CatRef,
                     FoldResultPid,
                     CatalogDone,
                     StreamProcessCount,
                     FinishedStreamProcessCount,
                     DeferredTables) ->
    %% kick off one fold_stream_process per catalog entry that comes back
    %%   increment StreamProcessCount
    %% when receive done from fold_stream_process processes, increment
    %%   FinishedStreamProcessCount
    %% when CatalogDone = true && StreamProcessCount == FinishedStreamProcessCount,
    %%   exit
    Me = self(),
    receive
        {CatRef, done} ->
            case StreamProcessCount of
                0 ->
                    FoldResultPid ! {fold_result, done},
                    true;
                _SPC ->
                    fold_catalog_process(CatRef, FoldResultPid, true,
                        StreamProcessCount, FinishedStreamProcessCount, DeferredTables)
            end;

        {CatRef, {Partition, Index, Field, Term, JSONProps}} ->
            %% kick off stream for this PIFT
            case (StreamProcessCount - FinishedStreamProcessCount) >
                  ?MAX_HANDOFF_STREAMS of
                true ->
                    %io:format("fold_catalog_process: deferring ~p.~p.~p~n",
                    %    [Index, Field, Term]),
                    self() ! {CatRef, {Partition, Index, Field, Term, JSONProps}},
                    fold_catalog_process(CatRef, FoldResultPid, CatalogDone,
                                         StreamProcessCount, FinishedStreamProcessCount, true);
                false ->
                    spawn_link(fun() ->
                        %% io:format("fold_catalog_process: catalog_query_response: ~p: ~p.~p.~p (~p)~n",
                        %%     [Partition, Index, Field, Term, JSONProps]),
                        {ok, Conn} = riak_sock_pool:checkout(?CONN_POOL),
                        try
                            IndexBin = to_binary(Index),
                            FieldBin = to_binary(Field),
                            TermBin = to_binary(Term),
                            {ok, StreamRef} = raptor_conn:stream(
                                                Conn,
                                                IndexBin,
                                                FieldBin,
                                                TermBin,
                                                to_binary(Partition)),
                          fold_stream_process(Me, FoldResultPid, StreamRef,
                                              IndexBin, FieldBin, TermBin)
                        after 
                            riak_sock_pool:checkin(?CONN_POOL, Conn)
                        end end),
                    fold_catalog_process(CatRef, FoldResultPid, CatalogDone,
                                         StreamProcessCount+1, FinishedStreamProcessCount, false)
                end;

        {fold_stream, done, _StreamRef1} ->
            %%io:format("fold_stream: done: ~p of ~p~n",
            %%    [FinishedStreamProcessCount, StreamProcessCount]),
            case FinishedStreamProcessCount >= (StreamProcessCount-1) andalso
                 CatalogDone == true andalso
                 DeferredTables == false of
                    true ->
                        %% io:format("fold_catalog_process: streaming complete (~p of ~p)~n",
                        %%     [(FinishedStreamProcessCount+1), StreamProcessCount]),
                        FoldResultPid ! {fold_result, done};
                    false ->
                        fold_catalog_process(CatRef, FoldResultPid, CatalogDone,
                            StreamProcessCount, FinishedStreamProcessCount+1, false)
                end;
        _Msg ->
            %% io:format("fold_catalog_process: unknown message: ~p~n", [_Msg]),
            fold_catalog_process(CatRef, FoldResultPid, CatalogDone,
                StreamProcessCount, FinishedStreamProcessCount, DeferredTables)
        after ?FOLD_TIMEOUT ->
            case FinishedStreamProcessCount >= (StreamProcessCount) of
                true -> ok;
                false ->
                    %%TODO: How should we handle cleanup here?
                    %% io:format("fold_catalog_process: timed out (~p of ~p), proceeding to {fold_result, done}~n",
                    %%     [FinishedStreamProcessCount, StreamProcessCount])
                    ok
            end,
            FoldResultPid ! {fold_result, done}
    end.

%% for each result of a stream process, package the entry in the
%%   form of an infamously shady put-embedded command and forward
%%   to receive_fold_results process
fold_stream_process(CatalogProcessPid, FoldResultPid, StreamRef, IndexBin, FieldBin, TermBin) ->
    receive
        {stream, StreamRef, timeout} ->
            CatalogProcessPid ! {fold_stream, done, StreamRef};
        {stream, StreamRef, "$end_of_table", _} ->
            %% io:format("fold_stream_process: table complete: ~p.~p.~p~n",
            %%     [Index, Field, Term]),
            CatalogProcessPid ! {fold_stream, done, StreamRef};
        {stream, StreamRef, Value, Props}=_Msg2 ->
            case Props of
                <<>> ->
                    Props2 = [];
                _ ->
                    Props2 = binary_to_term(Props)
            end,
            FieldTermBin = <<FieldBin/binary, ".", TermBin/binary>>,
            BObj = term_to_binary({FieldBin, TermBin, Value, Props2}),
            FoldResultPid ! {fold_result, IndexBin, FieldTermBin, BObj},
            fold_stream_process(CatalogProcessPid, FoldResultPid, StreamRef, 
                                IndexBin, FieldBin, TermBin);
        _Msg ->
            %% io:format("fold_stream_process: unknown message: ~p~n", [_Msg]),
            fold_stream_process(CatalogProcessPid, FoldResultPid, StreamRef, 
                                IndexBin, FieldBin, TermBin)
        after ?FOLD_TIMEOUT ->
            CatalogProcessPid ! {fold_stream, done, StreamRef},
            error_logger:warning_msg("fold_stream_process: table timed out: ~p.~p.~p~n",
                                      [IndexBin, FieldBin, TermBin])
    end.

%% receive the Fun0(processed) objects from all the "buckets" on this partition, accumulate them
%%   and return them
receive_fold_results(Fun, Acc) ->
    receive
        {fold_result, done} ->
            %% io:format("receive_fold_results: fold complete [~p objects].~n",
            %%           [Count]),
            Acc;
        {fold_result, IndexBin, FieldTermBin, BObj} ->
            receive_fold_results(Fun, Fun({IndexBin,FieldTermBin},BObj,Acc))
    after ?FOLD_TIMEOUT ->
            throw({timeout,Acc})
    end.

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
-endif.
