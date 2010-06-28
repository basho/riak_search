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

-export([start/2,stop/1,index/6,info/5,info_range/7,stream/6,catalog_query/3]).
-export([drop/1, is_empty/1, fold/3]).

-export([get/2,put/3,list/1,list_bucket/2,delete/2]).
-export([toggle_raptor_debug/0, shutdown_raptor/0]).
-export([sync/0, poke/1, raptor_status/0]).

-export([test_fold/0, test_is_empty/0, test_drop/0]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_search.hrl").

% @type state() = term().
-record(state, {partition}).

-define(FOLD_TIMEOUT, 30000).
-define(MAX_HANDOFF_STREAMS, 50).
-define(INFO_TIMEOUT, 5000).

%% @spec start(Partition :: integer(), Config :: proplist()) ->
%%          {ok, state()} | {{error, Reason :: term()}, state()}
%% @doc Start this backend.
start(Partition, _Config) ->
    {ok, #state { partition=Partition }}.

%% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(_State) ->
    ok.

%% @spec put(state(), BKey :: riak_object:bkey(), Val :: binary()) ->
%%         ok | {error, Reason :: term()}
%% @doc Route all commands through the object's value.
put(State, _BKey, ObjBin) ->
    Obj = binary_to_term(ObjBin),
    Command = riak_object:get_value(Obj),
    handle_command(State, Command).

index(Index, Field, Term, Value, Props, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = raptor_conn_pool:checkout(),
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

info(Index, Field, Term, Sender, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = raptor_conn_pool:checkout(),
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
                           raptor_conn_pool:checkin(Conn)
                       end
    end),

    noreply.

info_range(Index, Field, StartTerm, EndTerm, _Size, Sender, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = raptor_conn_pool:checkout(),
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
                           raptor_conn_pool:checkin(Conn)
                       end
    end),
    noreply.

stream(Index, Field, Term, FilterFun, Sender, State) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = raptor_conn_pool:checkout(),
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
                                       raptor_conn_pool:checkin(Conn)
                                   end
               end),
    noreply.

catalog_query(CatalogQuery, Sender, _State) ->
    {ok, Conn} = raptor_conn_pool:checkout(),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:catalog_query(
                                               Conn,
                                               CatalogQuery),
                           receive_catalog_query_results(StreamRef, Sender)
                       after
                           raptor_conn_pool:checkin(Conn)
                       end
               end),
    noreply.

fold(Folder, Acc, State) ->
%% spawn a process to kick off the catalog listing
%%   for the this partition, e.g.,
    io:format("fold(~p, ~p, ~p)~n", [State, Folder, Acc]),
    io:format("fold/sync...~n"),
    sync(),
    io:format("fold/sync complete.~n"),
    Partition = integer_to_list(State#state.partition),
    {ok, Conn} = raptor_conn_pool:checkout(),
    Me = self(),
    FoldCatRef = {fold_catalog, erlang:make_ref()},
    CatalogResultsPid = spawn_link(fun() ->
                                           fold_catalog_process(FoldCatRef, Me, Folder, Acc, 
                                                                false, 0, 0, false) end),
    spawn_link(fun() ->
                       try
                           {ok, StreamRef} = raptor_conn:catalog_query(
                                               Conn,
                                               ["partition_id:\"", Partition , "\""]),
                           Sender = {pid, FoldCatRef, CatalogResultsPid},
                           receive_catalog_query_results(StreamRef, Sender)
                       after
                           raptor_conn_pool:checkin(Conn)
                       end end),
    receive_fold_results(Acc, 0).

is_empty(State) ->
    io:format("is_empty(~p)~n", [State]),
    Partition = to_binary(State#state.partition),
    handle_command(no_state, {command,
                              <<"partition_count">>,
                              Partition,
                              <<"">>,
                              <<"">>}) == "0".

drop(State) ->
    Partition = to_binary(State#state.partition),
    handle_command(no_state, {command,
                              <<"drop_partition">>,
                              Partition,
                              <<"">>,
                              <<"">>}),
    ok.

handle_command(State, {delete_entry, Index, Field, Term, DocId}) ->
    Partition = to_binary(State#state.partition),
    {ok, Conn} = raptor_conn_pool:checkout(),
    try
        raptor_conn:delete_entry(Conn,
                                 to_binary(Index),
                                 to_binary(Field),
                                 to_binary(Term),
                                 to_binary(DocId),
                                 Partition)
    after
        raptor_conn_pool:checkin(Conn)
    end,
    ok;

handle_command(_State, {command, Command, Arg1, Arg2, Arg3}) ->
    {ok, Conn} = raptor_conn_pool:checkout(),
    try
        {ok, _StreamRef} = raptor_conn:command(Conn, Command, Arg1, Arg2, Arg3),
        receive
            {command, _ReqId, Response} ->
                Response
        end
    after
        raptor_conn_pool:checkin(Conn)
    end;

handle_command(_State, Other) ->
    throw({unexpected_operation, Other}).

receive_stream_results(StreamRef, Sender, FilterFun) ->
    receive_stream_results(StreamRef, Sender, FilterFun, []).

receive_stream_results(StreamRef, Sender, FilterFun, Acc0) ->
    case length(Acc0) > 500 of
        true ->
            io:format("chunk (~p) ~p~n", [length(Acc0), StreamRef]),
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
        Msg ->
            %% TODO: Should this throw - must be an error
            io:format("receive_stream_results(~p, ~p, ~p) -> ~p~n",
                [StreamRef, Sender, FilterFun, Msg]),
            riak_search_backend:stream_response_done(Sender)
    end,
    ok.

%% receive_stream_results2(StreamRef, OutputPid, OutputRef, FilterFun) ->
%%     receive
%%         {stream, StreamRef, "$end_of_table", _} ->
%%             OutputPid ! {result, '$end_of_table', OutputRef};
%%         {stream, StreamRef, Value, Props} ->
%%             Props2 = binary_to_term(Props),
%%             case FilterFun(Value, Props2) of
%%                 true -> OutputPid ! {result, {Value, Props2}, OutputRef};
%%                 _ -> skip
%%             end,
%%             receive_stream_results(StreamRef, OutputPid, OutputRef, FilterFun);
%%         Msg ->
%%             io:format("receive_stream_results(~p, ~p, ~p, ~p) -> ~p~n",
%%                 [StreamRef, OutputPid, OutputRef, FilterFun, Msg]),
%%             OutputPid ! {result, '$end_of_table', OutputRef}
%%     end,
%%     ok.

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
                                                       Field, Term, JSONProps),
            receive_catalog_query_results(StreamRef, Sender)
    end,
    ok.

%% @spec get(state(), BKey :: riak_object:bkey()) ->
%%         {ok, Val :: binary()} | {error, Reason :: term()}
%% @doc Get the object stored at the given bucket/key pair. The merge
%% backend does not support key-based lookups, so always return
%% {error, notfound}.
get(_State, _BKey) ->
    {error, notfound}.


%% @spec delete(state(), BKey :: riak_object:bkey()) ->
%%          ok | {error, Reason :: term()}
%% @doc Writes are not supported.
delete(_State, _BKey) ->
    {error, not_supported}.

%% @spec list(state()) -> [{Bucket :: riak_object:bucket(),
%%                          Key :: riak_object:key()}]
%% @doc Get a list of all bucket/key pairs stored by this backend
list(_State) ->
    io:format("list(~p)~n", [_State]),
    throw({error, not_supported}).

%% @spec list_bucket(state(), riak_object:bucket()) ->
%%           [riak_object:key()]
%% @doc Get a list of the keys in a bucket
list_bucket(_State, _Bucket) ->
    io:format("list_bucket(~p, ~p)~n", [_State, _Bucket]),
    throw({error, not_supported}).

%% receive catalog entries for current partition & kick
%%   off a stream process for each one in parallel
fold_catalog_process(CatRef,
                     FoldResultPid,
                     Fun0, Acc,
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
                    fold_catalog_process(CatRef, FoldResultPid, Fun0, Acc, true,
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
                    fold_catalog_process(CatRef, FoldResultPid, Fun0, Acc, CatalogDone,
                                         StreamProcessCount, FinishedStreamProcessCount, true);
                false ->
                    spawn_link(fun() ->
                        io:format("fold_catalog_process: catalog_query_response: ~p: ~p.~p.~p (~p)~n",
                            [Partition, Index, Field, Term, JSONProps]),
                        {ok, Conn} = raptor_conn_pool:checkout(),
                        try
                          {ok, StreamRef} = raptor_conn:stream(
                              Conn,
                              to_binary(Index),
                              to_binary(Field),
                              to_binary(Term),
                              to_binary(Partition)),
                          fold_stream_process(Me, FoldResultPid, StreamRef, Fun0, Acc, 
                                              Index, Field, Term)
                        after 
                            raptor_conn_pool:checkin(Conn)
                        end end),
                    fold_catalog_process(CatRef, FoldResultPid, Fun0, Acc, CatalogDone,
                                         StreamProcessCount+1, FinishedStreamProcessCount, false)
                end;

        {fold_stream, done, _StreamRef1} ->
            %%io:format("fold_stream: done: ~p of ~p~n",
            %%    [FinishedStreamProcessCount, StreamProcessCount]),
            case FinishedStreamProcessCount >= (StreamProcessCount-1) andalso
                 CatalogDone == true andalso
                 DeferredTables == false of
                    true ->
                        io:format("fold_catalog_process: streaming complete (~p of ~p)~n",
                            [(FinishedStreamProcessCount+1), StreamProcessCount]),
                        FoldResultPid ! {fold_result, done};
                    false ->
                        fold_catalog_process(CatRef, FoldResultPid, Fun0, Acc, CatalogDone,
                            StreamProcessCount, FinishedStreamProcessCount+1, false)
                end;
        Msg ->
            io:format("fold_catalog_process: unknown message: ~p~n", [Msg]),
            fold_catalog_process(CatRef, FoldResultPid, Fun0, Acc, CatalogDone,
                StreamProcessCount, FinishedStreamProcessCount, DeferredTables)
        after ?FOLD_TIMEOUT ->
            case FinishedStreamProcessCount >= (StreamProcessCount) of
                true -> ok;
                false ->
                    io:format("fold_catalog_process: timed out (~p of ~p), proceeding to {fold_result, done}~n",
                        [FinishedStreamProcessCount, StreamProcessCount])
            end,
            FoldResultPid ! {fold_result, done}
    end.

%% for each result of a stream process, package the entry in the
%%   form of an infamously shady put-embedded command and forward
%%   to receive_fold_results process
fold_stream_process(CatalogProcessPid, FoldResultPid, StreamRef, Fun0, Acc, Index, Field, Term) ->
    receive
        {stream, StreamRef, timeout} ->
            CatalogProcessPid ! {fold_stream, done, StreamRef};
        {stream, StreamRef, "$end_of_table", _} ->
            CatalogProcessPid ! {fold_stream, done, StreamRef},
            io:format("fold_stream_process: table complete: ~p.~p.~p~n",
                [Index, Field, Term]);
        {stream, StreamRef, Value, Props} ->
            IndexBin = riak_search_utils:to_binary(Index),
            FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
            BObj = term_to_binary({Field, Term, Value, Props}),
            FoldResultPid ! {fold_result, Fun0({IndexBin, FieldTermBin}, BObj, Acc)},
            fold_stream_process(CatalogProcessPid, FoldResultPid, StreamRef, Fun0, Acc, Index, Field, Term);
        Msg ->
            io:format("fold_stream_process: unknown message: ~p~n", [Msg]),
            fold_stream_process(CatalogProcessPid, FoldResultPid, StreamRef, Fun0, Acc, Index, Field, Term)
        after ?FOLD_TIMEOUT ->
            CatalogProcessPid ! {fold_stream, done, StreamRef},
            error_logger:warning_msg("fold_stream_process: table timed out: ~p.~p.~p~n",
                                      [Index, Field, Term])
    end.

%% receive the Fun0(processed) objects from all the "buckets" on this partition, accumulate them
%%   and return them
receive_fold_results(Acc, Count) ->
    receive
        {fold_result, done} ->
            io:format("receive_fold_results: fold complete [~p objects].~n",
                [Count]),
            Acc;
        {fold_result, _Obj} ->
            receive_fold_results(Acc, Count+1)
    end.

%%%

to_binary(A) when is_atom(A) -> to_binary(atom_to_list(A));
to_binary(B) when is_binary(B) -> B;
to_binary(I) when is_integer(I) -> to_binary(integer_to_list(I));
to_binary(L) when is_list(L) -> list_to_binary(L).

poke(Command) ->
    handle_command(no_state, {command, Command, <<"">>, <<"">>, <<"">>}).

sync() ->
    poke("sync").

toggle_raptor_debug() ->
    poke("toggle_debug").

shutdown_raptor() ->
    io:format("issuing raptor engine shutdown~n"),
    poke("shutdown").

raptor_status() ->
    Status = string:tokens(poke("status"), "`"),
    io:format("~p~n", [Status]),
    Status.

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
