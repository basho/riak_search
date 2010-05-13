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

-module(raptor_index_backend).
-author("John Muellerleile <johnm@basho.com>").
-export([start/2,stop/1,get/2,put/3,list/1,list_bucket/2,delete/2]).
-export([fold/3, drop/1, is_empty/1]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_search.hrl").

% @type state() = term().
-record(state, {partition, conn}).

%% @spec start(Partition :: integer(), Config :: proplist()) ->
%%          {ok, state()} | {{error, Reason :: term()}, state()}
%% @doc Start this backend.
start(Partition, Config) ->
    {ok, Conn} = raptor_conn_sup:new_conn(),
    {ok, #state { partition=Partition, conn=Conn }}.

%% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(State) ->
    ok.

%% @spec put(state(), BKey :: riak_object:bkey(), Val :: binary()) ->
%%         ok | {error, Reason :: term()}
%% @doc Route all commands through the object's value.
put(State, _BKey, ObjBin) ->
    Obj = binary_to_term(ObjBin),
    Command = riak_object:get_value(Obj),
    handle_command(State, Command).

handle_command(State, {index, Index, Field, Term, Value, Props}) ->
    TS = mi_utils:now_to_timestamp(erlang:now()),
    handle_command(State, {index, Index, Field, Term, 0, 0, Value, Props, TS});

handle_command(State, {index, Index, Field, Term, SubType, SubTerm, Value, Props, Timestamp}) ->
    %% Put with properties.
    Partition = list_to_binary("" ++ integer_to_list(State#state.partition)),
    Conn = State#state.conn,
    raptor_conn:index(Conn, 
        list_to_binary(Index), 
        list_to_binary(Field), 
        Term, 
        list_to_binary(integer_to_list(SubType)), 
        list_to_binary(integer_to_list(SubTerm)), 
        list_to_binary(Value), 
        Partition,
        term_to_binary(Props)),
    ok;

handle_command(State, {index, Index, Field, Term, SubType, SubTerm, Value, Props}) ->
    Partition = list_to_binary(integer_to_list(State#state.partition)),
    Conn = State#state.conn,
    raptor_conn:index(Conn, 
        list_to_binary(Index), 
        list_to_binary(Field), 
        Term, 
        list_to_binary(integer_to_list(SubType)), 
        list_to_binary(integer_to_list(SubTerm)), 
        list_to_binary(Value), 
        Partition,
        term_to_binary(Props)),
    %%TS = mi_utils:now_to_timestamp(erlang:now()),
    ok;

handle_command(State, {init_stream, OutputPid, OutputRef}) ->
    %% Do some handshaking so that we only stream results from one partition/node.
    Partition = State#state.partition,
    OutputPid!{stream_ready, Partition, node(), OutputRef},
    ok;

handle_command(State, {stream, Index, Field, Term, SubType, StartSubTerm, EndSubTerm, OutputPid, OutputRef, DestPartition, Node, FilterFun}) ->
    Partition = list_to_binary(integer_to_list(State#state.partition)),
    {ok, Conn} = raptor_conn_sup:new_conn(),
    case DestPartition == State#state.partition andalso Node == node() of
        true ->
            case SubType of
                all ->
                    SubType2 = 0;
                _ ->
                    SubType2 = SubType
            end,
            case StartSubTerm of
                all ->
                    StartSubTerm2 = 0;
                _ ->
                    StartSubTerm2 = StartSubTerm
            end,
            case EndSubTerm of
                all ->
                    EndSubTerm2 = 0;
                _ ->
                    EndSubTerm2 = EndSubTerm
            end,
            
            spawn(fun() ->
                {ok, StreamRef} = raptor_conn:stream(
                    Conn, 
                    list_to_binary(Index), 
                    list_to_binary(Field), 
                    list_to_binary(Term), 
                    list_to_binary(integer_to_list(SubType2)), 
                    list_to_binary(integer_to_list(StartSubTerm2)), 
                    list_to_binary(integer_to_list(EndSubTerm2)), 
                    Partition),
                receive_stream_results(StreamRef, OutputPid, OutputRef, FilterFun),
                raptor_conn:close(Conn)
            end),
            ok;
        false ->
            %% The requester doesn't want results from this node, so
            %% ignore. This is a hack, to get around the fact that
            %% there is no way to send a put or other command to a
            %% specific v-node.
            ignore
    end,
    ok;

handle_command(State, {info, Index, Field, Term, OutputPid, OutputRef}) ->
    Partition = list_to_binary(integer_to_list(State#state.partition)),
    {ok, Conn} = raptor_conn_sup:new_conn(),
    spawn(fun() ->
        {ok, StreamRef} = raptor_conn:info(
            Conn, 
            list_to_binary(Index), 
            list_to_binary(Field), 
            list_to_binary(Term), 
            Partition),
        receive_info_results(StreamRef, OutputPid, OutputRef),
        raptor_conn:close(Conn)
    end),
    ok;

handle_command(State, {info_range, Index, Field, StartTerm, EndTerm, Size, OutputPid, OutputRef}) ->
    Partition = list_to_binary(integer_to_list(State#state.partition)),
    {ok, Conn} = raptor_conn_sup:new_conn(),
    spawn(fun() ->
        {ok, StreamRef} = raptor_conn:info_range(
            Conn, 
            list_to_binary(Index), 
            list_to_binary(Field), 
            list_to_binary(StartTerm), 
            list_to_binary(EndTerm), 
            Partition),
        receive_info_range_results(StreamRef, OutputPid, OutputRef, []),
        raptor_conn:close(Conn)
    end),
    ok;

handle_command(_State, Other) ->
    throw({unexpected_operation, Other}).

receive_stream_results(StreamRef, OutputPid, OutputRef, FilterFun) ->
    receive
        {stream, Ref, "$end_of_table", _} ->
            OutputPid ! {result, '$end_of_table', OutputRef};
        {stream, Ref, Value, Props} ->
            Props2 = binary_to_term(Props),
            case FilterFun(Value, Props2) of
                true -> OutputPid ! {result, {Value, Props2}, OutputRef};
                _ -> skip
            end,
            receive_stream_results(StreamRef, OutputPid, OutputRef, FilterFun);
        Msg ->
            io:format("receive_stream_results(~p, ~p, ~p, ~p) -> ~p~n",
                [StreamRef, OutputPid, OutputRef, FilterFun, Msg]),
            OutputPid ! {result, '$end_of_table', OutputRef}
    end,
ok.

receive_info_range_results(StreamRef, OutputPid, OutputRef, Results) ->
    receive
        {info, StreamRef, "$end_of_info", 0} ->
            OutputPid ! {info_response, Results, OutputRef};
        {info, StreamRef, Term, Count} ->
            receive_info_range_results(StreamRef, OutputPid, OutputRef,
                Results ++ [{Term, node(), Count}]);
        Msg ->
            io:format("receive_info_range_results(~p, ~p, ~p) -> ~p~n",
                [StreamRef, OutputPid, OutputRef, Msg]),
            receive_info_range_results(StreamRef, OutputPid, OutputRef, [])
    end,
ok.

receive_info_results(StreamRef, OutputPid, OutputRef) ->
    receive
        {info, StreamRef, Term, Count} ->
            OutputPid ! {info_response, [{Term, node(), Count}], OutputRef};
        Msg ->
            io:format("receive_info_results(~p, ~p, ~p) -> ~p~n",
                [StreamRef, OutputPid, OutputRef, Msg]),
            OutputPid ! []
    end,
ok.


%% @spec get(state(), BKey :: riak_object:bkey()) ->
%%         {ok, Val :: binary()} | {error, Reason :: term()}
%% @doc Get the object stored at the given bucket/key pair. The merge
%% backend does not support key-based lookups, so always return
%% {error, notfound}.
get(_State, _BKey) ->
    {error, notfound}.

is_empty(State) ->
    Partition = list_to_binary(integer_to_list(State#state.partition)),
    Conn = State#state.conn,
    true.
    % xxx TODO
    %merge_index:is_empty(Pid).

fold(State, Fun, Acc) ->
    io:format("fold(~p, ~p, ~p)~n",
        [State, Fun, Acc]),
    [].

fold2(State, Fun, Acc) ->
    ?PRINT({fold, State, Fun, Acc}),
    %% The supplied function expects a BKey and an Object. Wrap this
    %% So that we can use the format that merge_index expects.
    WrappedFun = fun(Index, Field, Term, SubType, SubTerm, Value, Props, TS, AccIn) ->
        ?PRINT({wrapped_fun, Index, Field}),
        %% Construct the object...
        IndexBin = riak_search_utils:to_binary(Index),
        FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
        Payload = {index, Index, Field, Term, SubType, SubTerm, Value, Props, TS},
        BObj = term_to_binary(riak_object:new(IndexBin, FieldTermBin, Payload)),
        Fun({IndexBin, FieldTermBin}, BObj, AccIn)
    end,
    %%Pid = State#state.pid,
    %%{ok, FoldResult} = merge_index:fold(Pid, WrappedFun, Acc),
    %%FoldResult.
    ok.

drop(State) ->
    Partition = list_to_binary(integer_to_list(State#state.partition)),
    Conn = State#state.conn,
    ok.
    %merge_index:drop(Pid).

%% @spec delete(state(), BKey :: riak_object:bkey()) ->
%%          ok | {error, Reason :: term()}
%% @doc Writes are not supported.
delete(_State, _BKey) ->
    {error, not_supported}.

%% @spec list(state()) -> [{Bucket :: riak_object:bucket(),
%%                          Key :: riak_object:key()}]
%% @doc Get a list of all bucket/key pairs stored by this backend
list(_State) ->
    throw({error, not_supported}).


%% @spec list_bucket(state(), riak_object:bucket()) ->
%%           [riak_object:key()]
%% @doc Get a list of the keys in a bucket
list_bucket(_State, _Bucket) ->
    throw({error, not_supported}).

