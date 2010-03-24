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

-module(merge_index_backend).
-export([start/2,stop/1,get/2,put/3,list/1,list_bucket/2,delete/2]).
-export([fold/3, drop/1, is_empty/1]).

-include_lib("eunit/include/eunit.hrl").
% @type state() = term().
-record(state, {pid}).

%% @spec start(Partition :: integer(), Config :: proplist()) ->
%%          {ok, state()} | {{error, Reason :: term()}, state()}
%% @doc Start this backend.
start(Partition, Config) ->
    DefaultRootPath = filename:join([".", "data", "merge_index"]),
    RootPath = proplists:get_value(merge_index_backend_root, Config, DefaultRootPath),
    Rootfile = filename:join([RootPath, integer_to_list(Partition)]),
    {ok, Pid} = merge_index:start_link(Rootfile, Config),
    {ok, #state { pid=Pid }}.

%% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(_State) -> ok.

%% @spec get(state(), BKey :: riak_object:bkey()) ->
%%         {ok, Val :: binary()} | {error, Reason :: term()}
%% @doc Get the object stored at the given bucket/key pair. The merge
%% backend does not support key-based lookups, so always return
%% {error, notfound}.
get(_State, _BKey) ->
    {error, notfound}.


%% @spec put(state(), BKey :: riak_object:bkey(), Val :: binary()) ->
%%         ok | {error, Reason :: term()}
%% @doc Route all commands through the object's value.
put(State, BKey, ObjBin) ->      
    Pid = State#state.pid,
    {Index, FieldTerm} = BKey,
    IndexFieldTerm = list_to_binary([Index, ".", FieldTerm]),
    Obj = binary_to_term(ObjBin),
    Command = riak_object:get_value(Obj),
    handle_command(Pid, IndexFieldTerm, Command).
    
handle_command(Pid, IndexFieldTerm, {put, Value, Props}) ->
    %% io:format("Got a put: ~p ~p ~p~n", [BucketName, Value, Props]),
    %% Put with properties.
    merge_index:put(Pid, IndexFieldTerm, Value, Props),
    ok;

handle_command(Pid, IndexFieldTerm, {stream, OutputPid, OutputRef, FilterFun}) ->
    %% io:format("Got a stream: ~p ~p ~p~n", [BucketName, OutputPid, OutputRef]),
    %% Stream some results.
    merge_index:stream(Pid, IndexFieldTerm, OutputPid, OutputRef, FilterFun),
    ok;

handle_command(Pid, _, {range, Start, End, Inclusive, OutputPid, OutputRef}) ->
    {ok, Range} = merge_index:range(Pid, Start, End, Inclusive),
    OutputPid!{range_response, Range, OutputRef},
    ok;

handle_command(_Pid, IndexFieldTerm, Other) ->
    throw({unexpected_operation, IndexFieldTerm, Other}).



%% @spec delete(state(), BKey :: riak_object:bkey()) ->
%%          ok | {error, Reason :: term()}
%% @doc Writes are not supported.
delete(_State, _BKey) ->
    {error, not_supported}.

drop(_State) ->
    throw({error, not_supported}).

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

is_empty(State) -> 
    Pid = State#state.pid,
    merge_index:is_empty(Pid).

fold(_State, _Fun0, _Acc) ->
    throw({error, not_supported}).
