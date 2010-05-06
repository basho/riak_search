%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(basho_bench_driver_merge_index).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { pid }).
-define(INDEX, <<"index">>).
-define(FIELD, <<"field">>).
-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Get reference to local merge_index.
    Root = "../data_" ++ integer_to_list(Id),
    SyncInterval = basho_bench_config:get(merge_index_sync_interval, 30 * 1000),
    RolloverSize = basho_bench_config:get(merge_index_rollover_size, 50 * 1024 * 1024),
    Options = [{merge_index_sync_interval, SyncInterval}, {merge_index_rollover_size, RolloverSize}],
    {ok, Pid} = merge_index:start_link(Root, Options),
    {ok, #state { pid=Pid }}.

now_to_timestamp({Mega, Sec, Micro}) ->
    <<TS:64/integer>> = <<Mega:16/integer, Sec:24/integer, Micro:24/integer>>,
    TS.

run(index, KeyGen, ValueGen, State) ->
%%     ?PRINT(index),
    #state { pid=Pid } = State,
    TS = now_to_timestamp(now()),
    merge_index:index(Pid, ?INDEX, ?FIELD, KeyGen(), ValueGen(), [], TS),
    {ok, State};

run(info, KeyGen, _ValueGen, State) ->
%%     ?PRINT(info),
    #state { pid=Pid } = State,
    merge_index:info(Pid, ?INDEX, ?FIELD, KeyGen()),
    {ok, State};

run(stream, KeyGen, _ValueGen, State) ->
%%     ?PRINT(stream),
    #state { pid=Pid } = State,
    Ref = make_ref(),
    F = fun(_X, _Y) -> true end,
    merge_index:stream(Pid, ?INDEX, ?FIELD, KeyGen(), self(), Ref, F),
    collect_stream(Ref, 0, undefined),
    {ok, State}.

collect_stream(Ref, Count, LastKey) ->
    receive 
        {result, '$end_of_table', Ref} ->
            %% ?PRINT({stream_count, Count}),
            ok;
        {result, {Key, _Props}, Ref} when (LastKey == undefined orelse LastKey =< Key) ->
            collect_stream(Ref, Count + 1, Key);
        {result, {Key, _Props}, Ref} ->
            throw({key_out_of_order, Key})
    end.
