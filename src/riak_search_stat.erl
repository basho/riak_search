%% -------------------------------------------------------------------
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% @doc Collector for various Search stats.
-module(riak_search_stat).

-behaviour(gen_server).

%% API
-export([start_link /0, register_stats/0,
         get_stats/0,
         update/1,
         stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(APP, riak_search).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    riak_core_stat:register_stats(?APP, stats()).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() ->
    riak_core_stat:get_stats(?APP).

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

%% gen_server

init([]) ->
    register_stats(),
    {ok, riak_core_stat:prefix()}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, Prefix) ->
    update1(Prefix, Arg),
    {noreply, Prefix};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Update the given `Stat'.
-spec update1(term(), term()) -> ok.
update1(P, index_begin) ->
    exometer:update([P, ?APP, index_pending], 1);
update1(P, {index_end, Time}) ->
    exometer:update([P, ?APP, index_latency], Time),
    exometer:update([P, ?APP, index_throughput], 1),
    exometer:update([P, ?APP, index_pending], -1);
update1(P, {index_entries, N}) ->
    exometer:update([P, ?APP, index_entries], N);
update1(P, search_begin) ->
    exometer:update([P, ?APP, search_pending], 1);
update1(P, {search_end, Time}) ->
    exometer:update([P, ?APP, search_latency], Time),
    exometer:update([P, ?APP, search_throughput], 1),
    exometer:update([P, ?APP, search_pending], -1);
update1(P, search_fold_begin) ->
    exometer:update([P, ?APP, search_fold_pending], 1);
update1(P, {search_fold_end, Time}) ->
    exometer:update([P, ?APP, search_fold_latency], Time),
    exometer:update([P, ?APP, search_fold_throughput], 1),
    exometer:update([P, ?APP, search_fold_pending], -1);
update1(P, search_doc_begin) ->
    exometer:update([P, ?APP, search_doc_pending], 1);
update1(P, {search_doc_end, Time}) ->
    exometer:update([P, ?APP, search_doc_latency], Time),
    exometer:update([P, ?APP, search_doc_throughput], 1),
    exometer:update([P, ?APP, search_doc_pending], -1).


%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------

stats() ->
    [
     {index_entries, histogram},
     {index_latency, histogram},
     {index_pending, counter},
     {index_throughput, spiral},
     {search_pending, counter},
     {search_latency, histogram},
     {search_throughput, spiral},
     {search_fold_pending, counter},
     {search_fold_latency, histogram},
     {search_fold_throughput, spiral},
     {search_doc_pending, counter},
     {search_doc_latency, histogram},
     {search_doc_throughput, spiral}
    ].
