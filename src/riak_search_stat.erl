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
-export([get_stats/0,
         get_stats/1,
         start_link/0,
         update/1]).

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3,
         terminate/2]).

-record(state,
        {
          index_entries,
          index_latency,
          index_pending,
          index_throughput,
          solr_query_latency,
          solr_query_pending,
          solr_query_throughput,
          legacy
        }).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() -> get_stats(slide:moment()).

%% @doc Return aggregation of stats for given `Moment'.
-spec get_stats(integer()) -> proplists:proplist().
get_stats(Moment) -> gen_server:call(?MODULE, {get_stats, Moment}, infinity).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Update the given `Stat'.
-spec update(term()) -> ok.
update(Stat) -> gen_server:cast(?MODULE, {update, Stat, slide:moment()}).

%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------

init([]) ->
    %% TODO hardcode to legacy until basho_metrics is fixed
    case app_helper:get_env(riak_kv, legacy_stats, false) of
        false ->
            lager:warning("Overriding user-setting and using legacy"
                          " stats. Set {legacy_stats,true} to remove "
                          " this message.");
        true -> ok
    end,
    legacy_init().

handle_call({get_stats, Moment}, _From, S) ->
    {reply, produce_stats(Moment, S), S}.

handle_cast({update, Stat, Moment}, S=#state{legacy=B}) ->
    S2 = update(Stat, Moment, S, B),
    {noreply, S2}.

handle_info(Msg, S) ->
    {stop, {unexpected_info, Msg}, S}.

terminate(_Reason, _S) -> ignore.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------

index_stats(Moment, S) ->
    Thru = spiral_minute(Moment, #state.index_throughput, S),
    {_, Mean, {Median, NF, NN, Max}} =
        slide_minute(Moment, #state.index_latency, S, 0,
                     5000000, 20000, down),
    IPending = S#state.index_pending,
    {_, Mean2, {Median2, NF2, NN2, Max2}} =
        slide_minute(Moment, #state.index_entries, S, 0,
                     5000000, 20000, down),
    Thru2 = spiral_minute(Moment, #state.solr_query_throughput, S),
    {_, Mean3, {Median3, NF3, NN3, Max3}} =
        slide_minute(Moment, #state.solr_query_latency, S, 0,
                     5000000, 20000, down),
    QPending = S#state.solr_query_pending,

    [{search_index_throughput, Thru},
     {search_index_latency_mean, Mean},
     {search_index_latency_median, Median},
     {search_index_latency_95, NF},
     {search_index_latency_99, NN},
     {search_index_latency_100, Max},
     {search_index_pending, IPending},
     {search_index_entries_mean, Mean2},
     {search_index_entries_median, Median2},
     {search_index_entries_95, NF2},
     {search_index_entries_99, NN2},
     {search_index_entries_100, Max2},
     {search_solr_query_throughput, Thru2},
     {search_solr_query_latency_mean, Mean3},
     {search_solr_query_latency_median, Median3},
     {search_solr_query_latency_95, NF3},
     {search_solr_query_latency_99, NN3},
     {search_solr_query_latency_100, Max3},
     {search_solr_query_pending, QPending}].
    
legacy_init() ->
    {ok, #state{
       index_entries=slide:fresh(),
       index_latency=slide:fresh(),
       index_pending=0,
       index_throughput=spiraltime:fresh(),
       solr_query_pending=0,
       solr_query_latency=slide:fresh(),
       solr_query_throughput=spiraltime:fresh(),
       legacy=true
      }}.

produce_stats(Moment, S) ->
    L = [index_stats(Moment, S)],
    lists:append(L).

slide_minute(Moment, Elt, State, Min, Max, Bins, RoundingMode) ->
    {Count, Mean, Nines} =
        slide:mean_and_nines(element(Elt, State), Moment, Min, Max,
                             Bins, RoundingMode),
    {Count, Mean, Nines}.

spiral_minute(_Moment, Elt, State) ->
    {_,Count} = spiraltime:rep_minute(element(Elt, State)),
    Count.

update(Stat, Moment, S, true) -> update1(Stat, Moment, S);
update(_, _, _, _) -> throw(new_stats_not_supported).

update1(index_begin, _Moment, S=#state{index_pending=Pending}) ->
    S#state{index_pending=Pending + 1};
update1({index_end, Time}, Moment, S=#state{index_latency=Latency,
                                            index_pending=Pending,
                                            index_throughput=Thru}) ->
    S#state{index_latency=slide:update(Latency, Time, Moment),
            index_pending=Pending - 1,
            index_throughput=spiraltime:incr(1, Moment, Thru)};
update1({index_entries, N}, Moment, S=#state{index_entries=Entries}) ->
    S#state{index_entries=slide:update(Entries, N, Moment)};
update1(solr_query_begin, _Moment, S=#state{solr_query_pending=Pending}) ->
    S#state{solr_query_pending=Pending + 1};
update1({solr_query_end, Time}, Moment,
        S=#state{solr_query_latency=Latency,
                 solr_query_pending=Pending,
                 solr_query_throughput=Thru}) ->
    S#state{solr_query_latency=slide:update(Latency, Time, Moment),
            solr_query_pending=Pending - 1,
            solr_query_throughput=spiraltime:incr(1, Moment, Thru)}.
