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

%% API
-export([register_stats/0,
         get_stats/0,
         update/1,
        stats/0]).

-define(APP, riak_search).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

register_stats() ->
    [register_stat(Stat, Type) || {Stat, Type} <- stats()].

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() -> 
    {?APP, [{Name, get_metric_value({?APP, Name}, Type)} || {Name, Type} <- stats()]}.

%% @doc Update the given `Stat'.
-spec update(term()) -> ok.
update(index_begin) ->
    folsom_metrics:notify_existing_metric({?APP, index_pending}, {inc, 1}, counter);
update({index_end, Time}) ->
    folsom_metrics:notify_existing_metric({?APP, index_latency}, Time, histogram),
    folsom_metrics:notify_existing_metric({?APP, index_throughput}, 1, meter),
    folsom_metrics:notify_existing_metric({?APP, index_pending}, {dec, 1}, counter);
update({index_entries, N}) ->
    folsom_metrics:notify_existing_metric({?APP, index_entries}, N, histogram);
update(solr_query_begin) ->
    folsom_metrics:notify_existing_metric({?APP, solr_query_pending}, {inc, 1}, counter);
update({solr_query_end, Time}) ->
    folsom_metrics:notify_existing_metric({?APP, solr_query_latency}, Time, histogram),
    folsom_metrics:notify_existing_metric({?APP, solr_query_throughput}, 1, meter),
    folsom_metrics:notify_existing_metric({?APP, solr_query_pending}, {dec, 1}, counter).

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------
get_metric_value(Name, histogram) ->
    folsom_metrics:get_histogram_statistics(Name);
get_metric_value(Name, _Type) ->
    folsom_metrics:get_metric_value(Name).

stats() ->
    [{index_entries, histogram},
     {index_latency, histogram},
     {index_pending, counter},
     {index_throughput, meter},
     {solr_query_pending, counter},
     {solr_query_latency, histogram},
     {solr_query_throughput, meter}].

register_stat(Name, histogram) ->
%% get the global default histo type
    {SampleType, SampleArgs} = get_sample_type(Name),
    folsom_metrics:new_histogram({?APP, Name}, SampleType, SampleArgs);
register_stat(Name, meter) ->
    folsom_metrics:new_meter({?APP, Name});
register_stat(Name, counter) ->
    folsom_metrics:new_counter({?APP, Name}).

get_sample_type(Name) ->
    SampleType0 = app_helper:get_env(riak_search, stat_sample_type, {slide_uniform, {60, 1028}}),
    app_helper:get_env(riak_search, Name, SampleType0).
