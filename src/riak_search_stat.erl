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
         produce_stats/0,
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
    [(catch exometer_entry:delete([?APP, Name])) || {Name, _Type} <- stats()],
    [register_stat(Stat, Type) || {Stat, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

produce_stats() ->
    {?APP, [{Name, get_metric_value({?APP, Name}, Type)} || {Name, Type} <- stats()]}.

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, State) ->
    update1(Arg),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Update the given `Stat'.
-spec update1(term()) -> ok.
update1(index_begin) ->
    exometer_entry:update([?APP, index_pending], 1);
update1({index_end, Time}) ->
    exometer_entry:update([?APP, index_latency], Time),
    exometer_entry:update([?APP, index_throughput], 1),
    exometer_entry:update([?APP, index_pending], -1);
update1({index_entries, N}) ->
    exometer_entry:update([?APP, index_entries], N);
update1(search_begin) ->
    exometer_entry:update([?APP, search_pending], 1);
update1({search_end, Time}) ->
    exometer_entry:update([?APP, search_latency], Time),
    exometer_entry:update([?APP, search_throughput], 1),
    exometer_entry:update([?APP, search_pending], -1);
update1(search_fold_begin) ->
    exometer_entry:update([?APP, search_fold_pending], 1);
update1({search_fold_end, Time}) ->
    exometer_entry:update([?APP, search_fold_latency], Time),
    exometer_entry:update([?APP, search_fold_throughput], 1),
    exometer_entry:update([?APP, search_fold_pending], -1);
update1(search_doc_begin) ->
    exometer_entry:update([?APP, search_doc_pending], 1);
update1({search_doc_end, Time}) ->
    exometer_entry:update([?APP, search_doc_latency], Time),
    exometer_entry:update([?APP, search_doc_throughput], 1),
    exometer_entry:update([?APP, search_doc_pending], -1).


%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------
get_metric_value(Name, _Type) ->
    case exometer_entry:get_value(Name) of
	{ok, Value} ->
	    Value;
	{error,_} ->
	    unavailable
    end.


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

register_stat(Name, histogram) ->
%% get the global default histo type
    {SampleType, SampleArgs} = get_sample_type(Name),
    exometer_entry:new([?APP, Name], {histogram, SampleType, SampleArgs});
register_stat(Name, spiral) ->
    exometer_entry:new([?APP, Name], spiral);
register_stat(Name, counter) ->
    exometer_entry:new([?APP, Name], counter).

get_sample_type(Name) ->
    SampleType0 = app_helper:get_env(riak_search, stat_sample_type, {slide_uniform, {60, 1028}}),
    app_helper:get_env(riak_search, Name, SampleType0).
