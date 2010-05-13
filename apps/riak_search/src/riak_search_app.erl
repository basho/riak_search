%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at

%%   http://www.apache.org/licenses/LICENSE-2.0

%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_search_app).

-behaviour(application).

-define(SEARCH_BUCKET_PROPS, [{n_val, 1},
                              {backend, search_backend}]).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case riak_search_sup:start_link() of
        {ok, Pid} ->
            case application:get_env(riak_search, search_buckets) of
                undefined ->
            error_logger:info_msg("No search buckets defined");
                {ok, Buckets} ->
                    F = fun(Bucket) ->
                                error_logger:info_msg("Configuring search bucket ~p~n", [Bucket]),
                                ok = riak_core_bucket:set_bucket(list_to_binary(Bucket), ?SEARCH_BUCKET_PROPS)
                        end,
                    [F(Bucket) || Bucket <- Buckets]
            end,

            %% Set up the search_broadcast bucket. Any operations on
            %% this bucket will broadcast to all search_backend
            %% partitions.
            RingSize = app_helper:get_env(riak_core, ring_creation_size),
            riak_core_bucket:set_bucket(<<"search_broadcast">>, [{n_val, RingSize},
                                                                 {backend, search_backend}]),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.
