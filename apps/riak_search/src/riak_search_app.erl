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

-define(SEARCH_BUCKET_PROPS, [{n_val, 2},
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
                                error_logger:info_msg("Configuring search index ~p~n", [Bucket]),
                                ok = riak_core_bucket:set_bucket(list_to_binary(Bucket), ?SEARCH_BUCKET_PROPS)
                        end,
                    [F(Bucket) || Bucket <- Buckets]
            end,

            %% Register the search vnode with core and mark the node
            %% as available for search requests.
            riak_core:register_vnode_module(riak_search_vnode),
            riak_core_node_watcher:service_up(riak_search, self()),

           {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.
