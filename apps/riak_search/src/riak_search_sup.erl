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

-module(riak_search_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Config = {riak_search_config,
              {riak_search_config, start_link, []},
              permanent, 5000, worker, [riak_search_config]},
    VSup = {riak_search_vnode_sup,
            {riak_search_vnode_sup, start_link, []},
            permanent, infinity, supervisor, [riak_search_vnode_sup]},
    VMaster = {riak_search_vnode_master,
               {riak_core_vnode_master, start_link, [riak_search_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},

    Processes = [Config,
                 VSup,
                 VMaster],
    {ok, { {one_for_one, 5, 10}, Processes} }.

