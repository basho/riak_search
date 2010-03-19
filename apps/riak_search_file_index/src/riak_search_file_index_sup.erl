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

-module(riak_search_file_index_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    Master = {riak_search_file_index, 
              {riak_search_file_index, start_link, []},
              permanent, 2000, worker, [riak_search_file_index]},

    Merge = {riak_search_file_index_merge, 
              {riak_search_file_index_merge, start_link, []},
              permanent, 2000, worker, [riak_search_file_index_merge]},

    Stream = {riak_search_file_index_stream, 
              {riak_search_file_index_stream, start_link, []},
              permanent, 2000, worker, [riak_search_file_index_stream]},

    {ok,{{one_for_all,0,1}, [Master, Merge, Stream]}}.
