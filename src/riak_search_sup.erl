%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

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

    VMaster = {riak_search_vnode_master,
               {riak_core_vnode_master, start_link, [riak_search_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    Processes = [Config,
                 VMaster],
    {ok, { {one_for_one, 5, 10}, Processes} }.
