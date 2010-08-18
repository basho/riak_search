%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case riak_search_sup:start_link() of
        {ok, Pid} ->
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
