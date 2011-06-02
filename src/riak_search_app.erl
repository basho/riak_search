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
    case app_helper:get_env(riak_search, enabled, false) of
        true ->
            case riak_search_sup:start_link() of
                {ok, Pid} ->
                    %% Register the search vnode with core and mark the node
                    %% as available for search requests.
                    riak_core:register_vnode_module(riak_search_vnode),
                    riak_core_node_watcher:service_up(riak_search, self()),

                    %% Register our cluster_info app callback modules, with catch if
                    %% the app is missing or packaging is broken.
                    catch cluster_info:register_app(riak_search_cinfo),

                    {ok, Pid};
                Error ->
                    Error
            end;
        false -> noop
    end.

stop(_State) ->
    ok.
