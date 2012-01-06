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
            %% Ensure that the KV service has fully loaded.
            riak_core:wait_for_service(riak_kv),

            case riak_search_sup:start_link() of
                {ok, Pid} ->
                    %% Register the search vnode with core and mark the node
                    %% as available for search requests.
                    riak_core:register(riak_search, [
                        {vnode_module, riak_search_vnode},
                        {bucket_fixup, riak_search_kv_hook}
                    ]),

                    %% Register our cluster_info app callback modules, with catch if
                    %% the app is missing or packaging is broken.
                    catch cluster_info:register_app(riak_search_cinfo),

                    Root = app_helper:get_env(riak_search, url_root, "search"),

                    case rs_query_sup:start_link() of
                        {ok, _} ->
                            webmachine_router:add_route({[Root, index],
                                                         rs_query_wm, []}),
                            webmachine_router:add_route({[Root],
                                                         rs_query_wm, []}),
                            {ok, Pid};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        false -> {ok, self()}
    end.

stop(_State) ->
    ok.
