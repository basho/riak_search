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
                    case app_helper:get_env(riak_search,
                                            rolling_upgrade_code_swap,
                                            true) of
                        true -> rolling_upgrade();
                        false -> noop
                    end,

                    %% Register the search vnode with core and mark the node
                    %% as available for search requests.
                    riak_core:register([
                            {vnode_module, riak_search_vnode},
                            {bucket_fixup, riak_search_kv_hook}
                        ]),
                    riak_core_node_watcher:service_up(riak_search, self()),

                    %% Register our cluster_info app callback modules, with catch if
                    %% the app is missing or packaging is broken.
                    catch cluster_info:register_app(riak_search_cinfo),

                    Root = app_helper:get_env(riak_solr, solr_name, "solr"),
                    case riak_solr_sup:start_link() of
                        {ok, _} ->
                            webmachine_router:add_route({[Root, index, "update"],
                                                         riak_solr_indexer_wm, []}),
                            webmachine_router:add_route({[Root, "update"],
                                                         riak_solr_indexer_wm, []}),
                            webmachine_router:add_route({[Root, index, "select"],
                                                         riak_solr_searcher_wm, []}),
                            webmachine_router:add_route({[Root, "select"],
                                                         riak_solr_searcher_wm, []}),
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

rolling_upgrade() ->
    case all_app_versions(riak_search) of
        ["0.14.2","1.0.0"] ->
            lager:info("detected rolling upgrade from 0.14.2 to 1.0.0"),
            load_mod_on_all_nodes(riak_search_client)
    end.

all_app_versions(App) ->
    {Replies, _} = rpc:multicall(application, get_key, [App, vsn]),
    ordsets:from_list([Vsn || {ok, Vsn} <- Replies]).

load_mod_on_all_nodes(Mod) ->
    {Mod, Bin, File} = code:get_object_code(Mod),
    {_Replies, _} = rpc:multicall(code, load_binary, [Mod, File, Bin]),
    lager:info("loaded code for ~p on all nodes", [Mod]).
