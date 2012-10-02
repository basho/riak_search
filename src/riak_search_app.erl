%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_app).

-behaviour(application).

%% Application callbacks
-export([start/2, prep_stop/1, stop/1]).

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
                            {bucket_fixup, riak_search_kv_hook},
                            {repl_helper, riak_search_repl_helper},
                            {stat_mod, riak_search_stat}
                        ]),

                    %% Register our cluster_info app callback modules, with catch if
                    %% the app is missing or packaging is broken.
                    catch cluster_info:register_app(riak_search_cinfo),

                    Root = app_helper:get_env(riak_solr, solr_name, "solr"),

                    ok = riak_api_pb_service:register(riak_search_pb_query, 27, 28),

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
        false ->
            {ok, self()}
    end.

%% @doc Prepare to stop - called before the supervisor tree is shutdown
prep_stop(_State) ->
    try %% wrap with a try/catch - application carries on regardless, 
        %% no error message or logging about the failure otherwise.
        lager:info("Stopping application riak_search - marked service down.\n", []),
        riak_core_node_watcher:service_down(riak_search)

        %% TODO: Gracefully unregister riak_kv webmachine endpoints.
        %% Cannot do this currently as it calls application:set_env while this function
        %% is itself inside of application controller.  webmachine really needs it's own
        %% ETS table for dispatch information.
        %%[ webmachine_router:remove_route(R) || R <- some_list_of_routes...],
    catch
        Type:Reason ->
            lager:error("Stopping application riak_search - ~p:~p.\n", [Type, Reason])
    end,
    stopping.

stop(_State) ->
    lager:info("Stopped  application riak_search.\n", []),
    ok.
