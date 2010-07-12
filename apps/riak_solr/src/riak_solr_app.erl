%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, local_client/0]).

start(_StartType, _StartArgs) ->
    Root = app_helper:get_env(riak_solr, solr_name, "solr"),
    case riak_solr_sup:start_link() of
        {ok, _}=Result ->
            webmachine_router:add_route({[Root, index, "update"],
                                         riak_solr_indexer_wm, []}),
            webmachine_router:add_route({[Root, "update"],
                                         riak_solr_indexer_wm, []}),
            webmachine_router:add_route({[Root, index, "select"],
                                         riak_solr_searcher_wm, []}),
            webmachine_router:add_route({[Root, "select"],
                                         riak_solr_searcher_wm, []}),
            Result;
        Error ->
            Error
    end.

stop(_State) ->
    ok.

local_client() ->
    {ok, RiakClient} = riak:local_client(),
    {ok, SearchClient} = riak_search:local_client(),
    {ok, riak_solr_search_client:new(RiakClient, SearchClient)}.
