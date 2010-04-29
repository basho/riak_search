-module(riak_solr_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case riak_solr_sup:start_link() of
        {ok, _}=Result ->
            webmachine_router:add_route({["solr", index, "select"],
                                         riak_solr_searcher_wm, []}),
            webmachine_router:add_route({["solr", index],
                                         riak_solr_indexer_wm, []}),
            Result;
        Error ->
            Error
    end.

stop(_State) ->
    ok.
