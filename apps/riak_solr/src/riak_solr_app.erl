-module(riak_solr_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    webmachine_router:add_route({["solr", index, "select"], riak_search_solr_wm, []}),
    webmachine_router:add_route({["solr", "select"], riak_search_solr_wm, []}),
    riak_solr_sup:start_link().

stop(_State) ->
    ok.
