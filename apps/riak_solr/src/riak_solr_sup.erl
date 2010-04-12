-module(riak_solr_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    Processes = [{riak_solr_config,
                 {riak_solr_config, start_link, []},
                  permanent, 5000, worker, [riak_solr_config]}],
    {ok, {{one_for_one, 9, 10}, Processes}}.
