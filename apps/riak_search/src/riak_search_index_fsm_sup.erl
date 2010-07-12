%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_index_fsm_sup).
-behaviour(supervisor).
-export([start_link/0, start_child/0, init/1, stop/1]).

start_child() ->
    supervisor:start_child(?MODULE, []).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_S) -> ok.

%% @private
init([]) ->
    {ok,
     {{simple_one_for_one, 10, 10},
      [{undefined,
        {riak_search_index_fsm, start_link, []},
        temporary, 2000, worker, [riak_search_index_fsm]}]}}.
