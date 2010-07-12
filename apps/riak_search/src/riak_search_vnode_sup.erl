%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_vnode_sup).
-behaviour(supervisor).
-export([start_link/0, init/1, stop/1]).
-export([start_vnode/2]).

start_vnode(Mod, Index) when is_integer(Index) -> 
    supervisor:start_child(?MODULE, [Mod, Index]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_S) -> ok.

%% @private
init([]) ->
    {ok, 
     {{simple_one_for_one, 10, 10}, 
      [{undefined,
        {riak_core_vnode, start_link, []},
      temporary, brutal_kill, worker, dynamic}]}}.
