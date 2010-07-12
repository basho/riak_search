%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(qilr).

-include("qilr.hrl").

-export([new_analyzer/0, close_analyzer/1]).

new_analyzer() ->
    riak_sock_pool:checkout(?CONN_POOL).

close_analyzer(Analyzer) ->
    riak_sock_pool:checkin(?CONN_POOL, Analyzer).
