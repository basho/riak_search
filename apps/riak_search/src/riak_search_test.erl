%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_test).

-include("riak_search.hrl").

-export([index/2, search/1]).

index(Id, Content) ->
    D = #riak_idx_doc{id=Id, index="search", fields=[{"content", Content}]},
    {ok, C} = riak_search:local_client(),
    C:index_doc(D).

search(Query) ->
    {ok, C} = riak_search:local_client(),
    C:search("search", "content", Query).
