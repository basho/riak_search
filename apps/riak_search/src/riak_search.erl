%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search).
-export([
    client_connect/1,
    local_client/0,
    stream/4,
%%     multi_stream/3,
    info/3,
%%     term_preflist/3,
    info_range/5
%%     info_range_no_count/4,
%%     catalog_query/1,
%%     term/2
]).
-include("riak_search.hrl").

-define(TIMEOUT, 30000).

client_connect(Node) when is_atom(Node) ->
    {ok, Client} = riak:client_connect(Node),
    {ok, riak_search_client:new(Client)}.

local_client() ->
    {ok, Client} = riak:local_client(),
    {ok, riak_search_client:new(Client)}.


stream(Index, Field, Term, FilterFun) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    %% Calculate the preflist with full N but then only ask the first
    %% node in it.  Preflists are ordered with primaries first followed
    %% by fallbacks, so this will prefer a primary node over a fallback.
    [FirstEntry|_] = riak_core_apl:get_apl(Partition, N, riak_search),
    Preflist = [FirstEntry],
    riak_search_vnode:stream(Preflist, Index, Field, Term, FilterFun, self()).

info(Index, Field, Term) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N, riak_search),
    {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
    {ok, _Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, []).

info_range(Index, Field, StartTerm, EndTerm, Size) ->
    %% TODO: Duplicating current behavior for now - a PUT against a preflist with
    %%       the N val set to the size of the ring - this will mean no failbacks
    %%       will be available.  Instead should work out the preflist for
    %%       each partition index and find which node is responsible for that partition
    %%       and talk to that.
    Preflist = riak_core_apl:active_owners(riak_search),
    {ok, Ref} = riak_search_vnode:info_range(Preflist, Index, Field, StartTerm, EndTerm, 
                                             Size, self()),
    {ok, _Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, []).
