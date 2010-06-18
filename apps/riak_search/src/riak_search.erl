-module(riak_search).
-export([client_connect/1,
         local_client/0,
         stream/4,
         info/3,
         info_range/5]).
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
    [FirstEntry|_] = riak_core_apl:get_apl(Partition, N),
    Preflist = [FirstEntry],
    riak_search_vnode:stream(Preflist, Index, Field, Term, FilterFun, self()).

info(Index, Field, Term) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
    {ok, Results} = riak_search_backend:collect_info_response(N, Ref, []),
    %% TODO: Replace this with a middleman process that returns after 
    %% the first response.
    {ok, hd(Results)}.

%% info(Index, Field, Term) ->
%%     {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
%%     Preflist = riak_core_apl:get_apl(Partition, N),
%%     {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
%%     {ok, Results} = collect_info(N, Ref, []),
%%     %% TODO: Replace this with a middleman process that returns after 
%%     %% the first response.
%%     {ok, hd(Results)}.

info_range(Index, Field, StartTerm, EndTerm, Size) ->
    {ok, Ref} = riak_search_vnode:info_range(Index, Field, StartTerm, EndTerm, Size, self()),
    {ok, _Results} = collect_info(ringsize(), Ref, []).


collect_info(RepliesRemaining, Ref, Acc) ->
    receive
        {info_response, List, Ref} when RepliesRemaining > 1 ->
            collect_info(RepliesRemaining - 1, Ref, List ++ Acc);
        {info_response, List, Ref} when RepliesRemaining == 1 ->
            io:format("collect_info returning ~p\n", [List++Acc]),
            {ok, List ++ Acc}
%%         Other ->
%%             error_logger:info_msg("Unexpected response: ~p~n", [Other]),
%%             collect_info(RepliesRemaining, Ref, Acc)
    after 5000 ->
        error_logger:error_msg("range_loop timed out!"),
        throw({timeout, range_loop})
    end.

ringsize() ->
    app_helper:get_env(riak_core, ring_creation_size).
