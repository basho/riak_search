-module(riak_search).
-export([client_connect/1,
         local_client/0,
         stream/4,
         multi_stream/2,
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
    {_N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    riak_search_vnode:stream(Partition, 1, Index, Field, Term, FilterFun, self()).

multi_stream(IFTList, FilterFun) ->
    riak_search_vnode:multi_stream(IFTList, FilterFun, self()).

info(Index, Field, Term) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    {ok, Ref} = riak_search_vnode:info(Partition, N, Index, Field, Term, self()),
    {ok, Results} = collect_info(N, Ref, []),
    {ok, hd(Results)}.

info_range(Index, Field, StartTerm, EndTerm, Size) ->
    {ok, Ref} = riak_search_vnode:info_range(Index, Field, StartTerm, EndTerm, Size, self()),
    {ok, _Results} = collect_info(ringsize(), Ref, []).

collect_info(RepliesRemaining, Ref, Acc) ->
    receive
        {info_response, List, Ref} when RepliesRemaining > 1 ->
            collect_info(RepliesRemaining - 1, Ref, List ++ Acc);
        {info_response, List, Ref} when RepliesRemaining == 1 ->
            {ok, List ++ Acc}
    after 5000 ->
        throw({timeout, collect_info})
    end.

ringsize() ->
    app_helper:get_env(riak_core, ring_creation_size).
