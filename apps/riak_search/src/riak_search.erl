-module(riak_search).
-export([client_connect/1,
         local_client/0,
         stream/7,
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

stream(Index, Field, Term, SubType, StartSubTerm, EndSubTerm, FilterFun) ->
    %% Construct the operation...
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    {ok, Client} = riak:local_client(),
    Ref = make_ref(),

    %% How many replicas?
    BucketProps = riak_core_bucket:get_bucket(IndexBin),
    NVal = proplists:get_value(n_val, BucketProps),

    %% Figure out which nodes we can stream from.
    Payload1 = {init_stream, self(), Ref},
    Obj1 = riak_object:new(IndexBin, FieldTermBin, Payload1),
    Client:put(Obj1, 0, 0),
    {ok, Partition, Node} = wait_for_ready(NVal, Ref, undefined, undefined),

    %% Run the operation...
    Payload2 = {stream, Index, Field, Term, SubType, StartSubTerm, EndSubTerm, self(), Ref, Partition, Node, FilterFun},
    Obj2 = riak_object:new(IndexBin, FieldTermBin, Payload2),
    Client:put(Obj2, 0, 0),
    {ok, Ref}.

info(Index, Field, Term) ->
    %% Construct the operation...
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    Ref = make_ref(),
    Payload = {info, Index, Field, Term, self(), Ref},

    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    Client:put(Obj, 0, 0),

    %% How many replicas?
    BucketProps = riak_core_bucket:get_bucket(IndexBin),
    NVal = proplists:get_value(n_val, BucketProps),
    {ok, Results} = collect_info(NVal, Ref, []),
    {ok, hd(Results)}.

info_range(Index, Field, StartTerm, EndTerm, Size) ->
    %% Construct the operation...
    Bucket = <<"search_broadcast">>,
    Key = <<"ignored">>,
    Ref = make_ref(),
    Payload = {info_range, Index, Field, StartTerm, EndTerm, Size, self(), Ref},

    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(Bucket, Key, Payload),
    Client:put(Obj, 0, 0),
    {ok, _Results} = collect_info(ringsize(), Ref, []).

collect_info(RepliesRemaining, Ref, Acc) ->
    receive
        {info_response, List, Ref} when RepliesRemaining > 1 ->
            collect_info(RepliesRemaining - 1, Ref, List ++ Acc);
        {info_response, List, Ref} when RepliesRemaining == 1 ->
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

%% Get replies from all nodes that are willing to stream this
%% bucket. If there is one on the local node, then use it, otherwise,
%% use the first one that responds.
wait_for_ready(0, _Ref, Partition, Node) ->
    {ok, Partition, Node};
wait_for_ready(RepliesRemaining, Ref, Partition, Node) ->
    LocalNode = node(),
    receive
        {stream_ready, LocalPartition, LocalNode, Ref} ->
            {ok, LocalPartition, LocalNode};
        {stream_ready, _NewPartition, _NewNode, Ref} when Node /= undefined ->
            wait_for_ready(RepliesRemaining - 1, Ref, Partition, Node);
        {stream_ready, NewPartition, NewNode, Ref} ->
            wait_for_ready(RepliesRemaining -1, Ref, NewPartition, NewNode)
    end.
