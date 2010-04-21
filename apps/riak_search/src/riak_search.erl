-module(riak_search).
-export([
    execute/4,
    explain/4,
    index/7,
    stream/7,
    info/3,
    info_range/5
]).
-include("riak_search.hrl").

execute(OpList, DefaultIndex, DefaultField, Facets) ->
    %% Normalize, Optimize, and Expand Buckets.
    OpList1 = riak_search_preplan:preplan(OpList, DefaultIndex, DefaultField, Facets),

    %% Get the total number of terms and weight in query...
    {NumTerms, NumDocs, QueryNorm} = get_scoring_info(OpList1),

    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    QueryProps = [{num_docs, NumDocs}],
    {ok, NumInputs} = riak_search_op:chain_op(OpList1, self(), Ref, QueryProps),

    %% Gather and return results...
    Results = collect_results(NumInputs, Ref, []),

    %% Score and sort results...
    NewResults = sort_by_score(QueryNorm, NumTerms, Results),
    {ok, NewResults}.

%% Gather results from all connections
collect_results(Connections, Ref, Acc) ->
    receive
	{results, Results, Ref} ->
	    collect_results(Connections, Ref, Acc ++ Results);

	{disconnect, Ref} when Connections > 1  ->
	    collect_results(Connections - 1, Ref, Acc);

	{disconnect, Ref} when Connections == 1 ->
	    Acc;

	Other ->
	    throw({unexpected_message, Other})

    after 60 * 1000 ->
            ?PRINT(timeout),
            throw({timeout, Connections, Acc})
    end.


%% Return {NumTerms, NumDocs, QueryNorm}...
%% http://lucene.apache.org/java/2_4_0/api/org/apache/lucene/search/Similarity.html
get_scoring_info(Op) ->
    %% Get a list of scoring info...
    List = lists:flatten(get_scoring_info_1(Op)),

    %% Calculate NumTerms and NumDocs...
    NumTerms = length(List),
    NumDocs = lists:sum([NodeWeight || {NodeWeight, _} <- List]),

    %% Calculate the QueryNorm...
    F = fun({DocFrequency, Boost}, Acc) ->
        IDF = 1 + math:log((NumDocs + 1) / (DocFrequency + 1)),
        Acc + math:pow(IDF * Boost, 2)
    end,
    SumOfSquaredWeights = lists:foldl(F, 0, List),
    QueryNorm = 1 / math:pow(SumOfSquaredWeights, 0.5),

    %% Return.
    {NumTerms, NumDocs, QueryNorm}.
get_scoring_info_1(Op) when is_record(Op, term) ->
    DocFrequency = hd([X || {node_weight, _, X} <- Op#term.options]),
    Boost = proplists:get_value(boost, Op#term.options, 1),
    [{DocFrequency, Boost}];
get_scoring_info_1(Op) when is_tuple(Op) ->
    get_scoring_info_1(element(2, Op));
get_scoring_info_1(Ops) ->
    [get_scoring_info_1(X) || X <- Ops].


sort_by_score(QueryNorm, NumTerms, Results) ->
    SortedResults = lists:sort(calculate_scores(QueryNorm, NumTerms, Results)),
    [{Value, Props} || {_, Value, Props} <- SortedResults].

calculate_scores(QueryNorm, NumTerms, [{Value, Props}|Results]) ->
    ScoreList = proplists:get_value(score, Props),
    Coord = length(ScoreList) / NumTerms,
    Score = Coord * QueryNorm * lists:sum(ScoreList),
    NewProps = lists:keystore(score, 1, Props, {score, Score}),
    [{-1 * Score, Value, NewProps}|calculate_scores(QueryNorm, NumTerms, Results)];
calculate_scores(_, _, []) ->
    [].



explain(OpList, DefaultIndex, DefaultField, Facets) ->
    riak_search_preplan:preplan(OpList, DefaultIndex, DefaultField, Facets).

index(Index, Field, Term, SubType, SubTerm, Value, Props) ->
    %% Construct the operation...
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    Payload = {index, Index, Field, Term, SubType, SubTerm, Value, Props},

    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    Client:put(Obj, 0, 0).

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
    %% TODO - Handle inclusive.
    %% TODO - Handle wildcards.
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
            {ok, List ++ Acc};
        Other ->
            error_logger:info_msg("Unexpected response: ~p~n", [Other]),
            collect_info(RepliesRemaining, Ref, Acc)
    after 1000 ->
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
