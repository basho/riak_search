-module(riak_search_client, [RiakClient]).

-include("riak_search.hrl").

-define(DEFAULT_RESULT_SIZE, 10).

%% Indexing
-export([index_doc/1, index_store/2, index_store/3]).

%% Querying
-export([explain/3, explain/4, stream_search/3, search/3, search/4, doc_search/3,
         doc_search/5, collect_result/2, get_document/2]).

search(Index, DefaultField, Query) ->
    search(Index, DefaultField, Query, 60000).

search(Index, DefaultField, Query, Timeout) ->
    SearchRef = stream_search(Index, DefaultField, Query),
    Results = collect_results(SearchRef, Timeout, []),
    [Key || {Key, _Props} <- Results].

stream_search(Index, DefaultField, Query) ->
    {ok, Qilr} = qilr_parse:string(Query),
    execute(Qilr, Index, DefaultField, []).

doc_search(Index, DefaultField, Query) ->
    doc_search(Index, DefaultField, Query, ?DEFAULT_RESULT_SIZE, 60000).

doc_search(Index, DefaultField, Query, ResultSize, Timeout) ->
    case search(Index, DefaultField, Query, Timeout) of
        [] ->
            [];
        Results0 ->
            Results = dedup(truncate_list(ResultSize, Results0), []),
            [get_document(Index, DocId) || DocId <- Results]
    end.

explain(Index, DefaultField, Query) ->
    explain(Index, DefaultField, [], Query).

explain(Index, DefaultField, Facets, Query) ->
    {ok, Ops} = qilr_parse:string(Query),
    riak_search_preplan:preplan(Ops, Index, DefaultField, Facets).

index_doc(#riak_idx_doc{id=DocId, index=Index, fields=Fields}=Doc) ->
    [index_term(Index, Name, Value, DocId, []) || {Name, Value} <- analyze_fields(Fields, [])],
    DocBucket = Index ++ "_docs",
    DocObj = riak_object:new(riak_search_utils:to_binary(DocBucket),
                             riak_search_utils:to_binary(DocId),
                             Doc),
    RiakClient:put(DocObj, 1).

index_store(Doc, Obj) ->
    index_store(Doc, Obj, 1).

index_store(#riak_idx_doc{id=DocId}=Doc, Obj0, W) ->
    Md = dict:store("X-Riak-Search-Id", DocId, dict:new()),
    Obj = riak_object:update_metadata(Obj0, Md),
    ok = index_doc(Doc),
    RiakClient:put(Obj, W).

%% Internal functions
index_term(Index, Field, Term, Value, Props) ->
    index_internal(Index, Field, Term, {index, Index, Field, Term, Value, Props}).

index_internal(Index, Field, Term, Payload) ->
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
%% Run the operation...
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    RiakClient:put(Obj, 0).

analyze_fields([], Accum) ->
    Accum;
analyze_fields([{Name, Value}|T], Accum) ->
    {ok, Tokens} = basho_analyzer:analyze(Value),
    analyze_fields(T, [{Name, binary_to_list(Token)} || Token <- Tokens] ++ Accum).

truncate_list(Size, List) when Size >= length(List) ->
    List;
truncate_list(Size, List) ->
    [L1, _] = lists:split(Size, List),
    L1.

get_document(Index, DocId) ->
    DocBucket = riak_search_utils:from_binary(Index) ++ "_docs",
    {ok, Obj} = RiakClient:get(riak_search_utils:to_binary(DocBucket),
                               riak_search_utils:to_binary(DocId), 2),
    riak_object:get_value(Obj).

dedup([], Accum) ->
    lists:reverse(Accum);
dedup([H|T], Accum) ->
    NewAccum = case lists:member(H, Accum) of
                   false ->
                       [H|Accum];
                   true ->
                       Accum
               end,
    dedup(T, NewAccum).

execute(OpList, DefaultIndex, DefaultField, Facets) ->
%% Normalize, Optimize, and Expand Buckets.
    OpList1 = riak_search_preplan:preplan(OpList, DefaultIndex, DefaultField, Facets),

%% Get the total number of terms and weight in query...
    {NumTerms, NumDocs, QueryNorm} = get_scoring_info(OpList1),

%% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    QueryProps = [{num_docs, NumDocs}],
    {ok, NumInputs} = riak_search_op:chain_op(OpList1, self(), Ref, QueryProps),
    #riak_search_ref{id=Ref, termcount=NumTerms, inputcount=NumInputs,
                     querynorm=QueryNorm}.

collect_result(#riak_search_ref{inputcount=0}=SearchRef, _Timeout) ->
    {done, SearchRef};
collect_result(#riak_search_ref{id=Id, inputcount=InputCount}=SearchRef, Timeout) ->
    receive
        {results, Results, Id} ->
            {Results, SearchRef};
        {disconnect, Id} ->
            {[], SearchRef#riak_search_ref{inputcount=InputCount - 1}}
        after Timeout ->
             {error, timeout}
    end.

%% Gather results from all connections
collect_results(SearchRef, Timeout, Acc) ->
    case collect_result(SearchRef, Timeout) of
        {done, _} ->
            sort_by_score(SearchRef, Acc);
        {[], Ref} ->
            collect_results(Ref, Timeout, Acc);
        {Results, Ref} ->
            collect_results(Ref, Timeout, Acc ++ Results);
        Error ->
            Error
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


sort_by_score(#riak_search_ref{querynorm=QNorm, termcount=TermCount}, Results) ->
    SortedResults = lists:sort(calculate_scores(QNorm, TermCount, Results)),
    [{Value, Props} || {_, Value, Props} <- SortedResults].

calculate_scores(QueryNorm, NumTerms, [{Value, Props}|Results]) ->
    ScoreList = proplists:get_value(score, Props),
    Coord = length(ScoreList) / NumTerms,
    Score = Coord * QueryNorm * lists:sum(ScoreList),
    NewProps = lists:keystore(score, 1, Props, {score, Score}),
    [{-1 * Score, Value, NewProps}|calculate_scores(QueryNorm, NumTerms, Results)];
calculate_scores(_, _, []) ->
    [].
