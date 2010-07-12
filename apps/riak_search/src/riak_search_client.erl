%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_client, [RiakClient]).

-include("riak_search.hrl").

-define(MAX_MULTI_TERM_SZ, 250).
-define(OPTIMIZER_PROC_CT, 32).

-export([
    %% Searching...
    parse_query/2,
    search/5,
    search_doc/5,

    %% Stream Searching...
    stream_search/2,
    collect_result/2,

    %% Explain...
    explain/2,

    %% Indexing...
    index_terms/1, index_terms/2,
    get_index_fsm/0,
    stop_index_fsm/1,
    index_doc/2, index_doc/3,
    index_term/5,

    %% Delete
    delete_doc/2,
    delete_term/4
]).

-import(riak_search_utils, [
    from_binary/1,
    to_binary/1
]).


%% Parse the provided query. Returns either {ok, QueryOps} or {error,
%% Error}.
parse_query(IndexOrSchema, Query) ->
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    try
        qilr_parse:string(AnalyzerPid, Query, Schema)
    after
        qilr:close_analyzer(AnalyzerPid)
    end.

%% Run the Query, return the list of keys.
%% Timeout is in milliseconds.
%% Return the {Length, Results}.
search(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout) ->
    %% Execute the search and collect the results.
    SearchRef = stream_search(IndexOrSchema, QueryOps),
    Results = collect_results(SearchRef, Timeout, []),

    %% Dedup, and handle start and max results. Return matching
    %% documents.
    Results1 = truncate_list(QueryStart, QueryRows, Results),
    Length = length(Results),
    {Length, Results1}.

search_doc(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout) ->
    %% Get results...
    {Length, Results} = search(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout),
    MaxScore = case Results of
                   [] ->
                       "0.0";
                   [{_, Attrs}|_] ->
                       [MS] = io_lib:format("~g", [proplists:get_value(score, Attrs)]),
                       MS
               end,
    %% Fetch the documents in parallel.
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    Index = Schema:name(),
    F = fun({DocID, _}) ->
        riak_indexed_doc:get(RiakClient, Index, DocID)
    end,
    Documents = plists:map(F, Results, {processes, 4}),
    {Length, MaxScore, [X || X <- Documents, X /= {error, notfound}]}.

%% Run the query through preplanning, return the result.
explain(IndexOrSchema, QueryOps) ->
    %% Run the query through preplanning.
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    riak_search_preplan:preplan(QueryOps, Schema).

%% Create an index FSM, send the terms and shut it down
index_terms(Terms) ->
    {ok, Pid} = get_index_fsm(),
    ok = index_terms(Pid, Terms),
    stop_index_fsm(Pid).

%% Index terms against an index FSM
index_terms(Pid, Terms) ->
    riak_search_index_fsm:index_terms(Pid, Terms).

%% Get an index FSM
get_index_fsm() ->
    riak_search_index_fsm_sup:start_child().

%% Tell an index FSM indexing is complete and wait for it to shut down
stop_index_fsm(Pid) ->
    riak_search_index_fsm:done(Pid).

%% Index a specified #riak_idx_doc
index_doc(IdxDoc, AnalyzerPid) ->
    IndexPid = get_index_fsm(),
    try
        index_doc(IdxDoc, AnalyzerPid, IndexPid)
    after
        stop_index_fsm(IndexPid)
    end.
    
%% Index a specified #riak_idx_doc
index_doc(IdxDoc, AnalyzerPid, IndexPid) ->
    {ok, Postings} = riak_indexed_doc:analyze(IdxDoc, AnalyzerPid),
    index_terms(IndexPid, Postings),
    riak_indexed_doc:put(RiakClient, IdxDoc).

%% Index the specified term - better to use to plural 'terms' interfaces
index_term(Index, Field, Term, Value, Props) ->
    index_terms([{Index, Field, Term, Value, Props}]).

delete_term(Index, Field, Term, DocId) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    riak_search_vnode:delete_term(Preflist, Index, Field, Term, DocId).

truncate_list(QueryStart, QueryRows, List) ->
    %% Remove the first QueryStart results...
    case QueryStart =< length(List) of
        true  -> {_, List1} = lists:split(QueryStart, List);
        false -> List1 = []
    end,

    %% Only keep QueryRows results...
    case QueryRows /= infinity andalso QueryRows =< length(List1) of
        true  -> {List2, _} = lists:split(QueryRows, List1);
        false -> List2 = List1
    end,

    %% Return.
    List2.

stream_search(IndexOrSchema, OpList) ->
    %% Run the query through preplanning.
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    OpList1 = riak_search_preplan:preplan(OpList, Schema),

    %% Get the total number of terms and weight in query...
    {NumTerms, NumDocs, QueryNorm} = get_scoring_info(OpList1),
    
    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    QueryProps = [
        {num_docs, NumDocs},
        {index_name, Schema:name()},
        {default_field, Schema:default_field()}
    ],
    
    %% Start the query process ...
    {ok, NumInputs} = riak_search_op:chain_op(OpList1, self(), Ref, QueryProps),
    #riak_search_ref {
        id=Ref, termcount=NumTerms,
        inputcount=NumInputs, querynorm=QueryNorm }.

%% Gather all results from the provided SearchRef, return the list of
%% results sorted in descending order by score.
collect_results(SearchRef, Timeout, Acc) ->
    M = collect_result(SearchRef, Timeout),
    case M of
        {done, _} ->
            sort_by_score(SearchRef, Acc);
        {[], Ref} ->
            collect_results(Ref, Timeout, Acc);
        {Results, Ref} ->
            collect_results(Ref, Timeout, Acc ++ Results);
        Error ->
            Error
    end.

%% Collect one or more individual results in as non-blocking of a
%% fashion as possible. Returns {Results, SearchRef}, {done,
%% SearchRef}, or {error, timeout}.
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

%% Return {NumTerms, NumDocs, QueryNorm}...
%% http://lucene.apache.org/java/2_4_0/api/org/apache/lucene/search/Similarity.html
get_scoring_info(Op) ->
    %% Get a list of scoring info...
    List = lists:flatten(get_scoring_info_1(Op)),
    case List /= [] of
        true ->
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
            {NumTerms, NumDocs, QueryNorm};
        false ->
            {0, 0, 0}
    end.
get_scoring_info_1(Op) when is_record(Op, term) ->
    Weights = [X || {node_weight, _, X} <- Op#term.options],
    DocFrequency = hd(Weights ++ [0]),
    Boost = proplists:get_value(boost, Op#term.options, 1),
    [{DocFrequency, Boost}];
get_scoring_info_1(Op) when is_record(Op, mockterm) ->
    [];
get_scoring_info_1(#phrase{base_query=BaseQuery}) ->
    get_scoring_info_1(BaseQuery);
get_scoring_info_1(Op) when is_tuple(Op) ->
    get_scoring_info_1(element(2, Op));
get_scoring_info_1(Ops) when is_list(Ops) ->
    [get_scoring_info_1(X) || X <- Ops].

sort_by_score(#riak_search_ref{querynorm=QNorm, termcount=TermCount}, Results) ->
    SortedResults = lists:sort(calculate_scores(QNorm, TermCount, Results)),
    [{Value, Props} || {_, Value, Props} <- SortedResults].

calculate_scores(QueryNorm, NumTerms, [{Value, Props}|Results]) ->
    ScoreList = proplists:get_value(score, Props),
    Coord = length(ScoreList) / (NumTerms + 1),
    Score = Coord * QueryNorm * lists:sum(ScoreList),
    NewProps = lists:keystore(score, 1, Props, {score, Score}),
    [{-1 * Score, Value, NewProps}|calculate_scores(QueryNorm, NumTerms, Results)];
calculate_scores(_, _, []) ->
    [].

delete_doc(Index, DocId) ->
    case riak_indexed_doc:get(RiakClient, Index, DocId) of
        {error, notfound} ->
            {error, notfound};
        IdxDoc ->
            %% Get the postings...
            {ok, Postings} = riak_indexed_doc:analyze(IdxDoc),

            %% Delete the postings...
            F = fun(X) ->
                {Index, Field, Term, Value, _Props} = X,
                delete_term(Index, Field, Term, Value)
            end,
            plists:map(F, Postings, {processes, 4}),

            %% Delete the indexed doc.
            riak_indexed_doc:delete(RiakClient, Index, DocId)
    end.
