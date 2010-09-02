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
    %% MapReduce Searching...
    mapred/5, mapred_stream/6,

    %% Searching...
    parse_query/2,
    search/5,
    search_fold/5,
    search_doc/5,

    %% Explain...
    explain/2,

    %% Indexing...
    index_doc/2,
    index_term/5,
    index_terms/1, 

    %% Delete
    delete_doc/1,
    delete_doc_terms/1,
    delete_term/4,
    delete_terms/1
]).

-import(riak_search_utils, [
    from_binary/1,
    to_binary/1
]).

mapred(DefaultIndex, SearchQuery, MRQuery, ResultTransformer, Timeout) ->
    {ok, ReqID} = mapred_stream(DefaultIndex, SearchQuery, MRQuery, self(), ResultTransformer, Timeout),
    luke_flow:collect_output(ReqID, Timeout).
        
mapred_stream(DefaultIndex, SearchQuery, MRQuery, ClientPid, ResultTransformer, Timeout) ->
    InputDef = {modfun, riak_search, mapred_search, [DefaultIndex, SearchQuery]},
    {ok, {RId, FSM}} = RiakClient:mapred_stream(MRQuery, ClientPid, ResultTransformer, Timeout),
    RiakClient:mapred_dynamic_inputs_stream(FSM, InputDef, Timeout),
    luke_flow:finish_inputs(FSM),
    {ok, RId}.


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
    %% Execute the search, collect the results,.
    SearchRef1 = stream_search(IndexOrSchema, QueryOps),
    F = fun(Results, Acc) -> Acc ++ Results end,
    {ok, SearchRef2, Results} = fold_results(SearchRef1, Timeout, F, []),
    SortedResults = sort_by_score(SearchRef2, Results),

    %% Dedup, and handle start and max results. Return matching
    %% documents.
    Results1 = truncate_list(QueryStart, QueryRows, SortedResults),
    Length = length(SortedResults),
    {Length, Results1}.

%% Run the search query, fold results through function, return final
%% accumulator.
search_fold(IndexOrSchema, QueryOps, Fun, AccIn, Timeout) ->
    %% Execute the search, collect the results,.
    SearchRef = stream_search(IndexOrSchema, QueryOps),
    {ok, _NewSearchRef, AccOut} = fold_results(SearchRef, Timeout, Fun, AccIn),
    AccOut.

search_doc(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout) ->
    %% Get results...
    {Length, Results} = search(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout),
    MaxScore = case Results of
                   [] ->
                       "0.0";
                   [{_, _, Props}|_] ->
                       [MS] = io_lib:format("~g", [proplists:get_value(score, Props)]),
                       MS
               end,
    %% Fetch the documents in parallel.
    F = fun({Index, DocID, _}) ->
        riak_indexed_doc:get(RiakClient, Index, DocID)
    end,
    Documents = plists:map(F, Results, {processes, 4}),
    {Length, MaxScore, [X || X <- Documents, X /= {error, notfound}]}.

%% Run the query through preplanning, return the result.
explain(IndexOrSchema, QueryOps) ->
    %% Run the query through preplanning.
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    riak_search_preplan:preplan(QueryOps, Schema).

%% Index a specified #riak_idx_doc
index_doc(IdxDoc, AnalyzerPid) ->
    {ok, IdxDoc2} = riak_indexed_doc:analyze(IdxDoc, AnalyzerPid),
    Postings = riak_indexed_doc:postings(IdxDoc2),
    index_terms(Postings),
    riak_indexed_doc:put(RiakClient, IdxDoc2).

%% Index the specified term - better to use the plural 'terms' interfaces
index_term(Index, Field, Term, DocID, Props) ->
    index_terms([{Index, Field, Term, DocID, Props}]).

%% Given a list of terms of the form {I,F,T,V,P,K}, pull them off in
%% chunks, batch them according to preflist, and then index them in
%% parallel.
index_terms(Terms) ->
    BatchSize = app_helper:get_env(riak_search, index_batch_size, 10000),
    PartitionFun = riak_search_utils:partition_fun(),
    index_terms_1(Terms, BatchSize, PartitionFun, gb_trees:empty()).
index_terms_1([], _, _, _) ->
    ok;
index_terms_1(Terms, BatchSize, PartitionFun, PreflistCache) ->
    %% Split off the next chunk of terms.
    case length(Terms) > BatchSize of
        true -> 
            {Batch, Rest} = lists:split(BatchSize, Terms);
        false -> 
            {Batch, Rest} = {Terms, []}
    end,

    %% Create a table for grouping terms.
    Table = ets:new(batch, [protected, duplicate_bag]),
    try
        %% SubBatch the postings by partition...
        F1 = fun(Posting = {I,F,T,_V,_P,_K}) ->
                     Partition = PartitionFun(I,F,T),
                     ets:insert(Table, {Partition, Posting})
             end,
        [F1(X) || X <- Batch],

        %% Get the list of partitions we have. For each partition,
        %% look up the preflist unless it's already in cache.
        F2 = fun(Partition, Cache) ->
                     case gb_trees:lookup(Partition, Cache) of
                         {value, _} -> 
                             PreflistCache;
                         none -> 
                             %% Create a sample DocIdx in the section
                             %% of the ring where we want a partition.
                             DocIdx = <<(Partition - 1):160/integer>>,
                             NVal = riak_search_utils:n_val(),
                             Preflist = riak_core_apl:get_apl(DocIdx, NVal, riak_search),
                             gb_trees:insert(Partition, Preflist, Cache)
                     end
             end,
        Partitions = riak_search_utils:ets_keys(Table),
        NewPreflistCache = lists:foldl(F2, PreflistCache, Partitions),

        %% Store the batches.
        F3 = fun(Partition) ->
                     %% Read SubBatch from ets, strip off the Partition
                     %% number, and then index the batch.  TODO: Catch the
                     %% output of the delete statement, make sure it was
                     %% successful, if not then try again.
                     {value, Preflist} = gb_trees:lookup(Partition, NewPreflistCache),
                     SubBatch = [Posting || {_, Posting} <- ets:lookup(Table, Partition)],
                     riak_search_vnode:index(Preflist, SubBatch)
             end,
        plists:map(F3, Partitions, {processes, 4}),

        %% Repeat until there are no more terms...
        index_terms_1(Rest, BatchSize, PartitionFun, NewPreflistCache)
    after
        ets:delete(Table)
    end.
        

delete_doc(IdxDoc) ->
    delete_doc_terms(IdxDoc),
    riak_indexed_doc:delete(RiakClient, IdxDoc).

%% Delete all of the indexed terms in the IdxDoc - does not remove the IdxDoc itself
delete_doc_terms(IdxDoc) ->
    %% Build a list of terms to delete and send them over to the delete FSM
    Postings = riak_indexed_doc:postings(IdxDoc),
    delete_terms(Postings).

%% Delete the specified term - better to use the plural 'terms' interfaces.
delete_term(Index, Field, Term, DocID) ->
    K =  riak_search_utils:current_key_clock(),
    delete_terms([{Index, Field, Term, DocID, K}]).

%% Given a list of terms of the form {I,F,T,V,P,K}, pull them off in
%% chunks, batch them according to preflist, and then delete them in
%% parallel.
delete_terms(Terms) ->
    BatchSize = app_helper:get_env(riak_search, index_batch_size, 10000),
    PartitionFun = riak_search_utils:partition_fun(),
    delete_terms_1(Terms, BatchSize, PartitionFun, gb_trees:empty()).
delete_terms_1([], _, _, _) ->
    ok;
delete_terms_1(Terms, BatchSize, PartitionFun, PreflistCache) ->
    %% Split off the next chunk of terms.
    case length(Terms) > BatchSize of
        true -> 
            {Batch, Rest} = lists:split(BatchSize, Terms);
        false -> 
            {Batch, Rest} = {Terms, []}
    end,

    %% Create a table for grouping terms.
    Table = ets:new(batch, [protected, duplicate_bag]),
    try
        %% SubBatch the postings by partition...
        F1 = fun
                 ({I,F,T,V,_P,K}) ->    
                     Partition = PartitionFun(I,F,T),
                     ets:insert(Table, {Partition, {I,F,T,V,K}});
                 ({I,F,T,V,K}) ->
                     Partition = PartitionFun(I,F,T),
                     ets:insert(Table, {Partition, {I,F,T,V,K}})
             end,
        [F1(X) || X <- Batch],

        %% Get the list of partitions we have. For each partition,
        %% look up the preflist unless it's already in cache.
        F2 = fun(Partition, Cache) ->
                     case gb_trees:lookup(Partition, Cache) of
                         {value, _} -> 
                             PreflistCache;
                         none -> 
                             %% Create a sample DocIdx in the section
                             %% of the ring where we want a partition.
                             DocIdx = <<(Partition - 1):160/integer>>,
                             NVal = riak_search_utils:n_val(),
                             Preflist = riak_core_apl:get_apl(DocIdx, NVal, riak_search),
                             gb_trees:insert(Partition, Preflist, Cache)
                     end
             end,
        Partitions = riak_search_utils:ets_keys(Table),
        NewPreflistCache = lists:foldl(F2, PreflistCache, Partitions),

        %% Store the batches.
        F3 = fun(Partition) ->
                     %% Read SubBatch from ets, strip off the Partition
                     %% number, and then delete the batch.  TODO: Catch the
                     %% output of the delete statement, make sure it was
                     %% successful, if not then try again.
                     {value, Preflist} = gb_trees:lookup(Partition, NewPreflistCache),
                     SubBatch = [Posting || {_, Posting} <- ets:lookup(Table, Partition)],
                     riak_search_vnode:delete(Preflist, SubBatch)
             end,
        plists:map(F3, Partitions, {processes, 4}),

        %% Repeat until there are no more terms...
        delete_terms_1(Rest, BatchSize, PartitionFun, NewPreflistCache)
    after
        ets:delete(Table)
    end.


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

%% Receive results from the provided SearchRef, run through the
%% provided Fun starting with Acc.
%% Fun([Results], Acc) -> NewAcc.
fold_results(SearchRef, Timeout, Fun, Acc) ->
    case collect_result(SearchRef, Timeout) of
        {done, Ref} ->
            {ok, Ref, Acc};
        {error, timeout} ->
            {error, timeout};
        {Results, Ref} ->
            NewAcc = Fun(Results, Acc),
            fold_results(Ref, Timeout, Fun, NewAcc)
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
get_scoring_info_1(#phrase{props=Props}) ->
    BaseQuery = proplists:get_value(base_query, Props),
    get_scoring_info_1(BaseQuery);
get_scoring_info_1(Op) when is_record(Op, range) ->
    [];
get_scoring_info_1(Op) when is_tuple(Op) ->
    get_scoring_info_1(element(2, Op));
get_scoring_info_1(Ops) when is_list(Ops) ->
    [get_scoring_info_1(X) || X <- Ops].

sort_by_score(#riak_search_ref{querynorm=QNorm, termcount=TermCount}, Results) ->
    SortedResults = lists:sort(calculate_scores(QNorm, TermCount, Results)),
    [{Index, DocID, Props} || {_, Index, DocID, Props} <- SortedResults].

calculate_scores(QueryNorm, NumTerms, [{Index, DocID, Props}|Results]) ->
    ScoreList = proplists:get_value(score, Props),
    Coord = length(ScoreList) / (NumTerms + 1),
    Score = Coord * QueryNorm * lists:sum(ScoreList),
    NewProps = lists:keystore(score, 1, Props, {score, Score}),
    [{-1 * Score, Index, DocID, NewProps}|calculate_scores(QueryNorm, NumTerms, Results)];
calculate_scores(_, _, []) ->
    [].
