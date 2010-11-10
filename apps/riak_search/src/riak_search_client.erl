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

    %% Indexing...
    index_doc/2,
    index_docs/1, index_docs/2,
    index_term/5,
    index_terms/1, 

    %% Delete
    delete_docs/1,
    delete_doc_terms/1,
    delete_term/4,
    delete_terms/1
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
    DefaultIndex = Schema:name(),
    DefaultField = Schema:default_field(),
    lucene_parser:parse(
      riak_search_utils:to_list(DefaultIndex),
      riak_search_utils:to_list(DefaultField),
      riak_search_utils:to_list(Query)).
    
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

%% Index a specified #riak_idx_doc
index_doc(IdxDoc, AnalyzerPid) ->
    index_docs([IdxDoc], AnalyzerPid).

index_docs(IdxDocs) ->
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    try
        index_docs(IdxDocs, AnalyzerPid)
    after
        qilr:close_analyzer(AnalyzerPid)
    end,
    ok.

index_docs(IdxDocs, AnalyzerPid) ->
    %% For each doc, update the object stored in Riak KV, and generate
    %% a list of postings to add and delete.
    F = fun(IdxDoc0, {RiakObjsAccIn, DeleteAccIn, IndexAccIn}) ->
                %% Analyze the doc...
                IdxDoc = riak_indexed_doc:analyze(IdxDoc0, AnalyzerPid),

                %% Get the terms to delete...
                Index = riak_indexed_doc:index(IdxDoc), 
                DocId = riak_indexed_doc:id(IdxDoc),
                case riak_indexed_doc:get_obj(RiakClient, Index, DocId) of
                    {ok, OldRiakObj} ->
                        OldIdxDoc = riak_object:get_value(OldRiakObj),
                        DeleteTerms = riak_indexed_doc:postings(OldIdxDoc);
                    {error, notfound} ->
                        OldRiakObj = riak_indexed_doc:new_obj(Index, DocId),
                        DeleteTerms = []
                end,
                NewDeleteAcc = [DeleteTerms|DeleteAccIn],

                %% Get the terms to index...
                IndexTerms = riak_indexed_doc:postings(IdxDoc),
                NewIndexAcc = [IndexTerms|IndexAccIn],
                
                %% Store the document...
                RiakObj = riak_object:update_value(OldRiakObj, IdxDoc),
                NewRiakObjsAcc = [RiakObj|RiakObjsAccIn],
                
                %% Return.
                {NewRiakObjsAcc, NewDeleteAcc, NewIndexAcc}
        end,
    {RiakObjsAcc, DeleteAcc, IndexAcc} = lists:foldl(F, {[],[],[]}, IdxDocs),

    %% Create the Riak objects...
    [riak_indexed_doc:put_obj(RiakClient, X) || X <- RiakObjsAcc],

    %% Delete the old postings...
    FlatDeleteAcc = lists:flatten(DeleteAcc),
    delete_terms(FlatDeleteAcc),

    %% Add the new postings...
    FlatIndexAcc = lists:flatten(IndexAcc),
    index_terms(FlatIndexAcc),
    ok.
    

%% Index the specified term - better to use the plural 'terms' interfaces
index_term(Index, Field, Term, DocID, Props) ->
    index_terms([{Index, Field, Term, DocID, Props}]).

index_terms(Terms) ->
    IndexFun = fun(VNode, Postings) -> 
                       riak_search_vnode:index(VNode, Postings)
               end,
    process_terms(IndexFun, Terms).

%% Given a list of terms of the form {I,F,T,V,P,K}, pull them off in
%% chunks, batch them according to preflist, and then index them in
%% parallel.
process_terms(IndexFun, Terms) ->
    BatchSize = app_helper:get_env(riak_search, index_batch_size, 1000),
    process_terms_1(IndexFun, BatchSize, gb_trees:empty(), Terms).
process_terms_1(_, _, _, []) ->
    ok;
process_terms_1(IndexFun, BatchSize, PreflistCache, Terms) ->
    %% Split off the next chunk of terms.
    case length(Terms) > BatchSize of
        true -> 
            {Batch, Rest} = lists:split(BatchSize, Terms);
        false -> 
            {Batch, Rest} = {Terms, []}
    end,

    %% Create a table for grouping terms.
    SubBatchTable = ets:new(batch, [protected, duplicate_bag]),
    ReplicaTable = ets:new(batch, [protected, duplicate_bag]),
    try
        %% SubBatch the postings by {Partition, Index}. Our
        %% terminology overlaps, so to clarify what we mean here:
        %% Partition is the number between 0 and 2^160-1 identifying
        %% the place on the ring. Index is the actual search index,
        %% such as <<"products">>. We do this because later we'll need
        %% to look up the n_val for the Index.
        Batch1 = riak_search_ring_utils:zip_with_partition_and_index(Batch),
        true = ets:insert(SubBatchTable, Batch1),

        %% Ensure the PreflistCache contains all needed entries...
        F1 = fun(PIKey, Cache) ->
                     case gb_trees:lookup(PIKey, Cache) of
                         {value, _} -> 
                             PreflistCache;
                         none -> 
                             %% Create a sample DocIdx in the section
                             %% of the ring where we want a partition.
                             {Partition, Index} = PIKey,
                             DocIdx = <<(Partition - 1):160/integer>>,

                             %% Get the n_val...
                             {ok, Schema} = riak_search_config:get_schema(Index),
                             NVal = Schema:n_val(),

                             %% Calculate the preflist...
                             Preflist = riak_core_apl:get_apl(DocIdx, NVal, riak_search),
                             gb_trees:insert(PIKey, Preflist, Cache)
                     end
             end,
        PartitionIndexes = riak_search_utils:ets_keys(SubBatchTable),
        NewPreflistCache = lists:foldl(F1, PreflistCache, PartitionIndexes),

        %% For each SubBatch, look up the preflists that should
        %% contain those postings, and group them together.
        F2 = fun(PIKey) ->
                     %% Get the preflist.
                     {value, Preflist} = gb_trees:lookup(PIKey, NewPreflistCache),

                     %% Get the SubBatch, add replica for each entry in the preflist.
                     Replicas = [{VNode, Posting} || {_, Posting} <- ets:lookup(SubBatchTable, PIKey), VNode <- Preflist],
                     ets:insert(ReplicaTable, Replicas)
             end,
        [F2(X) || X <- PartitionIndexes],

        %% Index the postings.
        F3 = fun(VNode) ->
                     Postings = [Posting || {_, Posting} <- ets:lookup(ReplicaTable, VNode)],
                     IndexFun(VNode, Postings)
             end,
        VNodes = riak_search_utils:ets_keys(ReplicaTable),
        plists:map(F3, VNodes, {processes, 4}),

        %% Repeat until there are no more terms...
        process_terms_1(IndexFun, BatchSize, NewPreflistCache, Rest)
    after
        ets:delete(SubBatchTable),
        ets:delete(ReplicaTable)
    end.


%% Docs is a list of the form [{DocIndex, DocID}].
delete_docs(Docs) ->
    F = fun({DocIndex, DocID}, {ObjsAccIn, DeleteAccIn}) ->
                case riak_indexed_doc:get(RiakClient, DocIndex, DocID) of
                    IdxDoc when is_record(IdxDoc, riak_idx_doc) ->
                        DeleteTerms = riak_indexed_doc:postings(IdxDoc),
                        NewObjsAcc = [{DocIndex, DocID}|ObjsAccIn],
                        NewDeleteAcc = [DeleteTerms|DeleteAccIn],
                        {NewObjsAcc, NewDeleteAcc};
                    {error, notfound} ->
                        {ObjsAccIn, DeleteAccIn}
                end
        end,
    {ExistingObjs, DeleteAcc} = lists:foldl(F, {[],[]}, Docs),
    
    %% Delete the postings...
    FlatDeleteAcc = lists:flatten(DeleteAcc),
    delete_terms(FlatDeleteAcc),
    
    %% Delete the Riak objects...
    [RiakClient:delete(DocIndex, DocID) || {DocIndex, DocID} <- ExistingObjs],
    ok.


%% Delete all of the indexed terms in the IdxDoc - does not remove the IdxDoc itself
delete_doc_terms(IdxDoc) ->
    %% Build a list of terms to delete and send them over to the delete FSM
    Postings = riak_indexed_doc:postings(IdxDoc),
    delete_terms(Postings).

%% Delete the specified term - better to use the plural 'terms' interfaces.
delete_term(Index, Field, Term, DocID) ->
    K =  riak_search_utils:current_key_clock(),
    delete_terms([{Index, Field, Term, DocID, K}]).

delete_terms(Terms) ->
    DeleteFun = fun(VNode, Postings) ->
                        riak_search_vnode:delete(VNode, Postings)
                end,
    process_terms(DeleteFun, Terms).

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
    State = #search_state {},
    Props = riak_search_op:preplan(OpList, State),

    %% Get the schema...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),

    %% Get the total number of terms and weight in query...
    {NumTerms, NumDocs, QueryNorm} = get_scoring_info(Props),
    SearchState = #search_state {
      index=Schema:name(),
      field=Schema:default_field(),
      num_terms=NumTerms,
      num_docs=NumDocs,
      query_norm=QueryNorm,
      props=Props
     },

    %% Start the query process...
    Ref = make_ref(),
    {ok, NumInputs} = riak_search_op:chain_op(OpList, self(), Ref, SearchState),
    #riak_search_ref {
        id=Ref, termcount=NumTerms,
        inputcount=NumInputs, querynorm=QueryNorm }.
    
    
    %% %% Get the total number of terms and weight in query...
    %% {NumTerms, NumDocs, QueryNorm} = get_scoring_info(OpList1),
    
    %% %% Set up the operators. They automatically start when created...
    %% Ref = make_ref(),
    %% QueryProps = [
    %%     {num_docs, NumDocs},
    %%     {index_name, Schema:name()},
    %%     {default_field, Schema:default_field()}
    %% ],
    
    %% %% Start the query process ...
    %% {ok, NumInputs} = riak_search_op:chain_op(OpList1, self(), Ref, QueryProps),
    %% #riak_search_ref {
    %%     id=Ref, termcount=NumTerms,
    %%     inputcount=NumInputs, querynorm=QueryNorm }.

    

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
get_scoring_info(Props) ->
    ScoringProps = riak_search_op:props_by_tag(scoring, Props),

    %% Calculate num terms...
    NumTerms = length(ScoringProps),
    NumDocs = lists:sum([0] ++ [DocFrequency || {DocFrequency, _} <- ScoringProps]),

    %% Calculate the QueryNorm...
    F = fun({DocFrequency, Boost}, Acc) ->
                IDF = 1 + math:log((NumDocs + 1) / (DocFrequency + 1)),
                Acc + math:pow(IDF * Boost, 2)
        end,
    SumOfSquaredWeights = lists:foldl(F, 0, ScoringProps),
    QueryNorm = 1 / math:pow(SumOfSquaredWeights + 1, 0.5),
    {NumTerms, NumDocs, QueryNorm}.

sort_by_score(#riak_search_ref{querynorm=QNorm, termcount=TermCount}, Results) ->
    SortedResults = lists:sort(calculate_scores(QNorm, TermCount, Results)),
    [{Index, DocID, Props} || {_, Index, DocID, Props} <- SortedResults].

calculate_scores(QueryNorm, NumTerms, [{Index, DocID, Props}|Results]) ->
    ScoreList = proplists:get_value(score, Props, []),
    Coord = length(ScoreList) / (NumTerms + 1),
    Score = Coord * QueryNorm * lists:sum(ScoreList),
    NewProps = lists:keystore(score, 1, Props, {score, Score}),
    [{-1 * Score, Index, DocID, NewProps}|calculate_scores(QueryNorm, NumTerms, Results)];
calculate_scores(_, _, []) ->
    [].
