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
    mapred_search/5, mapred_search_stream/6,

    %% Searching...
    parse_query/2,
    search/5,
    search_fold/5,
    search_doc/5,

    %% Explain...
    explain/2,

    %% Indexing...
    index_terms/1, index_terms/2,
    get_index_fsm/0,
    stop_index_fsm/1,
    index_doc/2, index_doc/3,
    index_term/5,

    %% Delete
    delete_terms/1, delete_terms/2,
    get_delete_fsm/0,
    stop_delete_fsm/1,
    delete_doc_terms/1, delete_doc_terms/2,
    delete_doc/1, delete_doc/2,
    delete_term/4
]).

-import(riak_search_utils, [
    from_binary/1,
    to_binary/1
]).

mapred_search(Index, SearchString, Query, ResultTransformer, Timeout) ->
    Me = self(),
    {ok,MR_ReqId} = mapred_search_stream(Index, SearchString, Query, Me, ResultTransformer, Timeout),
    Results = luke_flow:collect_output(MR_ReqId, Timeout),
    Results.

mapred_search_stream(Index, SearchString, Query, ClientPid, ResultTransformer, Timeout) ->
    %% NOTE: Not proud of this code, as it creates a circular dependency on the 
    %% Parse the query...
    case parse_query(Index, SearchString) of
        {ok, Ops} ->
            QueryOps = Ops;
        {error, ParseError} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [SearchString, ParseError]),
            throw({search_stream, SearchString, ParseError}),
            QueryOps = undefined % Make compiler happy.
    end,

    %% Set up the mapreduce job...
    case RiakClient:mapred_stream(Query, ClientPid, ResultTransformer, Timeout) of
        {ok, {ReqId, FlowPid}} ->
            %% Perform a search, funnel results to the mapred job...
            F = fun(Results, Acc) ->
                %% Make the list of BKeys...
                BKeys = [{Index, DocID} || {DocID, _Props} <- Results],
                luke_flow:add_inputs(FlowPid, BKeys),
                Acc
            end,
            ok = search_fold(Index, QueryOps, F, ok, Timeout),
            luke_flow:finish_inputs(FlowPid),
            {ok, ReqId};
        Error ->
            Error
    end.


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
        ?PRINT({Index, DocID}),
        riak_indexed_doc:get(RiakClient, Index, DocID)
    end,
    Documents = plists:map(F, Results, {processes, 4}),
    {Length, MaxScore, [X || X <- Documents, X /= {error, notfound}]}.

%% Run the query through preplanning, return the result.
explain(IndexOrSchema, QueryOps) ->
    %% Run the query through preplanning.
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    riak_search_preplan:preplan(QueryOps, Schema).

%% Index the specified term - better to use the plural 'terms' interfaces
index_term(Index, Field, Term, DocID, Props) ->
    index_terms([{Index, Field, Term, DocID, Props}]).

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
    {ok, IndexPid} = get_index_fsm(),
    try
        index_doc(IdxDoc, AnalyzerPid, IndexPid)
    after
        stop_index_fsm(IndexPid)
    end.
    
%% Index a specified #riak_idx_doc
index_doc(IdxDoc, AnalyzerPid, IndexPid) ->
    {ok, IdxDoc2} = riak_indexed_doc:analyze(IdxDoc, AnalyzerPid),
    Postings = riak_indexed_doc:postings(IdxDoc2),
    index_terms(IndexPid, Postings),
    riak_indexed_doc:put(RiakClient, IdxDoc2).

%% Delete the specified term - better to use the plural 'terms' interfaces.
delete_term(Index, Field, Term, DocID) ->
    delete_terms([{Index, Field, Term, DocID}]).

%% Create a delete FSM, send the terms and shut it down.
delete_terms(Terms) ->
    {ok, Pid} = get_delete_fsm(),
    ok = delete_terms(Pid, Terms),
    stop_delete_fsm(Pid).
    
%% Delete terms against a delete FSM.
delete_terms(Pid, Terms) ->
    riak_search_delete_fsm:delete_terms(Pid, Terms).

%% Get a delete FSM
get_delete_fsm() ->
    riak_search_delete_fsm_sup:start_child().

%% Tell a delete FSM deleting is complete and wait for it to shut
%% down.
stop_delete_fsm(Pid) ->
    riak_search_delete_fsm:done(Pid).

%% Delete all of the indexed terms in the IdxDoc - does not remove the IdxDoc itself
delete_doc_terms(IdxDoc, DeletePid) ->
    %% Build a list of terms to delete and send them over to the delete FSM
    I = riak_indexed_doc:index(IdxDoc),
    V = riak_indexed_doc:id(IdxDoc),
    Fun = fun(F, T, _Pos, Acc) ->
                  [{I,F,T,V} | Acc]
          end,
    Terms = riak_indexed_doc:fold_terms(Fun, [], IdxDoc),
    delete_terms(DeletePid, Terms).

%% See delete_doc_terms/2
delete_doc_terms(IdxDoc) ->
    {ok, DeletePid} = get_delete_fsm(),
    try
        delete_doc_terms(IdxDoc, DeletePid)
    after
        stop_delete_fsm(DeletePid)
    end.    

%% Delete a specified #riak_idx_doc.
delete_doc(IdxDoc) ->
    {ok, DeletePid} = get_delete_fsm(),
    try
        delete_doc(IdxDoc, DeletePid)
    after
        stop_delete_fsm(DeletePid)
    end.

delete_doc(IdxDoc, DeletePid) ->
    delete_doc_terms(IdxDoc, DeletePid),
    riak_indexed_doc:delete(RiakClient, riak_indexed_doc:index(IdxDoc), 
                            riak_indexed_doc:id(IdxDoc)).

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
