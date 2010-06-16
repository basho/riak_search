-module(riak_search_client, [RiakClient]).

-include("riak_search.hrl").

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
    query_as_graph/1,

    %% Indexing...
    parse_idx_doc/2,
    get_idx_doc/2,
    store_idx_doc/1,
    index_term/5,

    %% Delete
    delete_document/2
]).

-import(riak_search_utils, [
    from_binary/1,
    to_binary/1
]).


%% Parse the provided query. Returns either {ok, QueryOps} or {error,
%% Error}.
parse_query(IndexOrSchema, Query) ->
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    Result = qilr_parse:string(AnalyzerPid, Query, list_to_atom(Schema:default_op()),
                               Schema:analyzer_factory()),
    qilr_analyzer:close(AnalyzerPid),
    Result.

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

    %% Fetch the documents in parallel.
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    Index = Schema:name(),
    F = fun({DocID, _}) ->
        get_idx_doc(Index, DocID)
    end,
    Documents = plists:map(F, Results, {processes, 4}),
    {Length, Documents}.

%% Run the query through preplanning, return the result.
explain(IndexOrSchema, QueryOps) ->
    %% Get schema information...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    DefaultIndex = Schema:name(),
    DefaultField = Schema:default_field(),
    Facets = [{DefaultIndex, Schema:field_name(X)} || X <- Schema:facets()],

    %% Run the query through preplanning.
    riak_search_preplan:preplan(QueryOps, DefaultIndex, DefaultField, Facets).

%% Parse a #riak_idx_doc{} record using the provided analyzer pid.
parse_idx_doc(AnalyzerPid, IdxDoc) when is_record(IdxDoc, riak_idx_doc) ->
    %% Extract fields, get schema...
    #riak_idx_doc{id=DocID, index=Index, fields=DocFields}=IdxDoc,
    {ok, Schema} = riak_search_config:get_schema(Index),


    %% Put together a list of Facet properties...
    F1 = fun(Facet, Acc) ->
        FName = Schema:field_name(Facet),
        case lists:keyfind(FName, 1, DocFields) of
            {FName, Value} ->
                [{FName, Value}|Acc];
            false ->
                Acc
        end
    end,
    FacetProps = lists:foldl(F1, [], Schema:facets()),

    %% For each Field = {FieldName, FieldValue}, split the FieldValue
    %% into terms. Build a list of positions for those terms, then get
    %% a de-duped list of the terms. For each, index the FieldName /
    %% Term / DocID / Props.
    F2 = fun({FieldName, FieldValue}, Acc2) ->
        {ok, Terms} = qilr_analyzer:analyze(AnalyzerPid, FieldValue, Schema:analyzer_factory()),
        PositionTree = get_term_positions(Terms),
        Terms1 = gb_trees:keys(PositionTree),
        F3 = fun(Term, Acc3) ->
            Props = build_props(Term, PositionTree),
            [{Index, FieldName, Term, DocID, Props ++ FacetProps}|Acc3]
        end,
        lists:foldl(F3, Acc2, Terms1)
    end,
    DocFields1 = DocFields -- FacetProps,
    lists:foldl(F2, [], DocFields1).

get_idx_doc(DocIndex, DocID) ->
    DocBucket = to_binary(from_binary(DocIndex) ++ "_docs"),
    case RiakClient:get(DocBucket, to_binary(DocID), 2) of
        {error, notfound} ->
            {error, notfound};
        {ok, Obj} ->
            riak_object:get_value(Obj)
    end.

store_idx_doc(IdxDoc) ->
    %% Store the document...
    #riak_idx_doc { id=DocID, index=DocIndex } = IdxDoc,
    DocBucket = riak_search_utils:to_binary(DocIndex ++ "_docs"),
    DocKey = riak_search_utils:to_binary(DocID),
    DocObj = riak_object:new(DocBucket, DocKey, IdxDoc),
    ok = RiakClient:put(DocObj, 2).


%% @private Given a list of tokens, build a gb_tree mapping words to a
%% list of word positions.
get_term_positions(Terms) ->
    F = fun(Term, {Pos, Acc}) ->
        case gb_trees:lookup(Term, Acc) of
            {value, Positions} ->
                {Pos + 1, gb_trees:update(Term, [Pos|Positions], Acc)};
            none ->
                {Pos + 1, gb_trees:insert(Term, [Pos], Acc)}
        end
    end,
    {_, Tree} = lists:foldl(F, {1, gb_trees:empty()}, Terms),
    Tree.

%% @private
%% Given a term and a list of positions, generate a list of
%% properties.
build_props(Term, PositionTree) ->
    case gb_trees:lookup(Term, PositionTree) of
        none ->
            [];
        {value, Positions} ->
            [
                {word_pos, Positions},
                {freq, length(Positions)}
            ]
    end.



%% Index the specified term.
index_term(Index, Field, Term, Value, Props) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    riak_search_vnode:index(Partition, N, Index, Field, Term, Value, Props).

delete_term(Index, Field, Term, DocId) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    riak_search_vnode:delete(Partition, N, Index, Field, Term, DocId).

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
    %% Get schema information...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    DefaultIndex = Schema:name(),
    DefaultField = Schema:default_field(),
    Facets = [{DefaultIndex, Schema:field_name(X)} || X <- Schema:facets()],

    %% Normalize, Optimize, and Expand Buckets.
    OpList1 = riak_search_preplan:preplan(OpList, DefaultIndex, DefaultField, Facets),

    %% Get the total number of terms and weight in query...
    {NumTerms, NumDocs, QueryNorm} = get_scoring_info(OpList1),

    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    QueryProps = [{num_docs, NumDocs},
                  {default_field, DefaultField},
                  {index_name, IndexOrSchema}],

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
get_scoring_info_1(#phrase{base_query=BaseQuery}) ->
    get_scoring_info_1(BaseQuery);
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

delete_document(Index, DocId) ->
    case get_idx_doc(Index, DocId) of
        {error, notfound} ->
            {error, notfound};
        IdxDoc ->
            #riak_idx_doc{id=DocID, index=Index, fields=DocFields}=IdxDoc,
            {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
            F2 = fun({FieldName, FieldValue}, Acc2) ->
                {ok, Terms} = qilr_analyzer:analyze(AnalyzerPid, FieldValue),
                PositionTree = get_term_positions(Terms),
                Terms1 = gb_trees:keys(PositionTree),
                F3 = fun(Term, Acc3) ->
                    Props = build_props(Term, PositionTree),
                    [{Index, FieldName, Term, DocID, Props}|Acc3]
                end,
                lists:foldl(F3, Acc2, Terms1)
            end,
            DocFields1 = DocFields,
            FieldTerms = lists:foldl(F2, [], DocFields1),
            io:format("FieldTerms = ~p~n", [FieldTerms]),
            lists:foreach(fun({_Index, Field, Term, _DocId, _Props}) ->
                delete_term(Index, Field, Term, DocId)
            end, FieldTerms),
            DocBucket = riak_search_utils:to_binary(Index ++ "_docs"),
            DocKey = riak_search_utils:to_binary(DocId),
            RiakClient:delete(DocBucket, DocKey, 1)
    end.

query_as_graph(OpList) ->
    G = digraph:new(),
    digraph:add_vertex(G, root, "root"),
    digraph:add_vertex(G, nodes, "nodes"),
    digraph:add_vertex(G, or_ops, "or_ops"),
    digraph:add_vertex(G, and_ops, "and_ops"),
    digraph:add_vertex(G, not_ops, "not_ops"),
    digraph:add_edge(G, root, nodes, "has-property"),
    digraph:add_edge(G, root, or_ops, "has-property"),
    digraph:add_edge(G, root, and_ops, "has-property"),
    digraph:add_edge(G, root, not_ops, "has-property"),
    query_as_graph(OpList, root, 0, G),
    dump_graph(G),
    G.

query_as_graph(OpList, Parent, C0, G) ->
    case is_list(OpList) of
        true ->
            lists:foldl(fun(Op, C) ->
                case Op of
                    [L] ->
                        %%io:format("[L] / ~p~n", [L]),
                        query_as_graph(L, Parent, C, G),
                        C+1;
                    {node, {lor, N}, _Node} ->
                        %%io:format("[~p] lor: ~p~n", [Node, N]),
                        V = {lor, C},
                        digraph:add_vertex(G, V, "or"),
                        digraph:add_edge(G, Parent, V, "has-or"),
                        digraph:add_edge(G, or_ops, V, "has-member"),
                        query_as_graph(N, V, C+1, G)+1;
                    {node, {land, N}, _Node} ->
                        %%io:format("[~p] land: ~p~n", [Node, N]),
                        V = {land, C},
                        digraph:add_vertex(G, V, "and"),
                        digraph:add_edge(G, Parent, V, "has-and"),
                        digraph:add_edge(G, and_ops, V, "has-member"),
                        query_as_graph(N, V, C+1, G)+1;
                    {lnot, N} ->
                        %%io:format("lnot: ~p~n", [N]),
                        V = {lnot, C},
                        digraph:add_vertex(G, V, "not"),
                        digraph:add_edge(G, Parent, V, "has-not"),
                        digraph:add_edge(G, not_ops, V, "has-member"),
                        query_as_graph(N, V, C+1, G)+1;
                    {term, IFT, Props} ->
                        %%io:format("term, IFT = ~p, Props = ~p~n",
                        %%    [IFT, Props]),
                        V = {term, IFT},
                        digraph:add_vertex(G, V, "term"),
                        digraph:add_edge(G, Parent, V, "has-term"),
                        query_as_graph(Props, V, C+1, G)+1;
                    {facets, _} -> %% ignore facets for now
                        C;
                    {node_weight, N, _NodeCount} ->
                        %%io:format("~p: ~p (~p)~n", [Parent, N, NodeCount]),
                        V = {node, N},
                        case lists:keysearch(N, 2, digraph:vertices(G)) of
                            false ->
                                digraph:add_vertex(G, V, "node"),
                                digraph:add_edge(G, nodes, V, "has-member");
                            _ -> skip
                        end,
                        digraph:add_edge(G, Parent, V, "has-location"),
                        C+1;
                    X ->
                        io:format("query_as_graph: Unknown node type: ~p~n", [X])
                end end, C0, OpList);
        false ->
            query_as_graph([OpList], Parent, C0, G)
    end.

dump_graph(G) ->
    dump_graph(G, root).

dump_graph(G, StartNode) ->
    dump_graph(G, StartNode, 1).

dump_graph(G, Parent, Tabs) ->
    lists:map(fun(Node) ->
        tab_n(Tabs),
        case length(digraph:out_neighbours(G, Node)) > 0 of
            true ->
                io:format("(~p) ~p~n", [Parent, Node]);
            false ->
                io:format("~p~n", [Node])
        end,
        case is_atom(Parent) andalso Parent /= root of
            true -> skip;
            _ -> dump_graph(G, Node, Tabs+2)
        end
    end, digraph:out_neighbours(G, Parent)),
    ok.

tab_n(Tabs) ->
    lists:map(fun(_) -> io:format("  ") end, lists:seq(0,Tabs)).
