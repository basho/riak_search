-module(riak_search_client, [RiakClient]).

-include("riak_search.hrl").

-define(DEFAULT_RESULT_SIZE, 10).
-define(DEFAULT_TIMEOUT, 60000).

-export([
    %% Searching...
    search/2, search/3,

    %% Doc Searching...
    search_doc/2,
    search_doc/5,

    %% Stream Searching...
    stream_search/2, 
    collect_result/2,

    %% Explain...
    explain/2,
    query_as_graph/1,

    %% Indexing...
    index_doc/2,
    index_doc/3,
    index_idx_doc/1, 
    index_idx_doc/2,
   
    %% Other
    optimize_query/1,
    optimize_terms/2
]).


%% Run the Query, return the list of keys.
search(IndexOrSchema, Query) ->
    search(IndexOrSchema, Query, ?DEFAULT_TIMEOUT).


%% Run the Query, return the list of keys.
%% Timeout is in milliseconds. 
%% Return the list of keys.
search(IndexOrSchema, Query, Timeout) ->
    SearchRef = stream_search(IndexOrSchema, Query),
    Results = collect_results(SearchRef, Timeout, []),
    [Key || {Key, _Props} <- Results].


%% Run the Query, results are fetchable using collect_result/2.
%% Returns SearchRef.
stream_search(IndexOrSchema, Query) ->
    %% Get the schema...
    Schema = to_schema(IndexOrSchema),

    %% Parse the query...
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    ParseResult = qilr_parse:string(AnalyzerPid, Query),
    qilr_analyzer:close(AnalyzerPid),

    %% Either execute or throw an error.
    case ParseResult of
        {ok, AST} ->
            execute(AST, Schema);
        Error ->
            throw(Error)
    end.


%% Search for Index, return {Length, [Docs]}.
search_doc(IndexOrSchema, Query) ->
    search_doc(IndexOrSchema, Query, 0, ?DEFAULT_RESULT_SIZE, ?DEFAULT_TIMEOUT).


%% Search for Index, return {Length, [Docs]}.
%% Limit by QueryStart and QueryRows, Timeout is in milliseconds.
search_doc(IndexOrSchema, Query, QueryStart, QueryRows, Timeout) ->
    %% Get the Schema and Index...
    Schema = to_schema(IndexOrSchema),
    Index = Schema:name(),
    
    %% Run the search...
    Results = search(Schema, Query, Timeout),
    Length = length(Results),

    %% Dedup, and handle start and max results. Return matching
    %% documents.
    Results1 = lists:usort(Results),
    Results2 = truncate_list(QueryStart, QueryRows, Results1),
    {Length, [get_document(Index, DocId) || DocId <- Results2]}.


get_document(Index, DocId) ->
    DocBucket = riak_search_utils:from_binary(Index) ++ "_docs",
    {ok, Obj} = RiakClient:get(riak_search_utils:to_binary(DocBucket),
                               riak_search_utils:to_binary(DocId), 2),
    riak_object:get_value(Obj).


%% Parse the Query, run it through preplanning, return the execution
%% plan.
explain(IndexOrSchema, Query) ->
    %% Get schema information...
    Schema = to_schema(IndexOrSchema),
    DefaultIndex = Schema:name(),
    DefaultField = Schema:default_field(),
    Facets = [{DefaultIndex, Schema:field_name(X)} || X <- Schema:facets()],

    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    ParseResult = qilr_parse:string(AnalyzerPid, Query),
    ?PRINT(ParseResult),
    qilr_analyzer:close(AnalyzerPid),
    case ParseResult of 
        {ok, Ops} ->
            ?PRINT(Ops),
            ?PRINT(DefaultIndex),
            ?PRINT(DefaultField),
            riak_search_preplan:preplan(Ops, DefaultIndex, DefaultField, Facets);
        Error ->
            throw(Error)
    end.


%% Index a solr XML formatted file.
index_doc(IndexOrSchema, Body) when is_binary(Body) ->
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    index_doc(AnalyzerPid, IndexOrSchema, Body),
    qilr_analyzer:close(AnalyzerPid),
    ok.

%% Index a solr XML formatted file using the provided analyzer pid.
index_doc(AnalyzerPid, IndexOrSchema, Body) ->
    %% Get the schema...
    Schema = to_schema(IndexOrSchema),
    Index = Schema:name(),
    
    %% Parse the xml...
    Commands = riak_solr_xml_xform:xform(Body),
    ok = Schema:validate_commands(Commands),

    %% Index the docs...
    F = fun(DictDoc) ->
        IdxDoc = to_riak_idx_doc(Index, DictDoc),
        index_idx_doc(AnalyzerPid, IdxDoc)
    end,
    {docs, Docs} = lists:keyfind(docs, 1, Commands),
    [F(X) || X <- Docs],
    ok.

%% @private
to_riak_idx_doc(Index, Doc0) ->
    Id = dict:fetch("id", Doc0),
    Doc = dict:erase(Id, Doc0),
    Fields = dict:to_list(dict:erase(Id, Doc)),
    #riak_idx_doc{id=Id, index=Index, fields=Fields, props=[]}.

%% Index a #riak_idx_doc{} record.
index_idx_doc(IdxDoc) when is_record(IdxDoc, riak_idx_doc) ->
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    index_idx_doc(AnalyzerPid, IdxDoc),
    qilr_analyzer:close(AnalyzerPid),
    ok.

%% Index a #riak_idx_doc{} record using the provided analyzer pid.
index_idx_doc(AnalyzerPid, IdxDoc) when is_record(IdxDoc, riak_idx_doc) ->
    %% Extract fields, get schema...
    #riak_idx_doc{id=DocID, index=Index, fields=DocFields}=IdxDoc,
    Schema = to_schema(Index),
    
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
    ?PRINT(FacetProps),

    %% For each Field = {FieldName, FieldValue}, split the FieldValue
    %% into terms. Build a list of positions for those terms, then get
    %% a de-duped list of the terms. For each, index the FieldName /
    %% Term / DocID / Props.
    F2 = fun({FieldName, FieldValue}) ->
        {ok, Terms} = qilr_analyzer:analyze(AnalyzerPid, FieldValue),
        PositionTree = get_term_positions(Terms),
        Terms1 = gb_trees:keys(PositionTree),
        [begin
            Props = build_props(Term, PositionTree),
            index_term(Index, FieldName, Term, DocID, Props ++ FacetProps)
        end || Term <- Terms1]
    end,
    DocFields1 = DocFields -- FacetProps,
    [F2(X) || X <- DocFields1],
    
    %% Now, write the document itself.
    DocBucket = Index ++ "_docs",
    DocObj = riak_object:new(riak_search_utils:to_binary(DocBucket),
                             riak_search_utils:to_binary(DocID),
                             IdxDoc),
    RiakClient:put(DocObj, 1).

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


%% Internal functions
index_term(Index, Field, Term, Value, Props) ->
    Payload = {index, Index, Field, Term, Value, Props},
    index_internal(Index, Field, Term, Payload).

index_internal(Index, Field, Term, Payload) ->
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    %% Run the operation...
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    RiakClient:put(Obj, 0).

truncate_list(QueryStart, QueryRows, List) ->
    %% Remove the first QueryStart results...
    case QueryStart =< length(List) of
        true  -> {_, List1} = lists:split(QueryStart, List);
        false -> List1 = []
    end,

    %% Only keep QueryRows results...
    case QueryRows =< length(List1) of
        true  -> {List2, _} = lists:split(QueryRows, List1);
        false -> List2 = List1
    end,

    %% Return.
    List2.


execute(OpList, IndexOrSchema) ->
    %% Get schema information...
    Schema = to_schema(IndexOrSchema),
    DefaultIndex = Schema:name(),
    DefaultField = Schema:default_field(),
    Facets = [{DefaultIndex, Schema:field_name(X)} || X <- Schema:facets()],
    
    %% Normalize, Optimize, and Expand Buckets.
    OpList1 = riak_search_preplan:preplan(OpList, DefaultIndex, DefaultField, Facets),

    %% Get the total number of terms and weight in query...
    {NumTerms, NumDocs, QueryNorm} = get_scoring_info(OpList1),

    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    QueryProps = [{num_docs, NumDocs}],

    %% Start the query process ... 
    {ok, NumInputs} = riak_search_op:chain_op(OpList1, self(), Ref, QueryProps),
    #riak_search_ref { 
        id=Ref, termcount=NumTerms, 
        inputcount=NumInputs, querynorm=QueryNorm }.

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

to_schema(IndexOrSchema) ->
    case is_tuple(IndexOrSchema) andalso element(1, IndexOrSchema) == riak_solr_schema of
        true  -> 
            IndexOrSchema;
        false -> 
            {ok, Schema} = riak_solr_config:get_schema(IndexOrSchema),
            Schema
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


%%%%%%%

optimize_query(OpList) ->
    G = query_as_graph(OpList),
    
    %% optimize_junction: AND, OR optimization
    JunctionOps = 
        digraph:out_neighbours(G, or_ops) ++
        digraph:out_neighbours(G, and_ops),
    io:format("JunctionOps = ~p~n", [JunctionOps]),

    lists:foreach(fun(JunctionNode) ->
        optimize_junction(G, JunctionNode)
    end, JunctionOps),
    
    OpList2 = graph_as_query(G, root, []),
    io:format("~n----original query OpList: ~n"),
    io:format("OpList  = ~p~n", [OpList]),
    io:format("~n----optimized query OpList: ~n"),
    io:format("OpList2 = ~p~n", [OpList2]),
    io:format("~n----~n"),
    [OpList2].

graph_as_query(G, Node, Acc) ->
    Out0 = digraph:out_neighbours(G, Node),
    case Node of
        and_ops -> Out = [];
        or_ops -> Out = [];
        not_ops -> Out = [];
        nodes -> Out = [];
        root -> Out = Out0;
        _ -> 
            case is_atom(Node) of
                true -> io:format("graph_as_query: unknown atom: ~p~n", [Node]),
                        Out = [];
                _ -> Out = Out0
            end
    end,
    
    case length(Out) of
        0 ->
            case Node of
                {multi_term, _NTerms, _TNode} ->
                    io:format("multi_term: ~p: ~p~n", [Node, Out]),
                    Node;
                and_ops -> [];
                or_ops -> [];
                not_ops -> [];
                nodes -> [];
                _ ->
                    io:format("0/Node = ~p~n", [Node]),
                    {unknown_node_type, Node}
            end;
        _ ->
            io:format("~p: n/Out = ~p~n", [Node, Out]),
            
            case Node of
                {lnot, _N} ->
                    io:format("lor: ~p: ~p~n", [Node, Out]),
                    Terms = lists:reverse(lists:map(fun(OutNode) ->
                        graph_as_query(G, OutNode, Acc)
                    end, Out)),
                    {lnot, Terms};
                {land, _N} ->
                    io:format("lor: ~p: ~p~n", [Node, Out]),
                    Terms = lists:reverse(lists:map(fun(OutNode) ->
                        graph_as_query(G, OutNode, Acc)
                    end, Out)),
                    {node, {land, Terms}, node()}; %% todo: real node?
                {lor, _N} ->
                    io:format("lor: ~p: ~p~n", [Node, Out]),
                    Terms = lists:reverse(lists:map(fun(OutNode) ->
                        io:format("lor: outnode = ~p~n", [OutNode]),
                        graph_as_query(G, OutNode, Acc)
                    end, Out)),
                    {node, {lor, Terms}, node()}; %% todo: real node?
                {term, {I, F, T}} ->
                    % todo: fix counts (until then, everyone has a 1 count)
                    % todo: fix faceting
                    NodeWeights = lists:reverse(lists:usort(lists:map(fun({node, N1}) ->
                        {node_weight, N1, 1}
                    end, Out))),
                    R = {term, {I, F, T}, [{facets, []}] ++ NodeWeights},
                    io:format("term: R = ~p~n", [R]),
                    R;
                root ->
                    io:format("root: ~p: ~p~n", [Node, Out]),
                    lists:foldl(fun(N, RAcc) ->
                        case graph_as_query(G, N, Acc) of
                            [] -> RAcc;
                            V -> RAcc ++ V
                        end
                    end, [], Out);
                _ ->
                    io:format("X? ~p: ~p~n", [Node, Out]),
                    {unknown_node_type, Node}
            end
    end.

optimize_junction(G, OrNode) ->
    io:format("optimize_junction(G, ~p)~n", [OrNode]),
    Terms0 = digraph:out_neighbours(G, OrNode),
    Terms = lists:filter(fun(E) ->
        case E of
            [] -> false;
            _ -> true
        end
    end, lists:map(fun(T0) ->
            case T0 of
                {term, _} ->
                    T0;
                _ -> []
            end
        end, Terms0)),
    io:format("Terms = ~p~n", [Terms]),
    L = lists:foldl(fun(T, Acc) ->
        io:format("optimize_junction: OrNode = ~p: T = ~p~n", [OrNode, T]),
        io:format("out_neighbours(G, T) = ~p~n", [digraph:out_neighbours(G, T)]),
        lists:map(fun(Node_N) ->
            case Node_N of
                {node, N} -> 
                    case proplists:is_defined(N, Acc) of
                        true ->
                            {N, TL} = proplists:lookup(N, Acc),
                            proplists:delete(N, Acc) ++ {N, lists:flatten(TL ++ [T])};
                        false -> Acc ++ {N, [T]}
                    end;
                _ -> Acc
            end
        end, digraph:out_neighbours(G, T))
    end, [], Terms),
    TCD = lists:sort(fun(A,B) ->
        {_Na, La} = A,
        {_Nb, Lb} = B,
        length(La) > length(Lb)
    end, L),
    io:format("TCD = ~p~n", [TCD]),
    lists:foreach(fun(N_NTerms) ->
        io:format("N_NTerms = ~p~n", [N_NTerms]),
        {Node, NodeTerms} = N_NTerms,
        RemTerms = lists:foldl(fun(RTerm, Acc) ->
            io:format("get_path(~p, ~p) = ~p~n", [OrNode, RTerm, digraph:get_path(G, OrNode, RTerm)]),
            case digraph:get_path(G, OrNode, RTerm) of
                false -> Acc;
                _ -> Acc ++ [RTerm]
            end
        end, [], NodeTerms),
        io:format("RemTerms = ~p~n", [RemTerms]),
        case RemTerms of
            [] -> skip;
            _ ->
                lists:foreach(fun(Nt) ->
                    digraph:del_path(G, OrNode, Nt),
                    digraph:del_vertex(G, Nt)
                end, RemTerms),
                Vtx = {multi_term, RemTerms, Node},
                digraph:add_vertex(G, Vtx),
                digraph:add_edge(G, OrNode, Vtx)
        end
    end, TCD),
    TCD.

%
% optimize_terms(Graph, RootNode)
%
% {lor, N} -> [ (T->N), ... ] 
% to:
% {lor, N} -> [ MG({T0, T1, ...}, N), ... ]
%
optimize_terms(G, RootNode) ->
    Terms = digraph:out_neighbours(G, RootNode),
    io:format("terms: ~p~n", [Terms]),
    G.

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
