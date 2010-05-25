-module(riak_search_client, [RiakClient]).

-include("riak_search.hrl").

-define(DEFAULT_RESULT_SIZE, 10).

%% Indexing
-export([index_doc/1, index_doc/2]).

%% Querying
-export([explain/3, explain/4, stream_search/3, stream_search/4, search/3,
         search/4, search/5, doc_search/3, doc_search/5, collect_result/2,
         get_document/2, query_as_graph/1]).

search(Index, DefaultField, Query) ->
    search(Index, DefaultField, Query, 60000).

search(Index, DefaultField, Query, Arg) when is_integer(Arg) ->
    SearchRef = stream_search(Index, DefaultField, Query),
    Results = collect_results(SearchRef, Arg, []),
    [Key || {Key, _Props} <- Results];
search(Index, DefaultField, Query, Arg) when is_atom(Arg) ->
    SearchRef = stream_search(Index, DefaultField, Query, Arg),
    Results = collect_results(SearchRef, 60000, []),
    [Key || {Key, _Props} <- Results].

search(Index, DefaultField, Query, Arg, Timeout) when is_integer(Timeout) ->
    SearchRef = stream_search(Index, DefaultField, Query, Arg),
    Results = collect_results(SearchRef, Timeout, []),
    [Key || {Key, _Props} <- Results].


stream_search(Index, DefaultField, Query) ->
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    case qilr_parse:string(AnalyzerPid, Query) of
        {ok, AST} ->
            R = execute(AST, Index, DefaultField, []);
        R ->
            throw(R)
    end,
    qilr_analyzer:close(AnalyzerPid),
    R.

stream_search(Index, DefaultField, Query, DefaultBool) ->
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    case qilr_parse:string(AnalyzerPid, Query, DefaultBool) of
        {ok, AST} ->
            R = execute(AST, Index, DefaultField, []);
        R ->
            throw(R)
    end,
    qilr_analyzer:close(AnalyzerPid),
    R.

doc_search(Index, DefaultField, Query) ->
    doc_search(Index, DefaultField, Query, 0, ?DEFAULT_RESULT_SIZE, 60000).

doc_search(Index, DefaultField, Query, QueryStart, QueryRows) ->
    doc_search(Index, DefaultField, Query, QueryStart, QueryRows, 60000).

doc_search(Index, DefaultField, Query, QueryStart, QueryRows, Timeout) ->
    case search(Index, DefaultField, Query, Timeout) of
        [] ->
            {0, []};
        Results0 ->
            Results = dedup(truncate_list(QueryStart, QueryRows, Results0), []),
            {length(Results0), [get_document(Index, DocId) || DocId <- Results]}
    end.

explain(Index, DefaultField, Query) ->
    explain(Index, DefaultField, [], Query).

explain(Index, DefaultField, Facets, Query) ->
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    {ok, Ops} = qilr_parse:string(AnalyzerPid, Query),
    R = riak_search_preplan:preplan(Ops, Index, DefaultField, Facets),
    qilr_analyzer:close(AnalyzerPid),
    R.

index_doc(Doc) ->
    {ok, Pid} = qilr_analyzer_sup:new_analyzer(),
    Result = (catch index_doc(Pid, Doc)),
    qilr_analyzer:close(Pid),
    Result.

index_doc(AnalyzerPid, #riak_idx_doc{id=DocId, index=Index, fields=Fields}=Doc) ->
    AnalyzedFields = analyze_fields(AnalyzerPid, Fields, []),
    WordMd = build_word_md(AnalyzedFields),
    [index_term(Index, Name, Value,
                DocId, build_props(Value, WordMd)) || {Name, Value} <- AnalyzedFields],
    DocBucket = Index ++ "_docs",
    DocObj = riak_object:new(riak_search_utils:to_binary(DocBucket),
                             riak_search_utils:to_binary(DocId),
                             Doc),
    RiakClient:put(DocObj, 1).

%% index_store(Doc, Obj) ->
%%     index_store(Doc, Obj, 1).

%% index_store(#riak_idx_doc{id=DocId}=Doc, Obj0, W) ->
%%     Md = dict:store("X-Riak-Search-Id", DocId, dict:new()),
%%     Obj = riak_object:update_metadata(Obj0, Md),
%%     ok = index_doc(Doc),
%%     RiakClient:put(Obj, W).

%% Internal functions
index_term(Index, Field, Term, Value, Props) ->
    index_internal(Index, Field, Term, {index, Index, Field, Term, Value, Props}).

index_internal(Index, Field, Term, Payload) ->
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
%% Run the operation...
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    RiakClient:put(Obj, 0).

analyze_fields(_AnalyzerPid, [], Accum) ->
    Accum;
analyze_fields(AnalyzerPid, [{Name, Value}|T], Accum) when is_list(Value) ->
    {ok, Tokens} = qilr_analyzer:analyze(AnalyzerPid, Value),
    Fields = [{Name, Token} || Token <- Tokens],
    analyze_fields(AnalyzerPid, T, Fields ++ Accum);
analyze_fields(AnalyzerPid, [{Name, Value}|T], Accum) ->
    analyze_fields(AnalyzerPid, T, [{Name, Value}|Accum]).

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
%% Start the query process ...
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
    M = collect_result(SearchRef, Timeout),
    io:format("collect_result: ~p~n", [M]),
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

build_props(Token, WordMd) ->
    case gb_trees:lookup(Token, WordMd) of
        none ->
            [];
        {value, Positions} ->
            [{word_pos, Positions},
             {freq, length(Positions)}]
    end.

build_word_md(Tokens) ->
    {_, Words} = lists:foldl(fun(Token, {Pos, Acc}) ->
                                     case gb_trees:lookup(Token, Acc) of
                                         {value, Positions} ->
                                             {Pos + 1, gb_trees:update(Token, [Pos|Positions], Acc)};
                                         none ->
                                             {Pos + 1, gb_trees:insert(Token, [Pos], Acc)}
                                     end end,
                             {1, gb_trees:empty()}, Tokens),
    Words.

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
                {multi_term, NTerms, TNode} ->
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
        {Na, La} = A,
        {Nb, Lb} = B,
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
