-module(riak_search_op_multi_term).
-export([
         preplan_op/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
-record(scoring_vars, {term_boost, doc_frequency, num_docs}).

preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef, QueryProps) ->
    spawn_link(fun() -> start_loop(Op, OutputPid, OutputRef, QueryProps) end),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, _QueryProps) ->
    %%%% TODO: SCORING
    %%%%  ^update: punt, map dependencies?
    ScoringVars = #scoring_vars {
        term_boost = 1,
        doc_frequency = 1,
        num_docs = 1
    },

    %% first element in term list holds facets
    {multi_term, EL, _OpN} = Op,
    {term, _IFT, T1_Props} = hd(EL),
    Facets = proplists:get_all_values(facets, T1_Props),

    %% create FilterFun    
    Fun = fun(_Value, Props) ->
        riak_search_facets:passes_facets(Props, Facets)
    end,
    
    %io:format("riak_search_op_multi_term: start_loop(~p, ~p, ~p, ~p)~n",
    %    [Op, OutputPid, OutputRef, QueryProps]),

    %%
    %% riak_search_op_multi_term: start_loop(
    %% Op:                     {multi_term,
    %%                          [{term,{"search","payload","garbage"}, Props},
    %%                          {term,{"search","payload","truck"}, Props}], 'dev1@127.0.0.1'},
    %% OutputPid:              <0.299.0>,
    %% OutputRef:              #Ref<0.0.0.781>,
    %% QueryProps:             [{num_docs, 231}])
    %%

    {multi_term, Terms, _TNode} = Op,
    {ok, Ref} = riak_search:multi_stream(Terms, Fun),
    loop(ScoringVars, Ref, OutputPid, OutputRef).

loop(ScoringVars, Ref, OutputPid, OutputRef) ->
    receive
        {result, '$end_of_table', Ref} ->
            %io:format("riak_search_op_multi_term: $end_of_table: Ref = ~p~n", [Ref]),
            OutputPid!{disconnect, OutputRef};

        {result_vec, ResultVec, Ref} ->
            ResultVec2 = lists:map(fun({Key, Props}) ->
                NewProps = calculate_score(ScoringVars, Props),
                {Key, NewProps} end, ResultVec),
            OutputPid!{results, ResultVec2, OutputRef},
            loop(ScoringVars, Ref, OutputPid, OutputRef);

        {result, {Key, Props}, Ref} ->
            NewProps = calculate_score(ScoringVars, Props),
            OutputPid!{results, [{Key, NewProps}], OutputRef},
            loop(ScoringVars, Ref, OutputPid, OutputRef)
    end.

calculate_score(ScoringVars, Props) ->
    %% Pull from ScoringVars...
    TermBoost = ScoringVars#scoring_vars.term_boost,
    DocFrequency = ScoringVars#scoring_vars.doc_frequency + 1,
    NumDocs = ScoringVars#scoring_vars.num_docs + 1,

    %% Pull freq from Props. (If no exist, use 1).
    Frequency = proplists:get_value(freq, Props, 1),
    DocFieldBoost = proplists:get_value(boost, Props, 1),

    %% Calculate the score for this term, based roughly on Lucene
    %% scoring. http://lucene.apache.org/java/2_4_0/api/org/apache/lucene/search/Similarity.html
    TF = math:pow(Frequency, 0.5),
    IDF = (1 + math:log(NumDocs/DocFrequency)),
    Norm = DocFieldBoost,

    Score = TF * math:pow(IDF, 2) * TermBoost * Norm,
    Props ++ [{score, [Score]}].

