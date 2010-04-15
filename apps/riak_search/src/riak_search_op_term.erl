-module(riak_search_op_term).
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

start_loop(Op, OutputPid, OutputRef, QueryProps) ->
    %% Get the scoring vars...
    ScoringVars = #scoring_vars {
        term_boost = proplists:get_value(boost, Op#term.options, 1),
        doc_frequency = hd([X || {node_weight, _, X} <- Op#term.options]),
        num_docs = proplists:get_value(num_docs, QueryProps)
    },

    %% Create filter function...
    Facets = proplists:get_all_values(facets, Op#term.options),
    Fun = fun(_Value, Props) ->
        riak_search_facets:passes_facets(Props, Facets)
    end,

    %% Get the subterm range...
    {Index, Field, Term} = Op#term.q,
    {SubType, StartSubTerm, EndSubTerm} = detect_subterms(lists:flatten(Facets)),

    %% Start streaming the results...
    {ok, Ref} = riak_search:stream(Index, Field, Term, SubType, StartSubTerm, EndSubTerm, Fun),

    %% Gather the results...
    loop(ScoringVars, Ref, OutputPid, OutputRef).

loop(ScoringVars, Ref, OutputPid, OutputRef) ->
    receive 
        {result, '$end_of_table', Ref} ->
            OutputPid!{disconnect, OutputRef};

        {result, {Key, Props}, Ref} ->
            NewProps = calculate_score(ScoringVars, Props),
            OutputPid!{results, [{Key, NewProps}], OutputRef},
            loop(ScoringVars, Ref, OutputPid, OutputRef)
    end.

calculate_score(ScoringVars, Props) ->
    %% Pull from ScoringVars...
    TermBoost = ScoringVars#scoring_vars.term_boost,
    DocFrequency = ScoringVars#scoring_vars.doc_frequency,
    NumDocs = ScoringVars#scoring_vars.num_docs,

    %% Pull freq from Props. (If no exist, use 1).
    Frequency = proplists:get_value(freq, Props, 1),
    DocFieldBoost = proplists:get_value(boost, Props, 1),

    %% Calculate the score for this term, based roughly on Lucene
    %% scoring. http://lucene.apache.org/java/2_4_0/api/org/apache/lucene/search/Similarity.html
    TF = math:pow(Frequency, 0.5),
    IDF = (1 + math:log(NumDocs/(DocFrequency + 1))),
    Norm = DocFieldBoost,
    
    Score = TF * math:pow(IDF, 2) * TermBoost * Norm,
    Props ++ [{score, [Score]}].
    
    

%% Detect a subterm filter if it's a top level facet
%% operation. Otherwise, if it's anything fancy, we will handle it
%% with the FilterFunction passed to the backend, using
%% riak_searc_facets:passes_facets/2. We do this because there might
%% be some crazy ANDs/NOTs/ORs that we just can't handle here.
detect_subterms(Ops) ->
    case detect_subterms_inner(Ops) of
        {SubType, StartSubTerm, EndSubTerm} ->
            {SubType, StartSubTerm, EndSubTerm};
        undefined ->
            SubType = detect_subtype(Ops),
            {SubType, all, all}
    end.

detect_subterms_inner([Op|Ops]) when is_record(Op, inclusive_range) ->
    StartOp = hd(Op#inclusive_range.start_op),
    EndOp = hd(Op#inclusive_range.end_op),
    case {StartOp#term.q, EndOp#term.q} of
        {{_, subterm, {SubType, StartSubTerm}}, {_, subterm, {SubType, EndSubTerm}}} ->
            {SubType, StartSubTerm, EndSubTerm};
        _ ->
            detect_subterms_inner(Ops)
    end;
detect_subterms_inner([Op|Ops]) when is_record(Op, exclusive_range) ->
    StartOp = hd(Op#exclusive_range.start_op),
    EndOp = hd(Op#exclusive_range.end_op),
    case {StartOp#term.q, EndOp#term.q} of
        {{_, subterm, {SubType, StartSubTerm}}, {_, subterm, {SubType, EndSubTerm}}} ->
            {SubType, StartSubTerm, EndSubTerm};
        _ ->
            detect_subterms_inner(Ops)
    end;
detect_subterms_inner([Op|Ops]) when is_record(Op, term) ->
    case Op#term.q of
        {_, subterm, {SubType, SubTerm}} ->
            {SubType, SubTerm, SubTerm};
        _ ->
            detect_subterms_inner(Ops)
    end;
detect_subterms_inner([_|Ops]) ->
    detect_subterms_inner(Ops);
detect_subterms_inner([]) ->
    undefined.


detect_subtype([Op|Ops]) when is_record(Op, term) ->
    case Op#term.q of
        {_, subterm, {SubType, _}} -> 
            SubType;
        _ ->
            detect_subtype(Ops)
    end;
detect_subtype([Op|Ops]) when is_tuple(Op) ->
    case detect_subtype(element(2, Op)) of
        0 -> detect_subtype(Ops);
        Value -> Value
    end;
detect_subtype([Op|Ops]) when is_list(Op) ->
    case detect_subtype(Op) of
        0 -> detect_subtype(Ops);
        Value -> Value
    end;
detect_subtype([]) ->
    0.
