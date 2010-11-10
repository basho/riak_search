%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_string).
-export([
         preplan/2,
         chain_op/4,
         analyze_term/3,
         calculate_score/2
        ]).

-import(riak_search_utils, [to_binary/1]).

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-define(STREAM_TIMEOUT, 15000).
-record(scoring_vars, {term_boost, doc_frequency, num_docs}).

preplan(Op, State) -> 
    %% If this is a single term or phrase search, then collect some
    %% info for scoring purposes.
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    TermString = to_binary(Op#string.s),
    
    %% Analyze the string and call preplan to get the #term properties.
    {ok, Terms} = analyze_term(IndexName, FieldName, TermString),
    
    %% Get the counts for each term. We use the properties to answer
    %% two questions: 1) what node should we run on and 2) what is the
    %% result's score. For that reason, we store the samem information
    %% in multiple ways to make it easier to pull out.
    F = fun(Term) ->
        TermOp = #term { id=Op#string.id, s=Term },
        TermProps = riak_search_op:preplan(TermOp, State),
        Counts = riak_search_op:props_by_tag(node_weight, TermProps),
        DocFrequency = lists:sum([Count || {_, Count} <- Counts]) / length(Counts),
        Boost = proplists:get_value(boost, Op#string.flags, 1.0),

        %% These are used to get the target node.
        TermProps ++ 
        %% This is used below for scoring.
        [{?OPKEY({term, Term}, Op), {DocFrequency, Boost}}] ++
        %% This is used in riak_search_client for scoring.
        [{?OPKEY(scoring, Op), {DocFrequency, Boost}}]
    end,
    Props = lists:flatten([F(X) || X <- Terms]),

    %% Pull out information to get the target node. This is ignored
    %% unless we are running a proximity or phrase search.
    TargetNode = riak_search_op:get_target_node(Props),
    StringProp = {?OPKEY(target_node, Op), TargetNode},
    [StringProp|Props].

chain_op(Op, OutputPid, OutputRef, State) ->
    spawn_link(fun() -> start_loop(Op, OutputPid, OutputRef, State) end),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, State) ->
    %% Get the current index/field...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    TermString = to_binary(Op#string.s),

    %% Get the list of terms...
    {ok, Terms} = analyze_term(IndexName, FieldName, TermString),
    ProximityVal = detect_proximity_val(Op#string.flags),
    WildcardType = detect_wildcard_type(TermString),
    case Terms of
        [Term] when WildcardType == none -> 
            %% Stream the results for a single term...
            TransformFun = generate_transform_function(Term, State),
            NewOp = #term { s=Term, transform=TransformFun };
        [Term] when WildcardType == end_wildcard_all -> 
            %% Stream the results for a '*' wildcard...
            {FromTerm, ToTerm} = calculate_range(Term, all),
            NewOp = #range_sized { from=FromTerm, to=ToTerm, size=all };
        [Term] when WildcardType == end_wildcard_single -> 
            %% Stream the results for a '?' wildcard...
            {FromTerm, ToTerm} = calculate_range(Term, single),
            NewOp = #range_sized { from=FromTerm, to=ToTerm, size=erlang:size(Term) };
        _ when is_integer(ProximityVal) ->
            TermOps = [#term { s=X, transform=generate_transform_function(X, State)} || X <- Terms],
            NewOp = #proximity { id=Op#string.id, ops=TermOps, proximity=ProximityVal };
        _ when ProximityVal == undefined ->
            TermOps = [#term { s=X, transform=generate_transform_function(X, State)} || X <- Terms],
            NewOp = #proximity { id=Op#string.id, ops=TermOps, proximity=exact }
    end,    
    riak_search_op:chain_op(NewOp, OutputPid, OutputRef, State).

%% Create transform function, taking scoring values into account.
generate_transform_function(Term, State) ->
    %% Calculate the DocFrequency...
    StateProps = State#search_state.props,
    {DocFrequency, Boost} = hd(riak_search_op:props_by_tag({term, Term}, StateProps)),

    %% Create the scoring vars record...
    ScoringVars = #scoring_vars {
        term_boost = Boost,
        doc_frequency = DocFrequency,
        num_docs = State#search_state.num_docs
    },

    %% TODO - Add inline field support to filter function.
    %% TODO - Filter out empty searches.
    IndexName = State#search_state.index,
    fun({DocID, Props}) ->
        NewProps = calculate_score(ScoringVars, Props),
        {IndexName, DocID, NewProps}
    end.
    


%% Return the proximity setting from flags
detect_proximity_val(Flags) ->
    proplists:get_value(proximity, Flags, undefined).


%% If the string ends with an unescaped * or ? then it's a wildcard,
%% but only if there are no other unescaped *'s or ?'s.
detect_wildcard_type(<<$\\, _,T/binary>>) ->
    %% Discard any escaped chars.
    detect_wildcard_type(T);
detect_wildcard_type(<<$*>>) ->
    %% Last char is an unescaped *.
    end_wildcard_all;
detect_wildcard_type(<<$?>>) ->
    %% Last char is an unescaped ?.
    end_wildcard_single;
detect_wildcard_type(<<$*,_/binary>>) ->
    inner_wildcard;
detect_wildcard_type(<<$?,_/binary>>) ->
    inner_wildcard;
detect_wildcard_type(<<_,T/binary>>) ->
    detect_wildcard_type(T);
detect_wildcard_type(<<>>) ->
    none.

calculate_range(B, all) ->
    {{inclusive, B}, {inclusive, binary_last(B)}};
calculate_range(B, single) ->
    {{inclusive, binary_first(B)}, {inclusive, binary_last(B)}}.

binary_first(Term)  ->
    <<Term/binary, 0/integer>>.

binary_last(Term)  ->
    <<Term/binary, 255/integer>>.
    


%% Analyze the term, return {ok, [Terms]} where Terms is a list of
%% term binaries. This is split into a separate function because
%% otherwise the compiler complains about an unsafe usage of 'Terms'.
analyze_term(IndexName, FieldName, TermString) ->
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    try
        {ok, Schema} = riak_search_config:get_schema(IndexName),
        Field = Schema:find_field(FieldName),
        AnalyzerFactory = Schema:analyzer_factory(Field),
        AnalyzerArgs = Schema:analyzer_args(Field),
        {ok, _Terms} = qilr_analyzer:analyze(AnalyzerPid, TermString, AnalyzerFactory, AnalyzerArgs)
    after 
        qilr:close_analyzer(AnalyzerPid)
    end.

calculate_score(ScoringVars, Props) ->
    %% Pull from ScoringVars...
    TermBoost = ScoringVars#scoring_vars.term_boost,
    DocFrequency = ScoringVars#scoring_vars.doc_frequency + 1,
    NumDocs = ScoringVars#scoring_vars.num_docs + 1,

    %% Pull freq from Props. (If no exist, use 1).
    Frequency = length(proplists:get_value(p, Props, [])),
    DocFieldBoost = proplists:get_value(boost, Props, 1),

    %% Calculate the score for this term, based roughly on Lucene
    %% scoring. http://lucene.apache.org/java/2_4_0/api/org/apache/lucene/search/Similarity.html
    TF = math:pow(Frequency, 0.5),
    IDF = (1 + math:log(NumDocs/DocFrequency)),
    Norm = DocFieldBoost,
    
    Score = TF * math:pow(IDF, 2) * TermBoost * Norm,
    ScoreList = case lists:keyfind(score, 1, Props) of
                    {score, OldScores} ->
                        [Score|OldScores];
                    false ->
                        [Score]
                end,
    lists:keystore(score, 1, Props, {score, ScoreList}).
