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
    %% Parse the term into subterms. Need to know the index and field,
    %% which means we need to have calculated scope, which means we need to pass in query state.
    %% Get info about the term, return in props.

    %% If this is a single term or phrase search, then collect some
    %% info for scoring purposes.
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    TermString = to_binary(Op#string.s),
    case detect_wildcard_type(TermString) of
        none ->
            %% Create properties used by other areas of code:
            %% {{string, Ref}, {Terms, DocFrequency, Boost}}
            Boost = proplists:get_value(boost, Op#string.flags, 1.0),
            {ok, Terms} = analyze_term(IndexName, FieldName, TermString),
            Ops = [#term { s=X } || X <- Terms],
            SubProps = riak_search_op:preplan(Ops, State),
            Counts = [Count || {{term, _}, {_, Count}} <- SubProps],
            DocFrequency = trunc(lists:sum(Counts) / length(Counts)),
            StringProp = {?OPKEY(Op), {Terms, DocFrequency, Boost}},
            [StringProp|SubProps];
        _ ->
            []
    end.

chain_op(Op, OutputPid, OutputRef, State) ->
    spawn_link(fun() -> start_loop(Op, OutputPid, OutputRef, State) end),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, State) ->
    %% Get the current index/field...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    TermString = to_binary(Op#string.s),
    StateProps = State#search_state.props,

    %% Get the list of terms...
    case lists:keyfind(?OPKEY(Op), 1, StateProps) of
        {_, Val} ->
            {Terms, DocFrequency, Boost} = Val;
        false ->
            {ok, Terms} = analyze_term(IndexName, FieldName, TermString),
            DocFrequency = 0,
            Boost = 0
    end,

    %% Create the scoring vars record...
    ScoringVars = #scoring_vars {
        term_boost = Boost,
        doc_frequency = DocFrequency,
        num_docs = State#search_state.num_docs
    },

    %% TODO - Add inline field support to filter function.
    %% TODO - Filter out empty searches.
    TransformFun = fun({DocID, Props}) ->
                           NewProps = calculate_score(ScoringVars, Props),
                           {IndexName, DocID, NewProps}
                   end,
    ProximityVal = detect_proximity_val(Op#string.flags),
    WildcardType = detect_wildcard_type(TermString),
    case Terms of
        [Term] when WildcardType == none -> 
            %% Stream the results for a single term...
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
            TermOps = [#term { s=X, transform=TransformFun } || X <- Terms],
            NewOp = #proximity { ops=TermOps, proximity=ProximityVal };
        _ when ProximityVal == undefined ->
            TermOps = [#term { s=X, transform=TransformFun } || X <- Terms],
            NewOp = #proximity { ops=TermOps, proximity=exact }
    end,    
    riak_search_op:chain_op(NewOp, OutputPid, OutputRef, State).


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
