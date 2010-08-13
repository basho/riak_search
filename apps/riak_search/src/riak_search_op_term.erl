%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_term).
-export([
         preplan_op/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
-define(STREAM_TIMEOUT, 15000).

-record(scoring_vars, {term_boost, doc_frequency, num_docs}).
preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef, QueryProps) ->
    spawn_link(fun() -> start_loop(Op, OutputPid, OutputRef, QueryProps) end),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, QueryProps) ->
    %% Get the scoring vars...
    ScoringVars = #scoring_vars {
        term_boost = proplists:get_value(boost, Op#term.options, 1),
        doc_frequency = hd([X || {node_weight, _, X} <- Op#term.options] ++ [0]),
        num_docs = proplists:get_value(num_docs, QueryProps)
    },

    %% Create filter function...
    Facets = proplists:get_all_values(facets, Op#term.options),
    Fun = fun(_DocID, Props) ->
        riak_search_facets:passes_facets(Props, Facets)
    end,

    %% Start streaming the results...
    {Index, Field, Term} = Op#term.q,
    {ok, Ref} = stream(Index, Field, Term, Fun),

    %% Gather the results...
    %% TODO - This type conversion should be removed in bug 484
    %% "Standardize on a string representation".
    IndexB = riak_search_utils:to_binary(Index),
    loop(IndexB, ScoringVars, Ref, OutputPid, OutputRef).

stream(Index, Field, Term, FilterFun) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    %% Calculate the preflist with full N but then only ask the first
    %% node in it.  Preflists are ordered with primaries first followed
    %% by fallbacks, so this will prefer a primary node over a fallback.
    [FirstEntry|_] = riak_core_apl:get_apl(Partition, N, riak_search),
    Preflist = [FirstEntry],
    riak_search_vnode:stream(Preflist, Index, Field, Term, FilterFun, self()).

loop(Index, ScoringVars, Ref, OutputPid, OutputRef) ->
    receive 
        {Ref, done} ->
            %io:format("riak_search_op_term: disconnect ($end_of_table)~n"),
            OutputPid!{disconnect, OutputRef};
            
        {Ref, {result_vec, ResultVec}} ->
            % todo: scoring
            F = fun({DocID, Props}) ->
                        NewProps = calculate_score(ScoringVars, Props),
                        {Index, DocID, NewProps} 
                end,
            ResultVec2 = lists:map(F, ResultVec),
            %io:format("ResultVec2 = ~p~n", [ResultVec2]),
            OutputPid!{results, ResultVec2, OutputRef},
            loop(Index, ScoringVars, Ref, OutputPid, OutputRef);

        %% TODO: Check if this is dead code
        {Ref, {result, {DocID, Props}}} ->
            NewProps = calculate_score(ScoringVars, Props),
            OutputPid!{results, [{Index, DocID, NewProps}], OutputRef},
            loop(Index, ScoringVars, Ref, OutputPid, OutputRef)
    after
        ?STREAM_TIMEOUT ->
            throw(stream_timeout)
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
    ScoreList = case lists:keyfind(score, 1, Props) of
                    {score, OldScores} ->
                        [Score|OldScores];
                    false ->
                        [Score]
                end,
    lists:keystore(score, 1, Props, {score, ScoreList}).
