%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_term).
-export([
         extract_scoring_props/1,
         preplan/2,
         chain_op/4,
         default_filter/2
        ]).

-import(riak_search_utils, [to_binary/1]).
-record(scoring_vars, {term_boost, doc_frequency, num_docs}).

%% Look up results from the index without any kind of text analyzis on
%% term. Filter and transform the results, and send them to the
%% OutputPid.

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

extract_scoring_props(#term{doc_freq=Frequency, boost=Boost}) ->
    {Frequency, Boost}.

%% Need term count for node planning. Used in #intersection and
%% #union. Calculate this during preplan based on where the most
%% results come from.

%% [{info, Index, Field, String}] -> [{Node, Count}]
%% Need term count for scoring, in riak_search_op_search.

%% {info, Index, Field, String} -> [{Node, Count}]
%% Generate term count in term, managed by string.

%% TODO Make this function less weird...
preplan(Op, State) ->
    %% Get info about the term, return in props...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    Term = to_binary(Op#term.s),
    Weights1 = info(IndexName, FieldName, Term),
    Weights2 = [{Node, Count} || {_, Node, Count} <- Weights1],
    TotalCount = lists:sum([Count || {_, _, Count} <- Weights1]),
    case length(Weights1) == 0 of
        true  ->
            throw({error, data_not_available, {IndexName, FieldName, Term}}),
            DocFrequency = undefined; %% Make compiler happy.
        false ->
            DocFrequency = TotalCount / length(Weights1)
    end,
    Op#term { weights=Weights2, doc_freq=DocFrequency }.

chain_op(Op, OutputPid, OutputRef, State) ->
    F = fun() ->
                erlang:link(State#search_state.parent),
                start_loop(Op, OutputPid, OutputRef, State)
        end,
    erlang:spawn_link(F),
    {ok, 1}.

-spec start_loop(any(), pid(), reference(), #search_state{}) -> any().
start_loop(Op, OutputPid, OutputRef, State) ->
    %% Get the current index/field...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    Term = to_binary(Op#term.s),

    %% Stream the results for a single term...
    FilterFun = State#search_state.filter,
    {ok, Ref} = stream(IndexName, FieldName, Term, FilterFun),

    %% Collect the results...
    TransformFun = generate_transform_function(Op, State),
    riak_search_op_utils:gather_stream_results(Ref, OutputPid, OutputRef, TransformFun).

-spec stream(index(), field(), s_term(), fun()) -> {ok, stream_ref()}.
stream(Index, Field, Term, FilterFun) ->
    %% Get the primary preflist, minus any down nodes. (We don't use
    %% secondary nodes since we ultimately read results from one node
    %% anyway.)
    Preflist = riak_search_utils:preflist(Index, Field, Term),

    %% Try to use the local node if possible. Otherwise choose
    %% randomly.
    case lists:keyfind(node(), 2, Preflist) of
        false ->
            PreflistEntry = riak_search_utils:choose(Preflist);
        PreflistEntry ->
            PreflistEntry = PreflistEntry
    end,
    riak_search_vnode:stream([PreflistEntry], Index, Field, Term, FilterFun,
                             self()).

default_filter(_, _) -> true.

info(Index, Field, Term) ->
    %% Get the primary preflist, minus any down nodes. (We don't use
    %% secondary nodes since we ultimately read results from one node
    %% anyway.)
    Preflist = riak_search_utils:preflist(Index, Field, Term),

    {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
    Timeout = app_helper:get_env(riak_search, range_loop_timeout, 5000),
    {ok, Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, [], Timeout),
    Results.

%% Create transform function, taking scoring values into account.
generate_transform_function(Op, State) ->
    %% Create the scoring vars record...
    ScoringVars = #scoring_vars {
        term_boost = Op#term.boost,
        doc_frequency = Op#term.doc_freq,
        num_docs = State#search_state.num_docs
    },

    %% Transform the result by adding the Index and calculating the
    %% Score.
    IndexName = State#search_state.index,
    fun({DocID, Props}) ->
        NewProps = calculate_score(ScoringVars, Props),
        {IndexName, DocID, NewProps}
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
