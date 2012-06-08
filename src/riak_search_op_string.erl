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
         detect_wildcard/1,
         detect_proximity_val/1,
         calculate_range/2
        ]).

-import(riak_search_utils, [to_binary/1]).

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-define(STREAM_TIMEOUT, 15000).

preplan(Op, State) -> 
    %% If this is a single term or phrase search, then collect some
    %% info for scoring purposes.
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    TermString = to_binary(Op#string.s),

    %% Analyze the string and call preplan to get the #term properties.
    {ok, Terms} = analyze_term(IndexName, FieldName, TermString),
    WildcardType = detect_wildcard(Op#string.flags),
    ProximityVal = detect_proximity_val(Op#string.flags),
    Boost = proplists:get_value(boost, Op#string.flags, 1.0),
    case Terms of
        [skip] ->
            throw({error, stopword_not_allowed_in_query, TermString}),
            NewOp = undefined; % Make compiler happy.
        [Term] when WildcardType == none ->
            %% Stream the results for a single term...
            NewOp = #term { s=Term, boost=Boost };
        [Term] when WildcardType == glob ->
            %% Stream the results for a '*' wildcard...
            {FromTerm, ToTerm} = calculate_range(Term, all),
            NewOp = #range_sized { from=FromTerm, to=ToTerm, size=all };
        [Term] when WildcardType == char ->
            %% Stream the results for a '?' wildcard...
            {FromTerm, ToTerm} = calculate_range(Term, single),
            %% Add 1 because Term doesn't include wildcard
            NewOp = #range_sized { from=FromTerm, to=ToTerm, size=size(Term)+1 };
        _ when is_integer(ProximityVal) ->
            %% Filter out skipped terms...
            TermOps = [#term { s=X, boost=Boost } || X <- Terms, X /= skip],
            NewOp = #proximity { id=Op#string.id, ops=TermOps, proximity=ProximityVal };
        _ when ProximityVal == undefined ->
            %% Error on skipped terms...
            HasSkippedTerms = length([X || X <- Terms, X == skip]) > 0,
            (not HasSkippedTerms) orelse throw({error, stopword_not_allowed_in_proximity, TermString}),
            TermOps = [#term { s=X, boost=Boost } || X <- Terms],
            NewOp = #proximity { id=Op#string.id, ops=TermOps, proximity=exact }
    end,
    riak_search_op:preplan(NewOp, State).

chain_op(Op, _OutputPid, _OutputRef, _State) ->
    %% Any #string{} operators should get rewritten to #term{},
    %% #proximity{}, or #range{} operators above.
    throw({invalid_query_tree, Op}).

%% Return the proximity setting from flags
detect_wildcard(Flags) ->
    proplists:get_value(wildcard, Flags, none).

%% Return the proximity setting from flags
detect_proximity_val(Flags) ->
    proplists:get_value(proximity, Flags, undefined).

calculate_range(B, all) ->
    {{inclusive, B}, {inclusive, calculate_range_last(B)}};
calculate_range(B, single) ->
    {{inclusive, calculate_range_first(B)}, {inclusive, calculate_range_last(B)}}.

calculate_range_first(skip)  ->
    skip;
calculate_range_first(Term)  ->
    <<Term/binary, 0/integer>>.

calculate_range_last(skip)  ->
    skip;
calculate_range_last(Term)  ->
    <<Term/binary, 255/integer>>.

%% Analyze the term, return {ok, [Terms]} where Terms is a list of
%% term binaries. This is split into a separate function because
%% otherwise the compiler complains about an unsafe usage of 'Terms'.
analyze_term(IndexName, FieldName, TermString) ->
    {ok, Schema} = riak_search_config:get_schema(IndexName),
    Field = Schema:find_field(FieldName),
    AnalyzerFactory = Schema:analyzer_factory(Field),
    AnalyzerArgs = Schema:analyzer_args(Field),
    riak_search:analyze(TermString, AnalyzerFactory, AnalyzerArgs).

