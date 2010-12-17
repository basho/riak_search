%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_operators_qc).
-ifdef(EQC).

%% This module tests the middle layers of search (preplanner and all
%% the operators except #term) by generating a sample query, executing
%% it, and checking the results. 
%%
%% A #mockterm{} operator is used instead of #term{} to prevent us
%% from needing a backing store.

-export([
    test/0, 
    test/1,
    calculate_mocked_results/1
]).

-import(riak_search_utils, [to_list/1]).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-include("riak_search.hrl").

-define(TEST_EUNIT_NODE, 'eunit@127.0.0.1').

maybe_start_network() ->
    os:cmd("epmd -daemon"),
    case net_kernel:start([?TEST_EUNIT_NODE]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        X ->
            X
    end.

test() -> 
    test(100).
test(N) ->
    maybe_start_network(),
    eqc:quickcheck(numtests(N, prop_operator_test())),
    ok.

%% Generators...

qilr_ops() ->
    frequency([
        {10, #intersection { ops=[?LAZY(qilr_ops1())] }},
        {10, #union { ops=[?LAZY(qilr_ops1())] }},
        {70, #mockterm { results=?LET(Size, nat(), resultbatch(Size)) }},
        {10, [?LAZY(qilr_ops())|[?LAZY(qilr_ops1())]]}
    ]).

qilr_ops1() ->
    frequency([
        {10, #intersection { ops=[?LAZY(qilr_ops1())] }},
        {10, #union { ops=[?LAZY(qilr_ops1())] }},
        {10, #negation { op=#intersection { ops=[?LAZY(qilr_ops1())] }}},
        {10, #negation { op=#union { ops=[?LAZY(qilr_ops1())] }}},
        {10, #negation { op=#mockterm { results=?LET(Size, nat(), resultbatch(Size)) }}},
        {70, #mockterm { results=?LET(Size, nat(), resultbatch(Size)) }},
        {10, [?LAZY(qilr_ops1())|[?LAZY(qilr_ops1())]]}
    ]).

resultbatch(Size) ->
    [begin
        resultlist(X * 10000)
    end || X <- lists:seq(1, Size)].
    
resultlist(N) ->
    M = N + random:uniform(3),
    oneof([
        [{index, M, [{score, scorelist()}]}],
        [{index, M, [{score, scorelist()}]}|?LAZY(resultlist(M))]
    ]).

scorelist() ->
    oneof([
        [choose(1, 100)],
        [choose(1, 100)|?LAZY(scorelist())]
    ]).

%% Properties...

prop_operator_test() ->
    maybe_start_network(),
    {ok, Client} = get_client(),
    ?FORALL(QilrOps, qilr_ops(), 
        ?WHENFAIL(?PRINT(QilrOps), begin
            %% Calculate mocked results...
            {MockedResultSet, _Negate} = calculate_mocked_results(QilrOps),
            MockedResults = lists:sort(sets:to_list(MockedResultSet)),

            %% Get results using Riak Search...
            {_, ActualResults} = Client:search("search", QilrOps, 0, infinity, 10000),
            ActualResults1 = lists:sort([X || {X, _} <- lists:flatten(ActualResults)]),

            %% Compare the sorted results...
            case MockedResults == ActualResults1 of
                true -> true;
                false ->
                    ?PRINT({QilrOps, MockedResults, ActualResults1}),
                    false
            end
        end)
    ).


%% calculate_mocked_results/1 mimics Riak Search operator logic given
%% a set of Qilr operators.

%% LIST - If we have a list of operations, treat it like an OR.

calculate_mocked_results([Op]) ->
    calculate_mocked_results(Op);
calculate_mocked_results(Ops) when is_list(Ops) ->
    calculate_mocked_results(#union { ops=Ops });

%% AND
calculate_mocked_results(#intersection { ops=Ops }) -> 
    F = fun(Op, Acc) -> 
        %% If this is a Negation, then Acc -- ResultSet.
        %% Otherwise, do an intersection.
        {ResultSet, Negate} = calculate_mocked_results(Op),
        case Negate of 
            true  when Acc==first -> first;
            true  when Acc/=first -> sets:subtract(Acc, ResultSet);
            false when Acc==first -> ResultSet;
            false when Acc/=first -> sets:intersection(ResultSet, Acc)
        end
    end,
    NewResultSet = lists:foldl(F, first, lists:flatten([Ops])),
    case NewResultSet == first of
        true -> {sets:new(), false};
        false -> {NewResultSet, false}
    end;

%% OR
calculate_mocked_results(#union { ops=Ops }) -> 
    F = fun(Op, Acc) -> 
        %% Negations are ignored in OR clauses.
        %% Otherwise, do a union.
        {ResultSet, Negate} = calculate_mocked_results(Op),
        case Negate of 
            true  -> Acc;
            false -> sets:union(ResultSet, Acc)
        end
    end,
    NewResultSet = lists:foldl(F, sets:new(), lists:flatten([Ops])),
    {NewResultSet, false};

%% NOT
calculate_mocked_results(#negation { op=Ops }) -> 
    {ResultSet, Negate} = calculate_mocked_results(Ops),
    %% If we're notting a not, then use an empty result set.
    case Negate of 
        true -> {sets:new(), false};
        false -> {ResultSet, true}
    end;

%% TERM
calculate_mocked_results(#mockterm { results=Results }) -> 
    Results1 = [X || {X, _} <- lists:flatten(Results)],
    {sets:from_list(Results1), false}.


get_client() ->
    case get(client) of
        undefined -> 
            put(client, riak_search:local_client());
        _ -> ignore
    end,
    get(client).

-endif. %EQC

