-module(op_tests).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-define(M, riak_search_op_proximity).

%%%===================================================================
%%% QuickCheck
%%%===================================================================

-ifdef(EQC).

is_sequential_prop() ->
    ?FORALL(L, non_empty(list(int())),
            begin
                A = lists:min(L),
                Z = lists:max(L),
                ?assertEqual(L =:= lists:seq(A, Z),
                             ?M:is_sequential(L)),
                true
            end).

is_sequential_test() ->
    true = eqc:quickcheck(eqc:numtests(5000, is_sequential_prop())).

-endif.

%%%===================================================================
%%% Unit
%%%===================================================================

remove_next_term_position_1_test() ->
    %% Test when ToRemove == undefined...
    Input =
        [[1,2,3],
         [4,5,6],
         [7,8,9]],
    Expected =
        [[2,3],
         [4,5,6],
         [7,8,9]],
    ?assertEqual(?M:remove_next_term_position(Input), Expected).

remove_next_term_position_2_test() ->
    %% Test when there is a single matching position in middle list...
    Input =
        [[4,5,6],
         [1,2,3],
         [7,8,9]],
    Expected =
        [[4,5,6],
         [2,3],
         [7,8,9]],
    ?assertEqual(?M:remove_next_term_position(Input), Expected).

remove_next_term_position_3_test() ->
    %% Test when there is a single matching position in end list...
    Input =
        [[4,5,6],
         [7,8,9],
         [1,2,3]],
    Expected =
        [[4,5,6],
         [7,8,9],
         [2,3]],
    ?assertEqual(?M:remove_next_term_position(Input), Expected).

remove_next_term_position_4_test() ->
    %% Test when there are multiple matching positions. Should only remove the last one.
    Input =
        [[1,2,3],
         [4,5,6],
         [7,8,9],
         [1,2,3]],
    Expected =
        [[1,2,3],
         [4,5,6],
         [7,8,9],
         [2,3]],
    ?assertEqual(?M:remove_next_term_position(Input), Expected).

remove_next_term_position_5_test() ->
    %% Test when there are multiple matching positions. Should only remove the last one.
    Input =
        [[0],
         [1,2,3],
         [4,5,6],
         [7,8,9],
         [1,2,3]],
    Expected =
        [[0],
         [1,2,3],
         [4,5,6],
         [7,8,9],
         [2,3]],
    ?assertEqual(?M:remove_next_term_position(Input), Expected).

remove_next_term_position_6_test() ->
    %% Test when a list is empty...
    Input =
        [[1,2,3],
         [4,5,6],
         [],
         [1,2,3]],
    Expected =
        undefined,
    ?assertEqual(?M:remove_next_term_position(Input), Expected).
