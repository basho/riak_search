-module(riak_search_utils_tests).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").

ptransform_test() ->
    Test = fun(List) ->
                   F = fun(X) -> X * 2 end,
                   ?assertEqual(lists:sort(riak_search_utils:ptransform(F,
                                                                        List)),
                                lists:map(F, List))
           end,
    Test(lists:seq(0, 0)),
    Test(lists:seq(1, 1)),
    Test(lists:seq(1, 2)),
    Test(lists:seq(1, 3)),
    Test(lists:seq(1, 20)),
    Test(lists:seq(1, 57)).

combine_terms_test() ->
    X = {<<"index">>, <<"docid">>, [{score, [1.0]},
                                    {p, [1, 4, 13]},
                                    {foo, bar}]},
    Y = {<<"index">>, <<"docid">>, [{score, [1.5]},
                                    {p, [5, 13, 19]},
                                    {foo, baz}]},
    Z = {<<"index">>, <<"docid2">>, []},

    XY = {<<"index">>, <<"docid">>, [{score, [1.0, 1.5]},
                                     {p, [1, 4, 13, 5, 13, 19]}]},

    ?assertEqual(XY, riak_search_utils:combine_terms(X, Y)),
    ?assertThrow({could_not_combine, X, Z},
                 riak_search_utils:combine_terms(X, Z)).

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

ptransform_test_qc_test() ->
    F = fun(X) -> X * 2 end,
    Prop = ?FORALL({List, NumProcesses}, {list(int()), choose(1, 8)},
                   lists:sort(riak_search_utils:ptransform(F,
                                                           List,
                                                           NumProcesses)) ==
                   lists:sort(lists:map(F, List))),
    ?assert(eqc:quickcheck(eqc:numtests(500, ?QC_OUT(Prop)))).

-endif.
