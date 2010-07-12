-module(testing_parser).

-include_lib("eunit/include/eunit.hrl").



parse(Query) ->
    file:write_file("/tmp/output.txt", io_lib:format("~p~n", [parse(Query, 'or')]), [append]),
    parse(Query, 'or').

parse(Query, Bool) ->
    Schema = riak_search_schema:new("search", undefined, "value", [],
        Bool, "com.basho.search.analysis.DefaultAnalyzerFactory"),
    qilr_parse:string(Query, Schema).

multiple_terms_test_() ->
    [fun() ->
             ?assertMatch({ok,[{lor,[{term,<<"planes">>,[]},
                                     {term,<<"trains">>,[]},
                                     {term,<<"automobiles">>,[]}]}]},
                   parse("planes trains automobiles")) end].

field_test_() ->
    [fun() ->
             ?assertMatch({ok,[{field,"title",<<"peace">>,[required]}]},
                          parse("+title:peace")),
             ?assertMatch({ok,[{field,"title",<<"scarlet">>,[prohibited]}]},
                          parse("-title:scarlet")) end].

prefix_test_() ->
    [fun() ->
             ?assertMatch({ok, [{term, <<"planes">>, [required]}]}, parse("+planes")),
             ?assertMatch({ok, [{term, <<"planes">>, [prohibited]}]}, parse("-planes")),
             ?assertMatch({ok,[{phrase,"\"planes trains\"",
                                {land,[{term,<<"planes">>,[required]},
                                       {term,<<"trains">>,[required]}]}}]},
                          parse("+\"planes trains\"")),
             ?assertMatch({ok,[{phrase,"\"planes trains\"",
                                {land,[{term,<<"planes">>,[prohibited]},
                                       {term,<<"trains">>,[prohibited]}]}}]},
                          parse("-\"planes trains\"")) end].

suffix_test_() ->
    [fun() ->
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.5"}]}]}, parse("solar~")),
             ?assertMatch({ok,[{term,<<"solar">>,[{proximity,5}]}]}, parse("solar~5")),
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.85"}]}]}, parse("solar~0.85")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, 2}]}]}, parse("solar^2")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, 0.9}]}]}, parse("solar^0.9")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{fuzzy,"0.5"}]},
                                       {term,<<"power">>,[{fuzzy,"0.5"}]}]}}]},
                          parse("\"solar power\"~")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{proximity,5}]},
                                       {term,<<"power">>,[{proximity,5}]}]}}]},
                          parse("\"solar power\"~5")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{fuzzy,"0.85"}]},
                                       {term,<<"power">>,[{fuzzy,"0.85"}]}]}}]},
                          parse("\"solar power\"~0.85")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{boost,2}]},
                                       {term,<<"power">>,[{boost,2}]}]}}]},
                          parse("\"solar power\"^2")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{boost,0.9}]},
                                       {term,<<"power">>,[{boost,0.9}]}]}}]},
                          parse("\"solar power\"^0.9")) end].

bool_test_() ->
    [fun() ->
             ?assertMatch({ok,[{land,[{term,<<"fish">>,[]},{term,<<"bicycle">>,[]}]}]},
                          parse("fish AND bicycle")),
             ?assertMatch({ok,[{land,[{field,"acc",{group,[{lnot,[{term,"afa",[]},{term,"aga",[]}]}]}},
                                      {field,"color",<<"oran">>,[prohibited,{wildcard,all}]}]}]},
                          parse("acc:(afa NOT aga) AND -color:oran*")),
             ?assertMatch({ok,[{land,[{field,"acc",
                                       {group,[{land,[{term,"afa",[]},
                                                      {group,[{lnot,[{term,"aga",[]}]}]}]}]}},
                                      {group,[{lnot,[{field,"color","oran*",[{wildcard,all}]}]}]}]}]},
                          parse("acc:(afa AND (NOT aga)) AND (NOT color:oran*)")),
             ?assertMatch({ok,[{land,[{group,[{lor,[{field,"color",<<"re">>,[{wildcard,all}]},
                                                    {field,"color",<<"blub">>,[{fuzzy,"0.5"}]}]}]},
                                      {field,"parity",
                                       [{exclusive_range,{term,"d",[]},{term,"f",[]}}]}]}]},
                          parse("(color:re* OR color:blub~) AND (parity:{d TO f})")),
             ?assertMatch({ok,[{lnot,[{land,[{term,"budweiser",[]},
                                             {term,"beer",[]}]}]}]},
                          parse("NOT budweiser AND beer")),
             ?assertMatch({ok,[{lor,[{term,<<"pizza">>,[]},{term,<<"spaghetti">>,[]}]}]},
                          parse("pizza OR spaghetti")),
             ?assertMatch({ok,[{lor,[{term,<<"basil">>,[]},{term,<<"oregano">>,[]}]}]},
                          parse("basil oregano")),
             ?assertMatch({ok,[{land,[{term,<<"basil">>,[]},{term,<<"oregano">>,[]}]}]},
                          parse("basil oregano", 'and')),
             ?assertMatch({ok,[{land,[{term,<<"fettucini">>,[]},{term,<<"alfredo">>,[]}]}]},
                          parse("fettucini && alfredo", 'and')),
             ?assertMatch({ok,[{lor,[{term,<<"apples">>,[]},{term,<<"oranges">>,[]}]}]},
                          parse("apples oranges", 'or')),
             ?assertMatch({ok,[{land,[{term,<<"french">>,[]},
                                      {lnot,[{term,"fries",[]}]}]}]},
                          parse("french AND NOT fries", 'and')) end].

grouping_test_() ->
    [fun() ->
             ?assertMatch({ok,[{group,[{land,[{term,<<"erlang">>,[]},
                                              {term,<<"sweden">>,[]}]}]}]},
                          parse("(erlang && sweden)")),
             ?assertMatch({ok,[{lor,[{term,<<"broccoli">>,[]},
                                     {group,[{land,[{term,<<"green">>,[]},
                                                    {term,<<"tasty">>,[]}]}]}]}]},
                          parse("broccoli (green AND tasty)")),
             ?assertMatch({ok,[{lor,[{term,<<"broccoli">>,[]},
                                     {group,[{land,[{term,<<"green">>,[]},
                                                    {term,<<"tasty">>,[]}]}]}]}]},
                          parse("broccoli || (green AND tasty)")),
             ?assertMatch({ok, [{term, <<"lisp">>, []}]},
                          parse("((((lisp))))")),
             ?assertMatch({ok,[{land,[{group,[{lor,[{term,<<"jakarta">>,[]},
                                                    {term,<<"apache">>,[]}]}]},
                                      {term,<<"website">>,[]}]}]},
                          parse("(jakarta OR apache) AND website")),
             ?assertMatch({ok,[{field,"title",
                                {group,[{lor,[{term,"python",[required]},
                                              {term,"cookbook",[{proximity,2},required]}]}]}}]},
                          parse("title:(+python +cookbook~2)")),
             ?assertMatch({ok,[{field,"color",{group,[{lor,[{term,"red",[]},
                                                            {term,"blue",[]}]}]}}]},
                          parse("color:(red blue)")),
             ?assertMatch({ok,[{lor,[{group,[{lor,[{term,<<"fuzzy">>,[]},
                                                   {term,<<"wuzzy">>,[]}]}]},
                                     {term,<<"bear">>,[]}]}]},
                          parse("(fuzzy wuzzy) bear")),
             ?assertMatch({ok,[{land,[{term,<<"duck">>,[]},
                                      {group,[{lnot,[{term,"goose",[]}]}]}]}]},
                          parse("duck AND (NOT goose)")),
             ?assertMatch({ok,[{lor,[{group,[{lnot,[{term,"goose",[]}]}]},
                                     {term,<<"duck">>,[]}]}]},
                          parse("(NOT goose) OR duck")),
             ?assertMatch({ok,[{lor,[{group,[{lnot,[{term,"goose",[]}]}]},
                                     {term,<<"duck">>,[]}]}]},
                          parse("(NOT goose) duck")),
             ?assertMatch({ok,[{land,[{term,<<"farm">>,[]},
                                      {group,[{lnot,[{field,"animal","sheep",[]}]}]}]}]},
                          parse("farm AND (NOT animal:sheep)")),
             ?assertMatch({ok,[{lor,[{term,<<"farm">>,[]},
                                     {group,[{lnot,[{field,"animal","sheep",[]}]}]}]}]},
                          parse("farm OR (NOT animal:sheep)")) end].

req_prohib_test_() ->
    [fun() ->
             ?assertMatch({ok, [{field, "product", <<"milk">>, [required]}]},
                          parse("+product:milk")),
             ?assertMatch({ok, [{field, "product", <<"eggs">>, [prohibited]}]},
                          parse("-product:eggs")),
             ?assertMatch({ok,[{field,"product",
                                {group,[{land,[{term,"milk",[required]},
                                               {term,"whole",[prohibited]}]}]}}]},
                          parse("product:(+milk AND -whole)")) end].

field_range_test_() ->
    [fun() ->
             ?assertMatch({ok,[{field,"title",[{inclusive_range,{term,"Aida",[]},
                                                {term,"Carmen",[]}}]}]},
                          parse("title:[Aida TO Carmen]")),
             ?assertMatch({ok,[{field,"title",[{inclusive_range,{term,"Aida",[]},
                                                {term,"Carmen",[]}}]}]},
                          parse("title:[Aida TO Carmen}")),
             ?assertMatch({ok,[{field,"mod_date",
                                 [{exclusive_range,{term,"20020101",[]},
                                   {term,"20030101",[]}}]}]},
                          parse("mod_date:{20020101 TO 20030101}")),
             ?assertMatch({ok,[{field,"mod_date",
                                 [{exclusive_range,{term,"20020101",[]},
                                   {term,"20030101",[]}}]}]},
                          parse("mod_date:{20020101 TO 20030101]")) end].

analysis_trimming_test_() ->
    [fun() ->
             ?assertMatch({ok, [{term, <<"television">>, []}]}, parse("the && television")),
             ?assertMatch({ok, [{land,[{term,<<"pen">>,[]},{term,<<"pad">>,[]}]}]},
                          parse("pen && (a pad)")) end].

%% escaped_chars_gen(Chars) ->
%%     escaped_chars_gen(Chars, []).

%% escaped_chars_gen([], Accum) ->
%%     Accum;
%% escaped_chars_gen([H|T], Accum) ->
%%     Term1 = lists:flatten(["\\", H, "lion"]),
%%     Term2 = lists:flatten(["li\\", H, "on"]),
%%     Term3 = lists:flatten(["lion\\", H]),
%%     F = fun() ->
%%                 ?assertMatch({ok, [{term, Term1, []}]},
%%                              parse(Term1)),
%%                 ?assertMatch({ok, [{term, Term2, []}]},
%%                              parse(Term2)),
%%                 ?assertMatch({ok, [{term, Term3, []}]},
%%                              parse(Term3)) end,
%%     escaped_chars_gen(T, [F|Accum]).
