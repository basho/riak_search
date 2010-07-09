-module(testing_parser).

-include_lib("eunit/include/eunit.hrl").



parse(AnalyzerPid, Query) ->
    parse(AnalyzerPid, Query, 'or').

parse(AnalyzerPid, Query, Bool) ->
    Schema = riak_search_schema:new("search", undefined, "value", [],
        Bool, "com.basho.search.analysis.DefaultAnalyzerFactory"),
    qilr_parse:string(AnalyzerPid, Query, Schema).

multiple_terms_test_() ->
    [fun() ->
             application:start(qilr),
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok,[{lor,[{term,<<"planes">>,[]},
                                     {term,<<"trains">>,[]},
                                     {term,<<"automobiles">>,[]}]}]},
                   parse(AnalyzerPid, "planes trains automobiles")) end].

field_test_() ->
    [fun() ->
             application:start(qilr),             
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok,[{field,"title",<<"peace">>,[required]}]},
                          parse(AnalyzerPid, "+title:peace")),
             ?assertMatch({ok,[{field,"title",<<"scarlet">>,[prohibited]}]},
                          parse(AnalyzerPid, "-title:scarlet")) end].

prefix_test_() ->
    [fun() ->
             application:start(qilr),
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok, [{term, <<"planes">>, [required]}]}, parse(AnalyzerPid, "+planes")),
             ?assertMatch({ok, [{term, <<"planes">>, [prohibited]}]}, parse(AnalyzerPid, "-planes")),
             ?assertMatch({ok,[{phrase,"\"planes trains\"",
                                {land,[{term,<<"planes">>,[required]},
                                       {term,<<"trains">>,[required]}]}}]},
                          parse(AnalyzerPid, "+\"planes trains\"")),
             ?assertMatch({ok,[{phrase,"\"planes trains\"",
                                {land,[{term,<<"planes">>,[prohibited]},
                                       {term,<<"trains">>,[prohibited]}]}}]},
                          parse(AnalyzerPid, "-\"planes trains\"")) end].

suffix_test_() ->
    [fun() ->
             application:start(qilr),             
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.5"}]}]}, parse(AnalyzerPid, "solar~")),
             ?assertMatch({ok,[{term,<<"solar">>,[{proximity,5}]}]}, parse(AnalyzerPid, "solar~5")),
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.85"}]}]}, parse(AnalyzerPid, "solar~0.85")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, 2}]}]}, parse(AnalyzerPid, "solar^2")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, 0.9}]}]}, parse(AnalyzerPid, "solar^0.9")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{fuzzy,"0.5"}]},
                                       {term,<<"power">>,[{fuzzy,"0.5"}]}]}}]},
                          parse(AnalyzerPid, "\"solar power\"~")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{proximity,5}]},
                                       {term,<<"power">>,[{proximity,5}]}]}}]},
                          parse(AnalyzerPid, "\"solar power\"~5")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{fuzzy,"0.85"}]},
                                       {term,<<"power">>,[{fuzzy,"0.85"}]}]}}]},
                          parse(AnalyzerPid, "\"solar power\"~0.85")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{boost,2}]},
                                       {term,<<"power">>,[{boost,2}]}]}}]},
                          parse(AnalyzerPid, "\"solar power\"^2")),
             ?assertMatch({ok,[{phrase,"\"solar power\"",
                                {land,[{term,<<"solar">>,[{boost,0.9}]},
                                       {term,<<"power">>,[{boost,0.9}]}]}}]},
                          parse(AnalyzerPid, "\"solar power\"^0.9")) end].

bool_test_() ->
    [fun() ->
             application:start(qilr),
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok,[{land,[{term,<<"fish">>,[]},{term,<<"bicycle">>,[]}]}]},
                          parse(AnalyzerPid, "fish AND bicycle")),
             ?assertMatch({ok,[{lnot,[{land,[{term,"budweiser",[]},
                                             {term,"beer",[]}]}]}]},
                          parse(AnalyzerPid, "NOT budweiser AND beer")),
             ?assertMatch({ok,[{lor,[{term,<<"pizza">>,[]},{term,<<"spaghetti">>,[]}]}]},
                          parse(AnalyzerPid, "pizza OR spaghetti")),
             ?assertMatch({ok,[{lor,[{term,<<"basil">>,[]},{term,<<"oregano">>,[]}]}]},
                          parse(AnalyzerPid, "basil oregano")),
             ?assertMatch({ok,[{land,[{term,<<"basil">>,[]},{term,<<"oregano">>,[]}]}]},
                          parse(AnalyzerPid, "basil oregano", 'and')),
             ?assertMatch({ok,[{land,[{term,<<"fettucini">>,[]},{term,<<"alfredo">>,[]}]}]},
                          parse(AnalyzerPid, "fettucini && alfredo", 'and')),
             ?assertMatch({ok,[{lor,[{term,<<"apples">>,[]},{term,<<"oranges">>,[]}]}]},
                          parse(AnalyzerPid, "apples oranges", 'or')),
             ?assertMatch({ok,[{land,[{term,<<"french">>,[]},
                                      {lnot,[{term,"fries",[]}]}]}]},
                          parse(AnalyzerPid, "french AND NOT fries", 'and')) end].

grouping_test_() ->
    [fun() ->
             application:start(qilr),
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok,[{group,[{land,[{term,<<"erlang">>,[]},
                                              {term,<<"sweden">>,[]}]}]}]},
                          parse(AnalyzerPid, "(erlang && sweden)")),
             ?assertMatch({ok,[{lor,[{term,<<"broccoli">>,[]},
                                     {group,[{land,[{term,<<"green">>,[]},
                                                    {term,<<"tasty">>,[]}]}]}]}]},
                          parse(AnalyzerPid, "broccoli (green AND tasty)")),
             ?assertMatch({ok,[{lor,[{term,<<"broccoli">>,[]},
                                     {group,[{land,[{term,<<"green">>,[]},
                                                    {term,<<"tasty">>,[]}]}]}]}]},
                          parse(AnalyzerPid, "broccoli || (green AND tasty)")),
             ?assertMatch({ok, [{term, <<"lisp">>, []}]},
                          parse(AnalyzerPid, "((((lisp))))")),
             ?assertMatch({ok,[{land,[{group,[{lor,[{term,<<"jakarta">>,[]},
                                                    {term,<<"apache">>,[]}]}]},
                                      {term,<<"website">>,[]}]}]},
                          parse(AnalyzerPid, "(jakarta OR apache) AND website")),
             ?assertMatch({ok,[{field,"title",
                                {group,[{lor,[{term,"python",[required]},
                                              {term,"cookbook",[{proximity,2},required]}]}]}}]},
                          parse(AnalyzerPid, "title:(+python +cookbook~2)")),
             ?assertMatch({ok,[{field,"color",{group,[{lor,[{term,"red",[]},
                                                            {term,"blue",[]}]}]}}]},
                          parse(AnalyzerPid, "color:(red blue)")),
             ?assertMatch({ok,[{lor,[{group,[{lor,[{term,<<"fuzzy">>,[]},
                                                   {term,<<"wuzzy">>,[]}]}]},
                                     {term,<<"bear">>,[]}]}]},
                          parse(AnalyzerPid, "(fuzzy wuzzy) bear")),
             ?assertMatch({ok,[{land,[{term,<<"duck">>,[]},
                                      {group,[{lnot,[{term,"goose",[]}]}]}]}]},
                          parse(AnalyzerPid, "duck AND (NOT goose)")),
             ?assertMatch({ok,[{lor,[{group,[{lnot,[{term,"goose",[]}]}]},
                                     {term,<<"duck">>,[]}]}]},
                          parse(AnalyzerPid, "(NOT goose) OR duck")),
             ?assertMatch({ok,[{lor,[{group,[{lnot,[{term,"goose",[]}]}]},
                                     {term,<<"duck">>,[]}]}]},
                          parse(AnalyzerPid, "(NOT goose) duck")),
             ?assertMatch({ok,[{land,[{term,<<"farm">>,[]},
                                      {group,[{lnot,[{field,"animal","sheep",[]}]}]}]}]},
                          parse(AnalyzerPid, "farm AND (NOT animal:sheep)")),
             ?assertMatch({ok,[{lor,[{term,<<"farm">>,[]},
                                     {group,[{lnot,[{field,"animal","sheep",[]}]}]}]}]},
                          parse(AnalyzerPid, "farm OR (NOT animal:sheep)")) end].

req_prohib_test_() ->
    [fun() ->
             application:start(qilr),
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok, [{field, "product", <<"milk">>, [required]}]},
                          parse(AnalyzerPid, "+product:milk")),
             ?assertMatch({ok, [{field, "product", <<"eggs">>, [prohibited]}]},
                          parse(AnalyzerPid, "-product:eggs")),
             ?assertMatch({ok,[{field,"product",
                                {group,[{land,[{term,"milk",[required]},
                                               {term,"whole",[prohibited]}]}]}}]},
                          parse(AnalyzerPid, "product:(+milk AND -whole)")) end].

field_range_test_() ->
    [fun() ->
             application:start(qilr),
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok,[{field,"title",[{inclusive_range,{term,"Aida",[]},
                                                {term,"Carmen",[]}}]}]},
                          parse(AnalyzerPid, "title:[Aida TO Carmen]")),
             ?assertMatch({ok,[{field,"title",[{inclusive_range,{term,"Aida",[]},
                                                {term,"Carmen",[]}}]}]},
                          parse(AnalyzerPid, "title:[Aida TO Carmen}")),
             ?assertMatch({ok,[{field,"mod_date",
                                 [{exclusive_range,{term,"20020101",[]},
                                   {term,"20030101",[]}}]}]},
                          parse(AnalyzerPid, "mod_date:{20020101 TO 20030101}")),
             ?assertMatch({ok,[{field,"mod_date",
                                 [{exclusive_range,{term,"20020101",[]},
                                   {term,"20030101",[]}}]}]},
                          parse(AnalyzerPid, "mod_date:{20020101 TO 20030101]")) end].

analysis_trimming_test_() ->
    [fun() ->
             application:start(qilr),
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_conn(),
             ?assertMatch({ok, [{term, <<"television">>, []}]}, parse(AnalyzerPid, "the && television")),
             ?assertMatch({ok, [{land,[{term,<<"pen">>,[]},{term,<<"pad">>,[]}]}]},
                          parse(AnalyzerPid, "pen && (a pad)")) end].

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
