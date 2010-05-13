-module(testing_parser).

-include_lib("eunit/include/eunit.hrl").

-define(PARSE(AnalyzerPid, X), qilr_parse:string(AnalyzerPid, X)).
-define(BOOL_PARSE(AnalyzerPid, Term, Bool), qilr_parse:string(AnalyzerPid, Term, Bool)).

-define(GEN_END, {generator, fun() -> [] end}).

%% escaped_chars_test_() ->
%%     escaped_chars_gen([$:,$(,$),$[,$],$+,$-,$!,$&,$|,$^,$~,$*,$?]).

multiple_terms_test_() ->
    [fun() ->
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
             ?assertMatch({ok, [{lor, [{term, "planes", []},
                                       {term, "trains", []},
                                       {term, "automobiles", []}]}]},
                   ?PARSE(AnalyzerPid, "planes trains automobiles")) end].

prefix_test_() ->
    [fun() ->
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
             ?assertMatch({ok, [{term, "planes", [required]}]}, ?PARSE(AnalyzerPid, "+planes")),
             ?assertMatch({ok, [{term, "planes", [prohibited]}]}, ?PARSE(AnalyzerPid, "-planes")),
             ?assertMatch({ok, [{group, [{land,
                                          [{term, "planes", [required]},
                                           {term, "trains", [required]}]}]}]},
                          ?PARSE(AnalyzerPid, "+\"planes trains\"")),
             ?assertMatch({ok, [{group, [{land,
                                          [{term, "planes", [prohibited]},
                                           {term, "trains", [prohibited]}]}]}]},
                          ?PARSE(AnalyzerPid, "-\"planes trains\"")) end].

suffix_test_() ->
    [fun() ->
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
             ?assertMatch({ok, [{term, "solar", [{fuzzy, 0.5}]}]}, ?PARSE(AnalyzerPid, "solar~")),
             ?assertMatch({ok, [{term, "solar", [{proximity, 5}]}]}, ?PARSE(AnalyzerPid, "solar~5")),
             ?assertMatch({ok, [{term, "solar", [{fuzzy, 0.85}]}]}, ?PARSE(AnalyzerPid, "solar~0.85")),
             ?assertMatch({ok, [{term, "solar", [{boost, 2}]}]}, ?PARSE(AnalyzerPid, "solar^2")),
             ?assertMatch({ok, [{term, "solar", [{boost, 0.9}]}]}, ?PARSE(AnalyzerPid, "solar^0.9")),
             ?assertMatch({ok,[{group,
                                [{land,
                                  [{term,"solar",[{fuzzy,0.5}]},
                                   {term,"power",[{fuzzy,0.5}]}]}]}]},
                          ?PARSE(AnalyzerPid, "\"solar power\"~")),
             ?assertMatch({ok,[{group,
                                [{land,
                                  [{term,"solar",[{proximity, 5}]},
                                   {term,"power",[{proximity, 5}]}]}]}]},
                          ?PARSE(AnalyzerPid, "\"solar power\"~5")),
             ?assertMatch({ok,[{group,
                                [{land,
                                  [{term,"solar",[{fuzzy, 0.85}]},
                                   {term,"power",[{fuzzy, 0.85}]}]}]}]},
                          ?PARSE(AnalyzerPid, "\"solar power\"~0.85")),
             ?assertMatch({ok,[{group,
                                [{land,
                                  [{term,"solar",[{boost, 2}]},
                                   {term,"power",[{boost, 2}]}]}]}]},
                          ?PARSE(AnalyzerPid, "\"solar power\"^2")),
             ?assertMatch({ok,[{group,
                                [{land,
                                  [{term,"solar",[{boost, 0.9}]},
                                   {term,"power",[{boost, 0.9}]}]}]}]},
                          ?PARSE(AnalyzerPid, "\"solar power\"^0.9")) end].

bool_test_() ->
    [fun() ->
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
             ?assertMatch({ok, [{land, [{term, "fish", []}, {term, "bicycle", []}]}]},
                          ?PARSE(AnalyzerPid, "fish AND bicycle")),
             ?assertMatch({ok, [{land,[{lnot,[{term,"budweiser",[]}]},
                                       {term,"beer",[]}]}]},
                          ?PARSE(AnalyzerPid, "NOT budweiser AND beer")),
             ?assertMatch({ok, [{lor, [{term, "pizza", []}, {term, "spaghetti", []}]}]},
                          ?PARSE(AnalyzerPid, "pizza OR spaghetti")),
             ?assertMatch({ok, [{lor, [{term, "basil", []}, {term, "oregano", []}]}]},
                          ?PARSE(AnalyzerPid, "basil oregano")),
             ?assertMatch({ok, [{land, [{term, "basil", []}, {term, "oregano", []}]}]},
                          ?BOOL_PARSE(AnalyzerPid, "basil oregano", 'and')),
             ?assertMatch({ok, [{land, [{term, "fettucini", []}, {term, "alfredo", []}]}]},
                          ?BOOL_PARSE(AnalyzerPid, "fettucini && alfredo", 'and')),
             ?assertMatch({ok, [{lor, [{term, "apples", []}, {term, "oranges", []}]}]},
                          ?BOOL_PARSE(AnalyzerPid, "apples oranges", 'or')) end].

grouping_test_() ->
    [fun() ->
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
             ?assertMatch({ok, [{group, [{land,[{term,"erlang",[]},{term,"sweden",[]}]}]}]},
                          ?PARSE(AnalyzerPid, "(erlang && sweden)")),
             ?assertMatch({ok,[{lor,[{term,"broccoli",[]},
                                     {group,[{land,[{term,"green",[]},{term,"tasty",[]}]}]}]}]},
                          ?PARSE(AnalyzerPid, "broccoli (green AND tasty)")),
             ?assertMatch({ok,[{lor,[{term,"broccoli",[]},
                                     {group,[{land,[{term,"green",[]},{term,"tasty",[]}]}]}]}]},
                          ?PARSE(AnalyzerPid, "broccoli || (green AND tasty)")),
             ?assertMatch({ok, [{term, "lisp", []}]},
                          ?PARSE(AnalyzerPid, "((((lisp))))")),
             ?assertMatch({ok,[{land,[{group,[{lor,[{term,"jakarta",[]},
                                                    {term,"apache",[]}]}]},
                                      {term,"website",[]}]}]},
                          ?PARSE(AnalyzerPid, "(jakarta OR apache) AND website")),
             ?assertMatch({ok, [{field, "title",
                                 {group, [{term, "python", [required]},
                                          {term, "cookbook", [{proximity, 2}, required]}]}}]},
                          ?PARSE(AnalyzerPid, "title:(+python +cookbook~2)")) end].

field_range_test_() ->
    [fun() ->
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
             ?assertMatch({ok, [{field, "title", [{inclusive_range, {term, "Aida", []}, {term, "Carmen", []}}]}]},
                          ?PARSE(AnalyzerPid, "title:[Aida TO Carmen]")),
             ?assertMatch({ok, [{field, "title", [{inclusive_range, {term, "Aida", []}, {term, "Carmen", []}}]}]},
                          ?PARSE(AnalyzerPid, "title:[Aida TO Carmen}")),
             ?assertMatch({ok, [{field, "mod_date", [{exclusive_range, {term, "20020101", []}, {term, "20030101", []}}]}]},
                          ?PARSE(AnalyzerPid, "mod_date:{20020101 TO 20030101}")),
             ?assertMatch({ok, [{field, "mod_date", [{exclusive_range, {term, "20020101", []}, {term, "20030101", []}}]}]},
                          ?PARSE(AnalyzerPid, "mod_date:{20020101 TO 20030101]")) end].

analysis_trimming_test_() ->
    [fun() ->
             {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
             ?assertMatch({ok, [{term, "television", []}]}, ?PARSE(AnalyzerPid, "the && television")),
             ?assertMatch({ok, [{land,[{term,"pen",[]},{term,"pad",[]}]}]}, ?PARSE(AnalyzerPid, "pen && (a pad)")) end].

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
%%                              ?PARSE(Term1)),
%%                 ?assertMatch({ok, [{term, Term2, []}]},
%%                              ?PARSE(Term2)),
%%                 ?assertMatch({ok, [{term, Term3, []}]},
%%                              ?PARSE(Term3)) end,
%%     escaped_chars_gen(T, [F|Accum]).
