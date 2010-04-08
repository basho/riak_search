-module(testing_parser).

-include_lib("eunit/include/eunit.hrl").

-define(PARSE(X), qilr_parse:string(X)).

-define(GEN_END, {generator, fun() -> [] end}).

escaped_chars_test_() ->
    escaped_chars_gen([$:,$(,$),$[,$],$+,$-,$!,$&,$|,$^,$~,$*,$?]).

multiple_terms_test() ->
    [?assertMatch({ok, [{lor, [{term, "planes", []},
                              {term, "trains", []},
                              {term, "automobiles", []}]}]},
                   ?PARSE("planes trains automobiles"))].

prefix_test() ->
    [?assertMatch({ok, [{term, "planes", [required]}]}, ?PARSE("+planes")),
     ?assertMatch({ok, [{term, "planes", [prohibited]}]}, ?PARSE("-planes")),
     ?assertMatch({ok, [{term, "planes trains", [required]}]}, ?PARSE("+\"planes trains\"")),
     ?assertMatch({ok, [{term, "planes trains", [prohibited]}]}, ?PARSE("-\"planes trains\""))].

suffix_test() ->
    [?assertMatch({ok, [{term, "solar", [{fuzzy, 0.5}]}]}, ?PARSE("solar~")),
     ?assertMatch({ok, [{term, "solar", [{proximity, 5}]}]}, ?PARSE("solar~5")),
     ?assertMatch({ok, [{term, "solar", [{fuzzy, 0.85}]}]}, ?PARSE("solar~0.85")),
     ?assertMatch({ok, [{term, "solar", [{boost, 2}]}]}, ?PARSE("solar^2")),
     ?assertMatch({ok, [{term, "solar", [{boost, 0.9}]}]}, ?PARSE("solar^0.9")),
     ?assertMatch({ok, [{term, "solar power", [{fuzzy, 0.5}]}]}, ?PARSE("\"solar power\"~")),
     ?assertMatch({ok, [{term, "solar power", [{proximity, 5}]}]}, ?PARSE("\"solar power\"~5")),
     ?assertMatch({ok, [{term, "solar power", [{fuzzy, 0.85}]}]}, ?PARSE("\"solar power\"~0.85")),
     ?assertMatch({ok, [{term, "solar power", [{boost, 2}]}]}, ?PARSE("\"solar power\"^2")),
     ?assertMatch({ok, [{term, "solar power", [{boost, 0.9}]}]}, ?PARSE("\"solar power\"^0.9"))].

bool_test() ->
    [?assertMatch({ok, [{land, [{term, "fish", []}, {term, "bicycle", []}]}]},
                  ?PARSE("fish AND bicycle")),
     ?assertMatch({ok, [{land, [{lnot, {term, "budweiser", []}}, {term, "beer", []}]}]},
                  ?PARSE("NOT budweiser AND beer")),
     ?assertMatch({ok, [{lor, [{term, "pizza", []}, {term, "spaghetti", []}]}]},
                  ?PARSE("pizza OR spaghetti")),
     ?assertMatch({ok, [{lor, [{term, "basil", []}, {term, "oregano", []}]}]},
                  ?PARSE("basil oregano"))].

grouping_test() ->
    [?assertMatch({ok, [{group, [{land, [{term, "erlang", []}, {term, "sweden", []}]}]}]},
                  ?PARSE("(erlang && sweden)")),
     ?assertMatch({ok,[{lor,[{term,"broccoli",[]},
                             {group,[{land,[{term,"green",[]},{term,"tasty",[]}]}]}]}]},
                  ?PARSE("broccoli (green AND tasty)")),
     ?assertMatch({ok,[{lor,[{term,"broccoli",[]},
                              {group,[{land,[{term,"green",[]},{term,"tasty",[]}]}]}]}]},
                  ?PARSE("broccoli || (green AND tasty)")),
     ?assertMatch({ok, [{group, [{term, "lisp", []}]}]},
                  ?PARSE("((((lisp))))")),
     ?assertMatch({ok,[{land,[{group,[{lor,[{term,"jakarta",[]},
                                            {term,"apache",[]}]}]},
                              {term,"website",[]}]}]},
                  ?PARSE("(jakarta OR apache) AND website")),
     ?assertMatch({ok, [{field, "title",
                         {group, [{term, "python", [required]},
                                  {term, "cookbook", [{proximity, 2}, required]}]}}]},
                  ?PARSE("title:(+python +cookbook~2)"))].

field_range_test() ->
    [?assertMatch({ok, [{field, "title", [{inclusive_range, {term, "Aida", []}, {term, "Carmen", []}}]}]},
                  ?PARSE("title:[Aida TO Carmen]")),
     ?assertMatch({ok, [{field, "title", [{inclusive_range, {term, "Aida", []}, {term, "Carmen", []}}]}]},
                   ?PARSE("title:[Aida TO Carmen}")),
     ?assertMatch({ok, [{field, "mod_date", [{exclusive_range, {term, "20020101", []}, {term, "20030101", []}}]}]},
                   ?PARSE("mod_date:{20020101 TO 20030101}")),
     ?assertMatch({ok, [{field, "mod_date", [{exclusive_range, {term, "20020101", []}, {term, "20030101", []}}]}]},
                  ?PARSE("mod_date:{20020101 TO 20030101]"))].

escaped_chars_gen([]) ->
    ?GEN_END;
escaped_chars_gen([H|T]) ->
    Term1 = lists:flatten(["\\", H, "lion"]),
    Term2 = lists:flatten(["li\\", H, "on"]),
    Term3 = lists:flatten(["lion\\", H]),
    {generator,
     fun() ->
             [?_test([?assertMatch({ok, [{term, Term1, []}]},
                                   ?PARSE(Term1)),
                      ?assertMatch({ok, [{term, Term2, []}]},
                                   ?PARSE(Term2)),
                      ?assertMatch({ok, [{term, Term3, []}]},
                                   ?PARSE(Term3))]) | escaped_chars_gen(T) ]
     end}.
