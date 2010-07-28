-module(testing_parser).

-include_lib("eunit/include/eunit.hrl").

parse(Query) ->
    parse(Query, 'or').

parse(Query, Bool) ->
    Schema = riak_search_schema:new("search", undefined, "value",
                                    [{riak_search_field, ".*", string, 0,
                                      undefined, false, true, undefined, undefined, false}],
                                    Bool, "com.basho.search.analysis.DefaultAnalyzerFactory"),
    qilr_parse:string(undefined, Query, Schema).

multiple_terms_test_() ->
    [fun() ->
             ?assertMatch({ok,[{lor,[{term,<<"planes">>,[]},
                                     {term,<<"trains">>,[]},
                                     {term,<<"automobiles">>,[]}]}]},
                   parse("planes trains automobiles")) end].

field_test_() ->
    [fun() ->
             ?assertMatch({ok,[{field,"title",{term, <<"peace">>, []},[required]}]},
                          parse("+title:peace")),
             ?assertMatch({ok,[{land,[{lor,[{field,"color",{term,<<"red">>,[]},[]},
                                            {field,"color",{term,<<"blue">>,[]},[]}]},
                                      {field,"acc",{term,<<"aja">>,[]},[]}]}]},
                          parse("(color:red OR color:blue) AND (acc:aja)")),
             ?assertMatch({ok,[{field,"title",{term, <<"scarlet">>, []},[prohibited]}]},
                          parse("-title:scarlet")) end].

prefix_test_() ->
    [fun() ->
             ?assertMatch({ok, [{term, <<"planes">>, [required]}]}, parse("+planes")),
             ?assertMatch({ok, [{term, <<"planes">>, [prohibited]}]}, parse("-planes")),
             ?assertMatch({ok,[{phrase,<<"\"planes trains\"">>,
                               [{base_query,[{land,[{term,<<"planes">>,[required]},
                                                    {term,<<"trains">>,[required]}]}]},
                                required]}]},
                          parse("+\"planes trains\"")),
             ?assertMatch({ok,[{phrase,<<"\"planes trains\"">>,
                                [{base_query, [{land, [{term, <<"planes">>, [prohibited]},
                                                       {term, <<"trains">>, [prohibited]}]}]},
                                  prohibited]}]},
                          parse("-\"planes trains\"")) end].

suffix_test_() ->
    [fun() ->
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.5"}]}]}, parse("solar~")),
             ?assertMatch({ok,[{term,<<"solar">>,[{proximity,"5"}]}]}, parse("solar~5")),
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.85"}]}]}, parse("solar~0.85")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, "2"}]}]}, parse("solar^2")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, "0.9"}]}]}, parse("solar^0.9")),
             ?assertMatch({ok,[{phrase, <<"\"solar power\"">>,
                                [{base_query, [{land, [{term, <<"solar">>, [{fuzzy, "0.5"}]},
                                                       {term, <<"power">>, [{fuzzy, "0.5"}]}]}]},
                                 {fuzzy, "0.5"}]}]},
                          parse("\"solar power\"~")),
             ?assertMatch({ok,[{phrase,<<"\"solar power\"">>,
                                [{base_query, [{land, [{term, <<"solar">>, []},
                                                       {term, <<"power">>, []}]}]},
                                 {proximity_terms,[<<"solar">>,<<"power">>]},
                                 {proximity,"5"}]}]},
                          parse("\"solar power\"~5")),
             ?assertMatch({ok,[{phrase,<<"\"solar power\"">>,
                                [{base_query,[{land,[{term,<<"solar">>,[{fuzzy,"0.85"}]},
                                                     {term,<<"power">>,[{fuzzy,"0.85"}]}]}]},
                                 {fuzzy,"0.85"}]}]},
                          parse("\"solar power\"~0.85")),
             ?assertMatch({ok,[{phrase,<<"\"solar power\"">>,
                                [{base_query, [{land, [{term, <<"solar">>, [{boost, "2"}]},
                                                       {term, <<"power">>, [{boost, "2"}]}]}]},
                                {boost, "2"}]}]},
                           parse("\"solar power\"^2")),
             ?assertMatch({ok,[{phrase,<<"\"solar power\"">>,
                                [{base_query, [{land, [{term, <<"solar">>, [{boost, "0.9"}]},
                                                       {term, <<"power">>, [{boost, "0.9"}]}]}]},
                                {boost, "0.9"}]}]},
                           parse("\"solar power\"^0.9")) end].

bool_test_() ->
    [fun() ->
             ?assertMatch({ok,[{land,[{term,<<"fish">>,[]},{term,<<"bicycle">>,[]}]}]},
                          parse("fish AND bicycle")),
             ?assertMatch({ok,[{field,"acc",
                                [{lor,[{term,<<"afa">>,[]},{lnot,[{term,<<"aga">>,[]}]}]}],
                                []}]},
                          parse("acc:(afa NOT aga)")),
              ?assertMatch({ok,[{land,[{field,"acc",
                                        [{land,[{term,<<"afa">>,[]},{lnot,[{term,<<"aga">>,[]}]}]}],
                                        []},
                                       {lnot,[{field,"color",
                                               {term,<<"oran">>,[{wildcard,all}]},
                                               []}]}]}]},
                           parse("acc:(afa AND (NOT aga)) AND (NOT color:oran*)")),
             ?assertMatch({ok,[{land,[{lor,[{field,"color",
                                             {term,<<"re">>,[{wildcard,all}]},
                                             []},
                                            {field,"color",{term,<<"blub">>,[{fuzzy,"0.5"}]},[]}]},
                                      {field,"parity",
                                       [{exclusive_range,{term,<<"100">>,[]},{term,<<"105">>,[]}}],
                                       []}]}]},
                          parse("(color:re* OR color:blub~) AND (parity:{100 TO 105})")),
             ?assertMatch({ok,[{lor,[{term,<<"flag">>,[]},
                                     {field,"color",
                                      [{lor,[{term,<<"red">>,[]},{term,<<"blue">>,[]}]}],
                                      []}]}]},
                         parse("flag OR color:(red blue)")),
             ?assertMatch({ok,[{land,[{lnot,[{term,<<"budweiser">>,[]}]},
                                      {term,<<"beer">>,[]}]}]},
                          parse("NOT budweiser AND beer")),
             ?assertMatch({ok,[{lor,[{term,<<"pizza">>,[]},{term,<<"spaghetti">>,[]}]}]},
                           parse("pizza OR spaghetti")),
             ?assertMatch({ok,[{lor,[{term,<<"basil">>,[]},{term,<<"oregano">>,[]}]}]},
                          parse("basil oregano")),
             ?assertMatch({ok,[{land,[{term,<<"fettucini">>,[]},{term,<<"alfredo">>,[]}]}]},
                          parse("fettucini && alfredo", 'and')),
             ?assertMatch({ok,[{lor,[{term,<<"apples">>,[]},{term,<<"oranges">>,[]}]}]},
                          parse("apples oranges", 'or')),
             ?assertMatch({ok,[{land,[{term,<<"french">>,[]},
                                      {lnot,[{term,<<"fries">>,[]}]}]}]},
                          parse("french AND NOT fries", 'and')) end].

grouping_test_() ->
    [fun() ->
             ?assertMatch({ok,[{land,[{term,<<"erlang">>,[]},
                                      {term,<<"sweden">>,[]}]}]},
                          parse("(erlang && sweden)")),
             ?assertMatch({ok,[{lor,[{term,<<"broccoli">>,[]},
                                      {land,[{term,<<"green">>,[]},
                                             {term,<<"tasty">>,[]}]}]}]},
                           parse("broccoli (green AND tasty)")),
             ?assertMatch({ok,[{lor,[{term,<<"broccoli">>,[]},
                                     {land,[{term,<<"green">>,[]},
                                            {term,<<"tasty">>,[]}]}]}]},
                          parse("broccoli || (green AND tasty)")),
             ?assertMatch({ok, [{term, <<"lisp">>, []}]},
                          parse("((((lisp))))")),
             ?assertMatch({ok,[{land,[{lor,[{term,<<"jakarta">>,[]},
                                            {term,<<"apache">>,[]}]},
                                      {term,<<"website">>,[]}]}]},
                          parse("(jakarta OR apache) AND website")),
             ?assertMatch({ok,[{field,"title",
                                [{lor,[{term,<<"python">>,[required]},
                                       {term,<<"cookbook">>,[required, {proximity,"2"}]}]}],
                                []}]},
                          parse("title:(+python +cookbook~2)")),
             ?assertMatch({ok,[{field,"color",[{lor,[{term,<<"red">>,[]},
                                                            {term,<<"blue">>,[]}]}],
                                []}]},
                          parse("color:(red blue)")),
             ?assertMatch({ok,[{lor,[{term,<<"fuzzy">>,[]},
                                     {term,<<"wuzzy">>,[]},
                                     {term,<<"bear">>,[]}]}]},
                          parse("(fuzzy wuzzy) bear")),
             ?assertMatch({ok,[{land,[{term,<<"duck">>,[]},
                                      {lnot,[{term,<<"goose">>,[]}]}]}]},
                          parse("duck AND (NOT goose)")),
             ?assertMatch({ok,[{lor,[{lnot,[{term,<<"goose">>,[]}]},
                                     {term,<<"duck">>,[]}]}]},
                          parse("(NOT goose) OR duck")),
             ?assertMatch({ok,[{lor,[{lnot,[{term,<<"goose">>,[]}]},
                                     {term,<<"duck">>,[]}]}]},
                          parse("(NOT goose) duck")),
             ?assertMatch({ok,[{land,[{term,<<"farm">>,[]},
                                      {lnot,[{field,"animal",{term,<<"sheep">>,[]},[]}]}]}]},
                          parse("farm AND (NOT animal:sheep)")),
             ?assertMatch({ok,[{lor,[{term,<<"farm">>,[]},
                                     {lnot,[{field,"animal",{term,<<"sheep">>,[]}, []}]}]}]},
                          parse("farm OR (NOT animal:sheep)")) end].

req_prohib_test_() ->
    [fun() ->
             ?assertMatch({ok,[{field,"product",{term,<<"milk">>,[]},[required]}]},
                          parse("+product:milk")),
             ?assertMatch({ok,[{field,"product",{term,<<"eggs">>,[]},[prohibited]}]},
                          parse("-product:eggs")),
             ?assertMatch({ok,[{field,"product",
                                [{land,[{term,<<"milk">>,[required]},
                                        {term,<<"whole">>,[prohibited]}]}],
                               []}]},
                          parse("product:(+milk AND -whole)")) end].

field_range_test_() ->
    [fun() ->
             ?assertMatch({ok,[{field,"title",[{inclusive_range,{term,<<"Aida">>,[]},
                                                {term,<<"Carmen">>,[]}}],
                                []}]},
                          parse("title:[Aida TO Carmen]")),
             ?assertMatch({ok,[{field,"title",[{inclusive_range,{term,<<"Aida">>,[]},
                                                {term,<<"Carmen">>,[]}}],
                                []}]},
                          parse("title:[Aida TO Carmen}")),
             ?assertMatch({ok,[{field,"mod_date",
                                 [{exclusive_range,{term,<<"20020101">>,[]},
                                   {term,<<"20030101">>,[]}}],
                                []}]},
                          parse("mod_date:{20020101 TO 20030101}")),
             ?assertMatch({ok,[{field,"mod_date",
                                 [{exclusive_range,{term,<<"20020101">>,[]},
                                   {term,<<"20030101">>,[]}}],
                                []}]},
                          parse("mod_date:{20020101 TO 20030101]")) end].

%% analysis_trimming_test_() ->
%%     [fun() ->
%%              ?assertMatch({ok, [{term, <<"television">>, []}]}, parse("the && television")),
%%              ?assertMatch({ok, [{land,[{term,<<"pen">>,[]},{term,<<"pad">>,[]}]}]},
%%                           parse("pen && (a pad)")) end].

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
