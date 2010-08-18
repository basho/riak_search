-module(testing_parser).

-include_lib("eunit/include/eunit.hrl").

parse(Query) ->
    parse(Query, 'or').

parse(Query, Bool) ->
    Schema = riak_search_schema:new("search", undefined, "value", "id",
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
             ?assertMatch({ok,[{lnot,[{field,"title",{term, <<"scarlet">>, []}, []}]}]},
                          parse("-title:scarlet")) end].

prefix_test_() ->
    [fun() ->
             ?assertMatch({ok, [{term, <<"planes">>, [required]}]}, parse("+planes")),
             ?assertMatch({ok, [{lnot, [{term, <<"planes">>, []}]}]}, parse("-planes")),
             ?assertMatch({ok,[{phrase,<<"\"planes trains\"">>,
                               [{base_query,[{land,[{term,<<"planes">>,[required]},
                                                    {term,<<"trains">>,[required]}]}]},
                                required]}]},
                          parse("+\"planes trains\"")),
             ?assertMatch({ok,[{phrase,<<"\"planes trains\"">>,
                                [{base_query, [{land, [{lnot, [{term, <<"planes">>, []}]},
                                                       {lnot, [{term, <<"trains">>, []}]}]}]},
                                  prohibited]}]},
                          parse("-\"planes trains\"")) end].

suffix_test_() ->
    [fun() ->
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.5"}]}]}, parse("solar~")),
             ?assertMatch({ok,[{term,<<"solar">>,[{proximity,"5"}]}]}, parse("solar~5")),
             ?assertMatch({ok,[{term,<<"solar">>,[{fuzzy,"0.85"}]}]}, parse("solar~0.85")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, "2"}]}]}, parse("solar^2")),
             ?assertMatch({ok, [{term, <<"solar">>, [{boost, "0.9"}]}]}, parse("solar^0.9")),
             ?assertMatch({ok, [{term, <<"solar">>, [{wildcard, all}]}]}, parse("solar*")),
             ?assertMatch({ok, [{term, <<"solar">>, [{wildcard, one}]}]}, parse("solar?")),
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
                                             {term,<<"yell">>,[{wildcard,all}]},
                                             []},
                                            {field,"color",{term,<<"blub">>,[{fuzzy,"0.5"}]},[]}]},
                                      {field,"parity",
                                       {exclusive_range,<<"100">>, <<"105">>}, []}]}]},
                          parse("(color:yell* OR color:blub~) AND (parity:{100 TO 105})")),
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
             ?assertMatch({ok,[{lnot, [{field,"product",{term,<<"eggs">>,[]},[]}]}]},
                          parse("-product:eggs")),
             ?assertMatch({ok,[{field,"product",
                                [{land,[{term,<<"milk">>,[required]},
                                        {lnot, [{term,<<"whole">>,[]}]}]}],
                               []}]},
                          parse("product:(+milk AND -whole)")) end].

field_range_test_() ->
    [fun() ->
             ?assertMatch({ok,[{field,"title",{inclusive_range, <<"Aida">>, <<"Carmen">>}, []}]},
                          parse("title:[Aida TO Carmen]")),
             ?assertMatch({ok,[{field,"title",{inclusive_range, <<"Aida">>, <<"Carmen">>}, []}]},
                          parse("title:[Aida TO Carmen}")),
             ?assertMatch({ok,[{field,"mod_date", {exclusive_range, <<"20020101">>, <<"20030101">>}, []}]},
                          parse("mod_date:{20020101 TO 20030101}")),
             ?assertMatch({ok,[{field,"mod_date", {exclusive_range, <<"20020101">>, <<"20030101">>}, []}]},
                          parse("mod_date:{20020101 TO 20030101]")) end].

analysis_trimming_test_() ->
    [fun() ->
             ?assertMatch({ok, [{term, <<"television">>, []}]}, parse("a && television")),
             ?assertMatch({ok, [{land,[{term,<<"pen">>,[]},{term,<<"pad">>,[]}]}]},
                          parse("pen && (a && pad)")) end].
