%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(lucene_scan_test).
-include_lib("eunit/include/eunit.hrl").

%% Simple string parsing.
string1_test() ->
    Expect = [
              {string, 1, "test"}
             ],
    test_helper("test", Expect).

%% Two terms.
string2_test() ->
    Expect = [
              {string, 1, "test1"},
              {string, 1, "test2"}
             ],
    test_helper("test1 test2", Expect).

%% Combinations of quoted and unquoted strings. 
string3_test() ->
    Expect = [
              {string, 1, "test1"},
              {string, 1, "test2"}
             ],
    test_helper("\"test1\" test2", Expect),
    test_helper("'test1' test2", Expect),
    test_helper("test1 \"test2\"", Expect),
    test_helper("test1 'test2'", Expect),
    test_helper("'test1' 'test2'", Expect),
    test_helper("\"test1\" \"test2\"", Expect).

%% Some special characters.
string4_test() ->
    Expect = [
              {string, 1, "test1!+-:'\""},
              {string, 1, "!+-:'\"test2"}
             ],
    test_helper("'test1!+-:\\'\\\"' '!+-:\\'\\\"test2'", Expect),
    test_helper("\"test1!+-:'\\\"\" \"!+-:'\\\"test2\"", Expect),
    test_helper("test1\\!\\+\\-\\:\\'\\\" \\!\\+\\-\\:\\'\\\"test2", Expect).

%% All special characters.
string5_test() ->
    Expect = [
              {string, 1, "test\"' +-!():^[]{}~*?^|\\test"}
             ],
    test_helper("test\\\"\\'\\ \\+\\-\\!\\(\\)\\:\\^\\[\\]\\{\\}\\~*?\\^\\|\\\\test", Expect),
    test_helper("'test\"\\' +-!():^[]{}~*?^|\\\\test'", Expect),
    test_helper("\"test\\\"' +-!():^[]{}~*?^|\\\\test\"", Expect).

%% Spaces and tabs.
string6_test() ->
    Expect = [
              {string, 1, "This is a\ttab."}
             ],
    test_helper("'This is a\ttab.'", Expect),
    test_helper("\"This is a\ttab.\"", Expect).

%% Scopes.
scope_test() ->
    Expect = [
              {string, 1, "scope"},
              {scope, 1, ":"},
              {string, 1, "test"}
             ],
    test_helper("scope:test", Expect),
    test_helper("scope:\"test\"", Expect),
    test_helper("scope:'test'", Expect).

%% Required term, single word.
required1_test() ->
    Expect = [
              {required, 1, "+"},
              {string, 1, "test"}
             ],
    test_helper("+test", Expect),
    test_helper("+'test'", Expect),
    test_helper("+\"test\"", Expect).

%% Required term, multiple words.
required2_test() ->
    Expect = [
              {string, 1, "test1"},
              {required, 1, "+"},
              {string, 1, "test2"}
             ],
    test_helper("test1 +test2", Expect),
    test_helper("test1 +'test2'", Expect),
    test_helper("test1 +\"test2\"", Expect).

%% Prohibited term, single word.
prohibited1_test() ->
    Expect = [
              {prohibited, 1, "-"},
              {string, 1, "test"}
             ],
    test_helper("-test", Expect),
    test_helper("-'test'", Expect),
    test_helper("-\"test\"", Expect).

%% Prohibited term, multiple words.
prohibited2_test() ->
    Expect = [
              {string, 1, "test1"},
              {prohibited, 1, "-"},
              {string, 1, "test2"}
             ],
    test_helper("test1 -test2", Expect),
    test_helper("test1 -'test2'", Expect),
    test_helper("test1 -\"test2\"", Expect).

%% Intersection.
intersection1_test() ->
    Expect = [
              {string, 1, "test1"},
              {intersection, 1, "AND"},
              {string, 1, "test2"}
             ],
    test_helper("test1 AND test2", Expect).

%% Intersection style 2.
intersection2_test() ->
    Expect = [
              {string, 1, "test1"},
              {intersection, 1, "&&"},
              {string, 1, "test2"}
             ],
    test_helper("test1 && test2", Expect).

%% Union.
union1_test() ->
    Expect = [
              {string, 1, "test1"},
              {union, 1, "OR"},
              {string, 1, "test2"}
             ],
    test_helper("test1 OR test2", Expect).

%% Union style 2.
union2_test() ->
    Expect = [
              {string, 1, "test1"},
              {union, 1, "||"},
              {string, 1, "test2"}
             ],
    test_helper("test1 || test2", Expect).
    
%% Negation.
negate1_test() ->
    Expect = [
              {string, 1, "test1"},
              {negation, 1, "NOT"},
              {string, 1, "test2"}
             ],
    test_helper("test1 NOT test2", Expect).

%% Negation style 2.
negation2_test() ->
    Expect = [
              {string, 1, "test1"},
              {negation, 1, "!"},
              {string, 1, "test2"}
             ],
    test_helper("test1 !test2", Expect).

%% Negation a phrase.
negation3_test() ->
    Expect = [
              {negation, 1, "NOT"},
              {string, 1, "test1 test2"}
             ],
    test_helper("NOT 'test1 test2'", Expect).

%% Fuzzy without a value.
fuzzy1_test() ->
    Expect = [
              {string, 1, "test"},
              {fuzzy, 1, 0.5}
             ],
    test_helper("test~", Expect).

%% Fuzzy with a value.
fuzzy2_test() ->
    Expect = [
              {string, 1, "test"},
              {fuzzy, 1, 0.8}
             ],
    test_helper("test~0.8", Expect).

%% Multiple fuzzy values.
fuzzy3_test() ->
    Expect = [
              {string, 1, "test1"},
              {fuzzy, 1, 0.1},
              {string, 1, "test2"},
              {fuzzy, 1, 0.2},
              {string, 1, "test3"},
              {fuzzy, 1, 0.3}
             ],
    test_helper("test1~0.1 test2~0.2 test3~0.3", Expect).

%% Proximity on a single phrase.
proximity1_test() ->
    Expect = [
              {string, 1, "test"},
              {proximity, 1, 5}
             ],
    test_helper("test~5", Expect).

proximity2_test() ->
    Expect = [
              {string, 1, "test1 test2"},
              {proximity, 1, 5}
             ],
    test_helper("'test1 test2'~5", Expect),
    test_helper("\"test1 test2\"~5", Expect).

group1_test() ->
    Expect = [
              {string, 1, "test1"},
              {group_start, 1, "("},
              {string, 1, "test2"},
              {string, 1, "test3"},
              {group_end, 1, ")"}
             ],
    test_helper("test1 (test2 test3)", Expect),
    test_helper("'test1' ('test2' \"test3\")", Expect).

group2_test() ->
    Expect = [
              {group_start, 1, "("},
              {string, 1, "test1"},
              {group_start, 1, "("},
              {string, 1, "test2"},
              {string, 1, "test3"},
              {group_end, 1, ")"},
              {group_end, 1, ")"}
             ],
    test_helper("(test1 (test2 test3))", Expect),
    test_helper("('test1' ('test2' \"test3\"))", Expect).

group3_test() ->
    Expect = [
              {string, 1, "scope"},
              {scope, 1, ":"},
              {group_start, 1, "("},
              {required, 1, "+"},
              {string, 1, "test1"},
              {required, 1, "+"},
              {string, 1, "test2 test3"},
              {group_end, 1, ")"}
             ],
    test_helper("scope:(+test1 +\"test2 test3\")", Expect).

range1_test() ->
    Expect = [
              {inclusive_start, 1, "["},
              {string, 1, "begin"},
              {range_to, 1, "TO"},
              {string, 1, "end"},
              {inclusive_end, 1, "]"}
             ],
    test_helper("[begin TO end]", Expect).

range2_test() ->
    Expect = [
              {string, 1, "scope"},
              {scope, 1, ":"},
              {inclusive_start, 1, "["},
              {string, 1, "begin"},
              {range_to, 1, "TO"},
              {string, 1, "end"},
              {inclusive_end, 1, "]"}
             ],
    test_helper("scope:[begin TO end]", Expect).

range3_test() ->
    Expect = [
              {exclusive_start, 1, "{"},
              {string, 1, "begin"},
              {range_to, 1, "TO"},
              {string, 1, "end"},
              {exclusive_end, 1, "}"}
             ],
    test_helper("{begin TO end}", Expect).

range4_test() ->
    Expect = [
              {string, 1, "scope"},
              {scope, 1, ":"},
              {exclusive_start, 1, "{"},
              {string, 1, "begin"},
              {range_to, 1, "TO"},
              {string, 1, "end"},
              {exclusive_end, 1, "}"}
             ],
    test_helper("scope:{begin TO end}", Expect).

boost1_test() ->
    Expect = [
              {string, 1, "test"},
              {boost, 1, 1}
             ],
    test_helper("test^", Expect).

boost2_test() ->
    Expect = [
              {string, 1, "test0"},
              {boost, 1, 0.5},
              {string, 1, "test1"},
              {boost, 1, 1.0},
              {string, 1, "test2"},
              {boost, 1, 2.0},
              {string, 1, "test 3"},
              {boost, 1, 3.0}
             ],
    test_helper("test0^0.5 test1^1 test2^2.0 \"test 3\"^3.0", Expect),
    test_helper("test0^0.5 test1^ test2^2.0 'test 3'^3.0", Expect).

wildcard1_test() ->
    Expect = [
              {wildcard_char, 1, "test?"}
             ],
    test_helper("test?", Expect).

wildcard2_test() ->
    Expect = [
              {wildcard_glob, 1, "test*"}
             ],
    test_helper("test*", Expect).

wildcard3_test() ->
    Expect = [
              {string, 1, "test\\*"}
             ],
    test_helper("test\\*", Expect).

integer1_test() ->
    Expect = [
              {string, 1, "scope"},
              {scope, 1, ":"},
              {string, 1, "123456789"}
             ],
    test_helper("scope:123456789", Expect).

integer2_test() ->
    Expect = [
              {string, 1, "scope"},
              {scope, 1, ":"},
              {string, 1, "-123456789"}
             ],
    test_helper("scope:\\-123456789", Expect),
    test_helper("scope:'-123456789'", Expect),
    test_helper("scope:\"-123456789\"", Expect).

time1_test() ->
    Expect = [
              {string, 1, "2007-04-05T14:30"}
             ],
    test_helper("2007\\-04\\-05T14\\:30", Expect),
    test_helper("'2007-04-05T14:30'", Expect).

time2_test() ->
    Expect = [
              {string, 1, "scope"},
              {scope, 1, ":"},
              {string, 1, "2007-04-05T14:30"}
             ],
    test_helper("scope:2007\\-04\\-05T14\\:30", Expect),
    test_helper("scope:'2007-04-05T14:30'", Expect),
    test_helper("scope:\"2007-04-05T14:30\"", Expect).
    
test_helper(String, Expect) ->
    {ok, Actual, _} = lucene_scan:string(String),
    Actual == Expect orelse throw({error, String, Actual, Expect}).

