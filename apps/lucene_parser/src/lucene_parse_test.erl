%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(lucene_parse_test).
-include_lib("eunit/include/eunit.hrl").
-include("lucene_parser.hrl").

%% Simple string parsing.
string1_test() ->
    Expect = [
        #string { s="test" }
    ],
    test_helper("test", Expect).

%% Two terms.
string2_test() ->
    Expect = [
        #string { s="test1"},
        #string { s="test2"}
    ],
    test_helper("test1 test2", Expect).

%% Combinations of quoted and unquoted strings. 
string3_test() ->
    Expect = [
        #string { s="test1"}, 
        #string { s="test2"}
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
        #string { s="test1!+-:'\"" },
        #string { s="!+-:'\"test2"}
    ],
    test_helper("'test1!+-:\\'\\\"' '!+-:\\'\\\"test2'", Expect),
    test_helper("\"test1!+-:'\\\"\" \"!+-:'\\\"test2\"", Expect),
    test_helper("test1\\!\\+\\-\\:\\'\\\" \\!\\+\\-\\:\\'\\\"test2", Expect).

%% All special characters.
string5_test() ->
    Expect = [
        #string { s="test\"' +-!():^[]{}~*?^|\\test"}
    ],
    test_helper("test\\\"\\'\\ \\+\\-\\!\\(\\)\\:\\^\\[\\]\\{\\}\\~*?\\^\\|\\\\test", Expect),
    test_helper("'test\"\\' +-!():^[]{}~*?^|\\\\test'", Expect),
    test_helper("\"test\\\"' +-!():^[]{}~*?^|\\\\test\"", Expect).

%% Spaces and tabs.
string6_test() ->
    Expect = [
        #string { s="This is a\ttab."}
    ],
    test_helper("'This is a\ttab.'", Expect),
    test_helper("\"This is a\ttab.\"", Expect).

%% Fields.
field_test() ->
    Expect = [
        #scope {
            field="field",
            ops=[#string { s="test" }]
        }
    ],
    test_helper("field:test", Expect),
    test_helper("field:\"test\"", Expect),
    test_helper("field:'test'", Expect).

%% Required term, single word.
required1_test() ->
    Expect = [
        #string { s="test", flags=[required] }
    ],
    test_helper("+test", Expect),
    test_helper("+'test'", Expect),
    test_helper("+\"test\"", Expect).

%% Required term, multiple words.
required2_test() ->
    Expect = [
        #string { s="test1" },
        #string { s="test2", flags=[required] }
    ],
    test_helper("test1 +test2", Expect),
    test_helper("test1 +'test2'", Expect),
    test_helper("test1 +\"test2\"", Expect).

%% Prohibited term, single word.
prohibited1_test() ->
    Expect = [
        #negation { op=#string { s="test" } }
    ],
    test_helper("-test", Expect),
    test_helper("-'test'", Expect),
    test_helper("-\"test\"", Expect).

%% Prohibited term, multiple words.
prohibited2_test() ->
    Expect = [
        #string { s="test1"},
        #negation { op=#string { s="test2" } }
    ],
    test_helper("test1 -test2", Expect),
    test_helper("test1 -'test2'", Expect),
    test_helper("test1 -\"test2\"", Expect).

required_and_prohibited1_test() ->
    Expect = [
        #intersection { ops=[
            #scope { field="acc", ops=[#string { s="aab", flags=[required] }] },
            #negation { op=#scope { field="acc", ops=[#string { s="aac" }] } }
        ]}
    ],
    test_helper("+acc:aab AND -acc:aac", Expect).
    

%% Intersection.
intersection1_test() ->
    Expect = [
        #intersection { ops=[
              #string { s="test1"},
              #string { s="test2"}
        ]}
    ],
    test_helper("test1 AND test2", Expect).

%% Intersection style 2.
intersection2_test() ->
    Expect = [
        #intersection { ops=[
              #string { s="test1"},
              #string { s="test2"}
        ]}
    ],
    test_helper("test1 && test2", Expect).

%% Union.
union1_test() ->
    Expect = [
        #union { ops=[
              #string { s="test1"},
              #string { s="test2"}
        ]}
    ],
    test_helper("test1 OR test2", Expect).

%% Union style 2.
union2_test() ->
    Expect = [
        #union { ops=[
              #string { s="test1"},
              #string { s="test2"}
        ]}
    ],
    test_helper("test1 || test2", Expect).

%% Negation.
negate1_test() ->
    Expect = [
        #string { s="test1" },
        #negation { op=#string { s="test2" } }
    ],
    test_helper("test1 NOT test2", Expect).

%% Negation style 2.
negate2_test() ->
    Expect = [
        #string { s="test1" },
        #negation { op=#string { s="test2" } }
    ],
    test_helper("test1 !test2", Expect).

%% Negate a phrase.
negate3_test() ->
    Expect = [
        #negation { op=#string { s="test1 test2" } }
    ],
    test_helper("NOT 'test1 test2'", Expect).

%% Fuzzy without a value.
fuzzy1_test() ->
    Expect = [
        #string { s="test", flags=[{fuzzy, 0.5}]}
    ],
    test_helper("test~", Expect).

%% Fuzzy with a value.
fuzzy2_test() ->
    Expect = [
        #string { s="test", flags=[{fuzzy, 0.8}]}
    ],
    test_helper("test~0.8", Expect).

%% Multiple fuzzy values.
fuzzy3_test() ->
    Expect = [
        #string { s="test1", flags=[{fuzzy, 0.1}]},
        #string { s="test2", flags=[{fuzzy, 0.2}]},
        #string { s="test3", flags=[{fuzzy, 0.3}]}
    ],
    test_helper("test1~0.1 test2~0.2 test3~0.3", Expect).

%% Proximity on a single phrase.
proximity1_test() ->
    Expect = [
        #string { s="test", flags=[{proximity, 5}]}
    ],
    test_helper("test~5", Expect).

proximity2_test() ->
    Expect = [
        #string { s="test1 test2", flags=[{proximity, 5}]}
    ],
    test_helper("'test1 test2'~5", Expect),
    test_helper("\"test1 test2\"~5", Expect).

group1_test() ->
    Expect = [
        #string { s="test1"},
        #group { ops=[
            #string { s="test2"},
            #string { s="test3"}
        ]}
    ],
    test_helper("test1 (test2 test3)", Expect),
    test_helper("'test1' ('test2' \"test3\")", Expect).

group2_test() ->
    Expect = [
        #group { ops=[
            #string { s="test1"},
            #group { ops=[
                #string { s="test2"},
                #string { s="test3"}
            ]}
        ]}
    ],
    test_helper("(test1 (test2 test3))", Expect),
    test_helper("('test1' ('test2' \"test3\"))", Expect).

group3_test() ->
    Expect = [
        #scope { index="index", field="field", ops=[
            #group { ops=[
              #string { s="test1", flags=[required] },
              #string { s="test2 test3", flags=[required] }
            ]}
        ]}
    ],
    test_helper("index.field:(+test1 +\"test2 test3\")", Expect).

range1_test() ->
    Expect = [
        #range {
            from=#inclusive { s="begin" },
            to=#inclusive { s="end" }
        }
    ],
    test_helper("[begin TO end]", Expect).

range2_test() ->
    Expect = [
        #scope { field="field", ops=[
            #range {
                from=#inclusive { s="begin" },
                to=#inclusive { s="end" }
            }
        ]}
    ],
    test_helper("field:[begin TO end]", Expect).

range3_test() ->
    Expect = [
        #range {
            from=#exclusive { s="begin" },
            to=#exclusive { s="end" }
        }
    ],
    test_helper("{begin TO end}", Expect).

range4_test() ->
    Expect = [
        #scope { field="field", ops=[
            #range {
                from=#exclusive { s="begin" },
                to=#exclusive { s="end" }
            }
        ]}
    ],
    test_helper("field:{begin TO end}", Expect).

boost1_test() ->
    Expect = [
        #string { s="test", flags=[{boost, 1.0}] }
    ],
    test_helper("test^", Expect).

boost2_test() ->
    Expect = [
        #string { s="test0", flags=[{boost, 0.5}] },
        #string { s="test1", flags=[{boost, 1.0}] },
        #string { s="test2", flags=[{boost, 2.0}] },
        #string { s="test 3", flags=[{boost, 3.0}] }
    ],
    test_helper("test0^0.5 test1^1 test2^2.0 \"test 3\"^3.0", Expect),
    test_helper("test0^0.5 test1^ test2^2.0 'test 3'^3.0", Expect).

wildcard1_test() ->
    Expect = [
        #string { s="test", flags=[{wildcard,char}] }
    ],
    test_helper("test?", Expect).

wildcard2_test() ->
    Expect = [
        #string { s="test", flags=[{wildcard,glob}] }
    ],
    test_helper("test*", Expect).

integer1_test() ->
    Expect = [
        #scope { field="field", ops=[
            #string { s="123456789"}
        ]}
    ],
    test_helper("field:123456789", Expect).

integer2_test() ->
    Expect = [
        #scope { field="field", ops=[
            #string { s="-123456789"}
        ]}
    ],
    test_helper("field:\\-123456789", Expect),
    test_helper("field:'-123456789'", Expect),
    test_helper("field:\"-123456789\"", Expect).

date1_test() ->
    Expect = [
        #string { s="2007-04-05T14:30"}
    ],
    test_helper("2007\\-04\\-05T14\\:30", Expect),
    test_helper("'2007-04-05T14:30'", Expect),
    test_helper("\"2007-04-05T14:30\"", Expect).

date2_test() ->
    Expect = [
        #scope { field="field", ops=[
            #string { s="2007-04-05T14:30"}
        ]}
    ],
    test_helper("field:2007\\-04\\-05T14\\:30", Expect),
    test_helper("field:'2007-04-05T14:30'", Expect),
    test_helper("field:\"2007-04-05T14:30\"", Expect).


complex1_test() ->
    Expect = [
        #scope { field="color", ops=[
            #group { ops=[
                #string { s="red" },
                #string { s="blue" }
            ]}
        ]}
    ],
    test_helper("color:(red blue)", Expect).

complex2_test() ->
    Expect = [
        #intersection { ops=[
            #scope { field="acc", ops=[#string { s="afa" }]},
            #scope { field="color", ops=[#string { s="red" }]},
            #scope { field="parity", ops=[#string { s="even" }]}
        ]}
    ],
    test_helper("acc:afa AND color:red AND parity:even", Expect).

complex3_test() ->
    Expect = [
        #scope { field="color", ops=[
            #union { ops=[
                #string { s="red" },
                #string { s="blue" }
            ]}
        ]}
    ],
    test_helper("color:(red OR blue)", Expect).

complex4_test() ->
    Expect = [
        #union { ops=[
            #scope { field="color", ops=[#string { s="red" }] },
            #scope { field="parity", ops=[#string { s="odd" }] },
            #scope { field="key", ops=[#string { s="AAB" }] }
        ]}
    ],
    test_helper("color:red OR parity:odd OR key:AAB", Expect).

complex5_test() ->
    Expect = [
        #scope { field="acc", ops=[
            #intersection { ops=[
                #string { s="aab" },
                #negation { op=#string { s="aac" } }
            ]}
        ]}
    ],
    test_helper( "acc:(aab AND NOT aac)", Expect).

complex7_test() ->
    Expect = [
        #scope { field="acc", ops=[
            #intersection { ops=[
                #string { s="aab" },
                #negation { op=#string { s="aac" } }
            ]}
        ]}
    ],
    test_helper( "acc:(aab AND (NOT aac))", Expect).

complex9_test() ->
    Expect = [
        #negation { op=#scope { field="acc", ops=[#string { s="AEB" }] } },
        #negation { op=#scope { field="parity", ops=[#string { s="even" }] } },
        #negation { op=#scope { field="color", ops=[#string { s="red" }] } },
        #negation { op=#scope { field="color", ops=[#string { s="orange" }] } },
        #negation { op=#scope { field="color", ops=[#string { s="yellow" }] } }
    ],
    test_helper( "-acc:AEB -parity:even -color:red -color:orange -color:yellow", Expect).

complex10_test() ->
    Expect = [
        #union { ops=[
            #intersection { ops=[
                #scope { field="color", ops=[#string { s="red" }] },
                #union { ops=[
                    #scope { field="parity", ops=[#string { s="even" }] },
                    #scope { field="key", ops=[#string { s="ABE" }] }
                ]}
            ]},
            #intersection { ops=[
                #union { ops=[
                    #scope { field="color", ops=[#string { s="blue" }] },
                    #scope { field="key", ops=[#string { s="ABC" }] }
                ]},
                #scope { field="parity", ops=[#string { s="odd" }] }
            ]}
        ]}
    ],
    test_helper( "(color:red AND (parity:even OR key:ABE)) OR ((color:blue OR key:ABC) AND parity:odd)", Expect).

complex11_test() ->
    Expect = [
        #intersection { ops=[
            #union { ops=[
                #scope { field="color", ops=[#string { s="re", flags=[{wildcard,glob}] }] },
                #scope { field="color", ops=[#string { s="blub", flags=[{fuzzy, 0.5}] }]}
            ]},
            #scope { field="parity", ops=[
                #range { from={exclusive, "d"}, to={exclusive, "f"} }
            ]}
        ]}
    ],
    test_helper( "(color:re* OR color:blub~) AND (parity:{d TO f})", Expect).

complex12_test() ->
    Expect = [
        #intersection { ops=[
            #scope { field="acc", ops=[#string { s="afa" }] },
            #negation { op=#scope { field="acc", ops=[#string { s="aga" }] } },
            #negation { op=#scope { field="color",
                                    ops=[#string { s="oran",
                                                   flags=[{wildcard,glob}] }] }}
        ]}
    ],
    test_helper( "(acc:afa AND -acc:aga) AND -color:oran*", Expect).

complex13_test() ->
    Expect = [
        #intersection { ops=[
            #scope { field="acc", ops=[#string { s="afa" }] },
            #negation { op=#scope { field="acc", ops=[#string { s="aga" }] } },
            #negation { op=#scope { field="color",
                                    ops=[#string { s="oran",
                                                   flags=[{wildcard,glob}] }] }}
        ]}
    ],
    test_helper( "(acc:afa AND (NOT acc:aga)) AND (NOT color:oran*)", Expect).

complex14_test() ->
    Expect = [
        #intersection { ops=[
            #scope { field="acc", ops=[
                #group { ops=[
                    #string { s="afa" },
                    #negation { op=#string { s="aga" } }
                ]}
            ]},
            #negation { op=#scope { field="color",
                                    ops=[#string { s="oran",
                                                   flags=[{wildcard,glob}] }] }}
        ]}
    ],
    test_helper( "acc:(afa NOT aga) AND -color:oran*", Expect).

complex15_test() ->
    Expect = [
        #intersection { ops=[
            #scope { field="acc", ops=[
                #intersection { ops=[
                    #string { s="afa" },
                    #negation { op=#string { s="aga" } }
                ]}
            ]},
            #negation { op=#scope { field="color",
                                    ops=[#string { s="oran",
                                                   flags=[{wildcard,glob}] }] }}
        ]}
    ],
    test_helper( "acc:(afa AND (NOT aga)) AND (NOT color:oran*)", Expect).


%% Scan and parse a query, compare to what we expect.
test_helper(String, Expect) ->
    {ok, Tokens, _} = lucene_scan:string(String),
    {ok, Actual} = lucene_parse:parse(Tokens),
    is_equal(Actual, Expect) orelse begin
        Msg = 
        "Query   : ~p~n"
        "Expected: ~p~n"
        "Actual  : ~p",
        error_logger:error_msg(Msg, [String, Expect, Actual]),
        throw({error_parsing_query, String})
    end.

%% Compare two query trees, ignoring the autogenerated ID.
is_equal(R1, R2) when is_reference(R1) andalso is_reference(R2) ->
    true;
is_equal(L1, L2) 
  when is_list(L1) andalso length(L1) > 0 andalso is_list(L2) andalso length(L2) > 0 -> 
    is_equal(hd(L1), hd(L2)) andalso is_equal(tl(L1), tl(L2));
is_equal(T1, T2) when is_tuple(T1) andalso is_tuple(T2) ->
    is_equal(tuple_to_list(T1), tuple_to_list(T2));
is_equal(Other, Other) -> 
    true;
is_equal(Other1, Other2) ->
    error_logger:error_msg("Mismatch: ~p /= ~p", [Other1, Other2]),
    false.

