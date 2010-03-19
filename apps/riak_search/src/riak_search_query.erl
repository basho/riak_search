-module(riak_search_query).
-export([
         execute/1,
         tests/0,
         test/2
        ]).
-include("riak_search.hrl").

%% - term  - DONE for now.
%% - lnot  - DONE
%% - land  - DONE
%% - lor   - DONE
%% - group - DONE
%% - field - DONE 
%% - facets - facet in AND case, facet in OR case.
%% - fuzzy terms
%% - inclusive_range
%% - exclusive_range
%% - node


%% Execute the query operation, 
%% Accepts a Qilr query.
%% Returns {ok, Results}
execute(OpList) -> 
    %% Normalize, Optimize, and Expand Buckets.
    OpList1 = #group { ops=OpList },
    OpList2 = riak_search_preplan:preplan(OpList1, "defIndex", "defField", []),
    ?PRINT(OpList2),

    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    {ok, NumInputs} = riak_search_op:chain_op(OpList2, self(), Ref),

    %% Gather and return results...
    Results = gather_results(NumInputs, Ref, []),
    {ok, Results}.

%% Gather results from all connections
gather_results(Connections, Ref, Acc) ->
    receive
	{results, Results, Ref} -> 
	    gather_results(Connections, Ref, Acc ++ Results);

	{disconnect, Ref} when Connections > 1  ->
	    gather_results(Connections - 1, Ref, Acc);

	{disconnect, Ref} when Connections == 1 ->
	    Acc;

	Other ->
	    throw({unexpected_message, Other})

    after 1 * 1000 ->
            ?PRINT(timeout),
            throw({timeout, Connections, Acc})
    end.

tests() ->
%%     test("+b +c b", "c"), %% NOT CLEAR HOW TO HANDLE THIS CASE
%%     test("+a -b (ab)", "ab"), %% PARSER FAILS HERE

%%     %% TEST +/-
%%     test("c b a", "abc"),
%%     test("+c +b +a", ""),
%%     test("+c ac", "c"),
%%     test("+c OR +b AND -a", "bc"),
%%     test("(+abc -def -a)", "bc"),

%%     %% TEST AND
%%     test("a AND a", "a"),
%%     test("aa AND a", "a"),
%%     test("aa AND aa", "aa"),
%%     test("a AND b", ""),
%%     test("a AND ab", "a"),
%%     test("a AND ba", "a"),
%%     test("ab AND ba", "ab"),
%%     test("ba AND ba", "ab"),
%%     test("ba AND bac", "ab"),
%%     test("abc AND bcd AND cde", "c"),
%%     test("abcd AND bcde AND cdef AND defg", "d"),

%%     %% TEST NOT
%%     test("aa AND (NOT a)", ""),
%%     test("aa AND (NOT b)", "aa"),
%%     test("a AND (NOT ab)", ""),
%%     test("a AND (NOT ba)", ""),
%%     test("ab AND (NOT a)", "b"),
%%     test("cba AND (NOT b)", "ac"),
    
%%     %% TEST OR
%%     test("a OR a", "a"),
%%     test("aa OR a", "aa"),
%%     test("a OR ab", "ab"),
%%     test("a OR ab", "ab"),
%%     test("abc OR def", "abcdef"),
%%     test("abc OR def OR ghi", "abcdefghi"),

%%     %% TEST GROUPS
%%     test("a OR (a OR (a OR (a OR b)))", "ab"),
%%     test("a OR (b OR (c OR (d OR a)))", "abcd"),
    ok.

test(Q, ExpectedResults) ->
    %% Parse...
    {ok, Qilr} = qilr_parse:string(Q),
%%     ?PRINT(Qilr),

    %% Execute...
    {ok, Results} = execute(Qilr),
%%     ?PRINT(Results),

    %% Check result...
    case Results == ExpectedResults of
        true ->
            io:format(" [.] PASS - ~s~n", [Q]);
        false ->
            io:format(" [X] FAIL - ~s~n", [Q])
    end.


