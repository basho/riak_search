-module(riak_search_query).
-export([
         execute/1,
         tests/0
        ]).
-include("riak_search.hrl").

%% - term  - DONE for now.
%% - lnot  - DONE	 
%% - land  - DONE
%% - lor   - DONE
%% - group - DONE
%% - field - DONE	 
%% - inclusive_range
%% - exclusive_range
%% - node 


%% Execute the query operation, 
%% Accepts a Qilr query.
%% Returns {ok, Results}
execute(OpList) -> 
    %% Normalize, Optimize, and Expand Buckets.
    OpList1 = riak_search_preplan:preplan("defIndex", "defField", OpList),

    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    {ok, NumInputs} = riak_search_op:chain_op(OpList1, self(), Ref),

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
            ?PRINT(timenout),
            throw({timeout, Connections, Acc})
    end.

tests() ->
    %% TEST AND
    run_test("a AND a", "a"),
    run_test("aa AND a", "a"),
    run_test("aa AND aa", "aa"),
    run_test("a AND b", ""),
    run_test("a AND ab", "a"),
    run_test("a AND ba", "a"),
    run_test("ab AND ba", "ab"),
    run_test("ba AND ba", "ab"),
    run_test("ba AND bac", "ab"),
    run_test("abc AND bcd AND cde", "c"),
    run_test("abcd AND bcde AND cdef AND defg", "d"),

    %% TEST NOT
    run_test("aa AND (NOT a)", ""),
    run_test("aa AND (NOT b)", "aa"),
    run_test("a AND (NOT ab)", ""),
    run_test("a AND (NOT ba)", ""),
    run_test("ab AND (NOT a)", "b"),
    run_test("cba AND (NOT b)", "ac"),

    %% TEST +/-
    run_test("+a -b", "a"),
    
    %% TEST OR
    run_test("a OR a", "a"),
    run_test("aa OR a", "aa"),
    run_test("a OR ab", "ab"),
    run_test("a OR ab", "ab"),
    run_test("abc OR def", "abcdef"),
    run_test("abc OR def OR ghi", "abcdefghi"),

    %% TEST GROUPS
    run_test("a OR (a OR (a OR (a OR b)))", "ab"),
    run_test("a OR (b OR (c OR (d OR a)))", "abcd"),
    ok.

run_test(Q, ExpectedResults) ->
    %% Parse...
    {ok, Qilr} = qilr_parse:string(Q),

    %% Execute...
    {ok, Results} = execute(Qilr),

    %% Check result...
    case Results == ExpectedResults of
        true ->
            io:format(" [.] PASS - ~s~n", [Q]);
        false ->
            io:format(" [X] FAIL - ~s~n", [Q])
    end.


