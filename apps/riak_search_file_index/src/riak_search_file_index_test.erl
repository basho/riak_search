-module(riak_search_file_index_test).
-include("riak_search_file_index.hrl").
-export([test/0]).

test() ->
    riak_search_file_index_sup:start_link(),

    F = fun(N) ->
        Bucket = bucket(N), 
        Key = integer_to_list(N),
        Props = [{a, N}, {b, N}],
        riak_search_file_index:put(Bucket, Key, Props),
        case (N rem 10000 == 0) of
            true -> io:format(".");
            false -> ignore
        end
    end,
    [F(X) || X <- lists:seq(1, 100000)],

    Ref = make_ref(),
    riak_search_file_index:stream(bucket(1), self(), Ref),
    collect_results(Ref, 0),
    ok.

bucket(N) ->
    "bucket" ++ integer_to_list(N rem 10).

collect_results(Ref, Count) ->
    receive 
        {result, '$end_of_table', Ref} ->
            ?PRINT(Count),
            ok;
        {result, Value, Ref} ->
%%             ?PRINT(Value),
            collect_results(Ref, Count + 1)
    end.
    
