-module(test).
-export([test/0, test/1]).
-define(IS_CHAR(C), ((C >= $A andalso C =< $Z) orelse (C >= $a andalso C =< $z))).



test() ->
    Files = filelib:wildcard("../../../Enron/*"),
    {Files1, _} = lists:split(20, lists:sort(Files)),
%%     {Files1, _} = lists:split(length(Files), lists:sort(Files)),
    [index_file(File) || File <- Files1],
    ok.

test(Q) ->
    {ok, Qilr} = qilr_parse:string(Q),
    {ok, Results} = riak_search_query:execute(Qilr),
    Results.

%% Index the file.
index_file(File) ->
    io:format("File: ~p~n", [File]),
    {ok, Bytes} = file:read_file(File),
    Props = [{"color", random_color()}],
    index_file_inner(Bytes, [], File, Props).

index_file_inner(<<>>, [], _, _) -> 
    ok;

index_file_inner(<<>>, Acc, File, Props) ->
    Word = string:to_lower(lists:reverse(Acc)),
    index_word(Word, File, Props);

index_file_inner(<<C, Rest/binary>>, Acc, File, Props) when ?IS_CHAR(C) ->
    index_file_inner(Rest, [C|Acc], File, Props);

index_file_inner(<<_, Rest/binary>>, [], File, Props) ->
    index_file_inner(Rest, [], File, Props);

index_file_inner(<<_, Rest/binary>>, Acc, File, Props) ->
    Word = string:to_lower(lists:reverse(Acc)),
    index_word(Word, File, Props),
    index_file_inner(Rest, [], File, Props).
        
index_word(Term, Filename, Props) ->
    case length(Term) > 0 of
        true ->
            Index = "search",
            Field = "default",
            riak_search:put(Index, Field, Term, Filename, Props);
        false ->
            ok
    end.

random_color() ->
    N = random:uniform(5),
    lists:nth(N, [
        "red",
        "orange",
        "yellow",
        "green",
        "blue"
    ]).
    

collect_results(Ref, Count) ->
    receive 
        {result, '$end_of_table', Ref} ->
            ok;
        {result, Value, Ref} ->
            io:format(":: ~p~n", [Value]),
            collect_results(Ref, Count + 1)
    end.


-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).


