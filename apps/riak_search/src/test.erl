-module(test).
-export([test/0, test/1, testtree/0]).
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

testtree() ->
    T = gb_trees:from_orddict([{X, X * 2} || X <- lists:seq(1, 100)]),
    gb_tree_select(all, 20, true, T).


gb_tree_select(Start, End, Inclusive, Tree) ->
    {_, T} = Tree,
    gb_tree_select(Start, End, Inclusive, T, []).

gb_tree_select(_, _, _, nil, Acc) -> 
    Acc;
gb_tree_select(Start, End, Inclusive, {Key, Value, Left, Right}, Acc) ->
    LBound = (Start == all) orelse (Start < Key) orelse (Start == Key andalso Inclusive),
    RBound = (End == all) orelse (Key < End) orelse (End == Key andalso Inclusive),

    %% If we are within bounds, then add this value...
    NewAcc = case LBound andalso RBound of
        true -> [{Key, Value}|Acc];
        false -> Acc
    end,

    %% If we are in lbound, then go left...
    NewAcc1 = case LBound of
        true ->  gb_tree_select(Start, End, Inclusive, Left, NewAcc);
        false -> NewAcc
    end,

    %% If we are in rbound, then go right...
    NewAcc2 = case RBound of
        true ->  gb_tree_select(Start, End, Inclusive, Right, NewAcc1);
        false -> NewAcc1
    end,
    NewAcc2.
