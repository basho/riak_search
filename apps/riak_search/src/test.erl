-module(test).
-export([start/0, test/0, test/1]).
-define(IS_CHAR(C), ((C >= $A andalso C =< $Z) orelse (C >= $a andalso C =< $z))).

start() ->
    {ok, Pid} = merge_index:start("./data/index"),
%%     eprof:start_profiling([Pid]),
    ok.

test() ->
    %% Read enron text, split by space, write to backend.
    Files = filelib:wildcard("../../../Enron/*"),
%%     {Files1, _} = lists:split(2, lists:sort(Files)),
    {Files1, _} = lists:split(length(Files), lists:sort(Files)),
    [index_file(File) || File <- Files1],
%%     eprof:analyse(),
    ok.

%% test(Word) ->
%%     %% Run some searches.
%%     Ref = make_ref(),
%%     riak_search_file_index:stream(string:to_lower(Word), self(), Ref),
%%     collect_results(Ref, 0),
%%     ok.

test(Q) ->
    {ok, Qilr} = qilr_parse:string(Q),
    {ok, Results} = riak_search_query:execute(Qilr),
    Results.


%% Index the file.
index_file(File) ->
    io:format("File: ~p~n", [File]),
    {ok, Bytes} = file:read_file(File),
    index_file_inner(Bytes, [], File).

index_file_inner(<<>>, [], _) -> 
    ok;

index_file_inner(<<>>, Acc, File) ->
    Word = string:to_lower(lists:reverse(Acc)),
    index_word(Word, File);

index_file_inner(<<C, Rest/binary>>, Acc, File) when ?IS_CHAR(C) ->
    index_file_inner(Rest, [C|Acc], File);

index_file_inner(<<_, Rest/binary>>, [], File) ->
    index_file_inner(Rest, [], File);

index_file_inner(<<_, Rest/binary>>, Acc, File) ->
    Word = string:to_lower(lists:reverse(Acc)),
    index_word(Word, File),
    index_file_inner(Rest, [], File).
        
collect_results(Ref, Count) ->
    receive 
        {result, '$end_of_table', Ref} ->
            ok;
        {result, Value, Ref} ->
            io:format(":: ~p~n", [Value]),
            collect_results(Ref, Count + 1)
    end.
    
index_word(Word, File) ->
    case length(Word) > 0 of
        true ->
%%             io:format("Word: ~s~n", [Word]);
            merge_index:put(Word, File, []);
        false ->
            ok
    end.
