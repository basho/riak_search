-module(test).
-export([test/0, test/1]).
-define(IS_CHAR(C), ((C >= $A andalso C =< $Z) orelse (C >= $a andalso C =< $z))).



test() ->
    {ok, Client} = riak:local_client(),
    Files = filelib:wildcard("../../../Enron/*"),
%%      {Files1, _} = lists:split(1, lists:sort(Files)),
    {Files1, _} = lists:split(length(Files), lists:sort(Files)),
    [index_file(Client, File) || File <- Files1],
    ok.

test(Q) ->
    {ok, Qilr} = qilr_parse:string(Q),
    {ok, Results} = riak_search_query:execute(Qilr),
    Results.

%% Index the file.
index_file(Client, File) ->
    io:format("File: ~p~n", [File]),
    {ok, Bytes} = file:read_file(File),
    index_file_inner(Client, Bytes, [], File).

index_file_inner(_, <<>>, [], _) -> 
    ok;

index_file_inner(Client, <<>>, Acc, File) ->
    Word = string:to_lower(lists:reverse(Acc)),
    index_word(Client, Word, File);

index_file_inner(Client, <<C, Rest/binary>>, Acc, File) when ?IS_CHAR(C) ->
    index_file_inner(Client, Rest, [C|Acc], File);

index_file_inner(Client, <<_, Rest/binary>>, [], File) ->
    index_file_inner(Client, Rest, [], File);

index_file_inner(Client, <<_, Rest/binary>>, Acc, File) ->
    Word = string:to_lower(lists:reverse(Acc)),
    index_word(Client, Word, File),
    index_file_inner(Client, Rest, [], File).
        
index_word(Client, Word, File) ->
    case length(Word) > 0 of
        true ->
            BucketName = list_to_binary(Word),
            Payload = {put, File, []},
            Obj = riak_object:new(<<"search">>, BucketName, Payload),
            Client:put(Obj, 1, 0);
        false ->
            ok
    end.

collect_results(Ref, Count) ->
    receive 
        {result, '$end_of_table', Ref} ->
            ok;
        {result, Value, Ref} ->
            io:format(":: ~p~n", [Value]),
            collect_results(Ref, Count + 1)
    end.
    
