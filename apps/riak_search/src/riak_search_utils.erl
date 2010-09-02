%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_utils).

-export([
    iterator_tree/3,
    combine_terms/2,
    to_atom/1,
    to_binary/1,
    to_utf8/1,
    to_boolean/1,
    to_list/1,
    to_integer/1,
    to_float/1,
    from_binary/1,
    index_recursive/2,
    n_val/0,
    current_key_clock/0,
    choose/1,
    ets_keys/1,
    partition_fun/0,
    calc_partition/3
]).

-include("riak_search.hrl").


%% Chain a list of iterators into what looks like one single iterator.
%% The SelectFun/2 takes two iterators (which each provide {Value,
%% Props, IteratorFun}). The SelectFun is responsible for choosing
%% which value is next in the series, and returning {Index, Value, Props,
%% NewIteratorFun}.
iterator_tree(SelectFun, OpList, QueryProps) ->
    %% Turn all operations into iterators and then combine into a tree.
    Iterators = [iterator_tree_op(X, QueryProps) || X <- OpList],
    iterator_tree_combine(SelectFun, Iterators).

%% @private Given a list of iterators, combine into a tree. Works by
%% walking through the list pairing two iterators together (which
%% combines a level of iterators) and then calling itself recursively
%% until there is only one iterator left.
iterator_tree_combine(SelectFun, Iterators) ->
    case iterator_tree_combine_inner(SelectFun, Iterators) of
        [OneIterator] -> 
            OneIterator;
        ManyIterators -> 
            iterator_tree_combine(SelectFun, ManyIterators)
    end.
iterator_tree_combine_inner(_SelectFun, []) ->
    [];
iterator_tree_combine_inner(_SelectFun, [Iterator]) ->
    [Iterator];
iterator_tree_combine_inner(SelectFun, [IteratorA,IteratorB|Rest]) ->
    Iterator = fun() -> SelectFun(IteratorA(), IteratorB()) end,
    [Iterator|iterator_tree_combine_inner(SelectFun, Rest)].
    
%% Chain an operator, and build an iterator function around it. The
%% iterator will return {Result, NotFlag, NewIteratorFun} each time it is called, or block
%% until one is available. When there are no more results, it will
%% return {eof, NotFlag}.
iterator_tree_op(Op, QueryProps) ->
    %% Spawn a collection process...
    Ref = make_ref(),
    Pid = spawn_link(fun() -> collector_loop(Ref, []) end),

    %% Chain the op...
    riak_search_op:chain_op(Op, Pid, Ref, QueryProps),

    %% Return an iterator function. Returns
    %% a new result.
    fun() -> iterator_tree_inner(Pid, make_ref(), Op) end.

%% Iterator function body.
iterator_tree_inner(Pid, Ref, Op) ->
    Pid!{get_result, self(), Ref},
    receive
        {result, eof, Ref} ->
            {eof, Op};
        {result, Result, Ref} ->
            {Result, Op, fun() -> iterator_tree_inner(Pid, Ref, Op) end};
        X ->
            io:format("iterator_tree_inner(~p, ~p, ~p)~n>> unknown message: ~p~n", [Pid, Ref, Op, X])
    end.

%% Collect messages in the process's mailbox, and wait until someone
%% requests it.
collector_loop(Ref, []) ->
    receive
        {results, Results, Ref} ->
            collector_loop(Ref, Results);
        {disconnect, Ref} ->
            collector_loop(Ref, eof)
    end;
collector_loop(Ref, [Result|Results]) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, Result, OutputRef},
            collector_loop(Ref, Results)
    end;
collector_loop(_Ref, eof) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, eof, OutputRef}
    end.


%% Gine
combine_terms({Index, DocID, Props1}, {Index, DocID, Props2}) ->
    %% score list is concatenation of each term's scores
    ScoreList1 = proplists:get_value(score, Props1, []),
    ScoreList2 = proplists:get_value(score, Props2, []),
    ScoreList = ScoreList1++ScoreList2,

    %% word position is concatentation of each term's scores
    WordPos1 = proplists:get_value(word_pos, Props1, []),
    WordPos2 = proplists:get_value(word_pos, Props2, []),
    WordPos = WordPos1++WordPos2,

    %% frequency is sum of each term's frequency
    Freq1 = proplists:get_value(freq, Props1, 0),
    Freq2 = proplists:get_value(freq, Props2, 0),
    Freq = Freq1+Freq2,

    %% only include the common properties from the rest of the list
    Intersection = sets:to_list(sets:intersection(sets:from_list(Props1),
                                                  sets:from_list(Props2))),

    %% overwrite whatever score/position/frequency came out of intersection
    NewProps = lists:foldl(fun({K, V}, Acc) ->
                                   lists:keystore(K, 1, Acc, {K, V})
                           end,
                           Intersection,
                           [{score, ScoreList},
                            {word_pos, WordPos},
                            {freq, Freq}]
                           ),
    {Index, DocID, NewProps};
combine_terms(Other1, Other2) ->
    error_logger:error_msg("Could not combine terms: [~p, ~p]~n", [Other1, Other2]),
    throw({could_not_combine, Other1, Other2}).

to_list(A) when is_atom(A) -> atom_to_list(A);
to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(I) when is_integer(I) -> integer_to_list(I);
to_list(F) when is_float(F) -> float_to_list(F);
to_list(L) when is_list(L) -> L.

to_atom(A) when is_atom(A) -> A;
to_atom(B) when is_binary(B) -> to_atom(binary_to_list(B));
to_atom(I) when is_integer(I) -> to_atom(integer_to_list(I));
to_atom(L) when is_list(L) -> list_to_atom(binary_to_list(list_to_binary(L))).

to_binary(A) when is_atom(A) -> to_binary(atom_to_list(A));
to_binary(B) when is_binary(B) -> B;
to_binary(I) when is_integer(I) -> to_binary(integer_to_list(I));
to_binary(L) when is_list(L) -> list_to_binary(L).

to_utf8(A) when is_atom(A) -> to_utf8(atom_to_list(A));
to_utf8(B) when is_binary(B) -> B;
to_utf8(I) when is_integer(I) -> to_utf8(integer_to_list(I));
to_utf8(L) when is_list(L) -> unicode:characters_to_binary(L).


to_integer(A) when is_atom(A) -> to_integer(atom_to_list(A));
to_integer(B) when is_binary(B) -> to_integer(binary_to_list(B));
to_integer(I) when is_integer(I) -> I;
to_integer(L) when is_list(L) -> list_to_integer(L).

to_float(F) ->
    list_to_float(to_list(F)).

to_boolean(B) ->
    A = to_atom(B),
    (A == yes) orelse (A == true) orelse (A == '1').

from_binary(B) when is_binary(B) ->
    binary_to_list(B);
from_binary(L) ->
    L.

%% Recursively index the provided file or directory, running
%% the specified function on the body of any files.
index_recursive(Callback, Directory) ->
    io:format(" :: Traversing directory: ~s~n", [Directory]),
    Files = filelib:wildcard(Directory),
    io:format(" :: Found ~p files...~n", [length(Files)]),

    F = fun(File) -> index_recursive_file(Callback, File) end,
    plists:map(F, Files, {processes, 8}),
    ok.

%% @private
%% Full-text index the specified file.
index_recursive_file(Callback, File) ->
    Basename = filename:basename(File),
    io:format(" :: Processing file: ~s~n", [Basename]),
    case file:read_file(File) of
        {ok, Bytes} ->
            Callback(Basename, Bytes);
        {error, eisdir} ->
            index_recursive(Callback, filename:join(File, "*"));
        Err ->
            io:format("index_file(~p): error: ~p~n", [File, Err])
    end.

%% @private
%% N val for search replication - currently fixed size for all buckets
n_val() ->
    app_helper:get_env(riak_search, n_val, 2).

%% Return a key clock to use for revisioning IFTVPs
current_key_clock() ->
    {MegaSeconds,Seconds,MilliSeconds}=erlang:now(),
    (MegaSeconds * 1000000000000) + 
    (Seconds * 1000000) + 
    MilliSeconds.

%% Choose a random element from the List or Array.
choose(List) when is_list(List) ->
    random:seed(now()),
    N = random:uniform(length(List)),
    lists:nth(N, List);
choose(Array) when element(1, Array) == array ->
    random:seed(now()),
    N = random:uniform(Array:size()),
    Array:get(N - 1).


%% Given an ETS table, return a list of keys.
ets_keys(Table) ->
    Key = ets:first(Table),
    [Key|ets_keys_1(Table, Key)].
ets_keys_1(Table, Key) ->
    case ets:next(Table, Key) of
        '$end_of_table' -> 
            [];
        NextKey -> 
            [NextKey|ets_keys_1(Table, NextKey)]
    end.



% @doc Returns a function F(Index, Field, Term) -> integer() that can
% be used to calculate the partition on the ring. It is used in places
% where we need to make repeated calls to get the actual partition
% (not the DocIdx) of an Index/Field/Term combination. NOTE: This, or something like it,
% should probably get moved to Riak Core in the future. 
-define(RINGTOP, trunc(math:pow(2,160)-1)).  % SHA-1 space
partition_fun() ->
    %% Get the number of partitions.
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {chstate, _, _, CHash, _} = Ring,
    {NumPartitions, _} = CHash,

    %% Return a function to calculate the partition.
    fun(Index, Field, Term) ->
            <<IndexAsInt:160/integer>> = calc_partition(Index, Field, Term),
            Inc = ?RINGTOP div NumPartitions,
            RingPos = (IndexAsInt div Inc) + 1,
            case RingPos == NumPartitions of
                true -> 0;
                false -> RingPos * Inc
            end
    end.

calc_partition(Index, Field, Term) ->
    %% Work out which partition to use
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    riak_core_util:chash_key({IndexBin, FieldTermBin}).

