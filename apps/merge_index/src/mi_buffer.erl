%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_buffer).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
    new/1,
    filename/1,
    close_filehandle/1,
    delete/1,
    filesize/1,
    size/1,
    write/7, write/2,
    info/4,
    iterator/1, iterator/4, iterators/6
]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.


-record(buffer, {
    filename,
    handle,
    table,
    size
}).

%%% Creates a disk-based append-mode buffer file with support for a
%%% sorted iterator.

%% Open a new buffer. Returns a buffer structure.
new(Filename) ->
    %% Open the existing buffer file...
    filelib:ensure_dir(Filename),
    {ok, WriteOpts} = application:get_env(merge_index, buffer_write_options),
    {ok, FH} = file:open(Filename, [read, write, raw, binary] ++ WriteOpts),

    %% Read into an ets table...
    Table = ets:new(buffer, [duplicate_bag, public]),
    open_inner(FH, Table),
    {ok, Size} = file:position(FH, cur),

    %% Return the buffer.
    #buffer { filename=Filename, handle=FH, table=Table, size=Size }.

open_inner(FH, Table) ->
    case read_value(FH) of
        {ok, {Index, Field, Term, Value, Props, TS}} ->
            write_to_ets(Table, [{Index, Field, Term, Value, Props, TS}]),
            open_inner(FH, Table);
        eof ->
            ok
    end.

filename(Buffer) ->
    Buffer#buffer.filename.

delete(Buffer) ->
    ets:delete(Buffer#buffer.table),
    close_filehandle(Buffer),
    file:delete(Buffer#buffer.filename),
    file:delete(Buffer#buffer.filename ++ ".deleted"),
    ok.

close_filehandle(Buffer) ->
    file:close(Buffer#buffer.handle).

%% Return the current size of the buffer file.
filesize(Buffer) ->
    Buffer#buffer.size.

size(Buffer) ->
    ets:info(Buffer#buffer.table, size).

%% Write the value to the buffer.
%% Returns the new buffer structure.
write(Index, Field, Term, Value, Props, TS, Buffer) ->
    write([{Index, Field, Term, Value, Props, TS}], Buffer).

write(Postings, Buffer) ->
    %% Write to file...
    FH = Buffer#buffer.handle,
    BytesWritten = write_to_file(FH, Postings),

    %% Return a new buffer with a new tree and size...
    write_to_ets(Buffer#buffer.table, Postings),

    %% Return the new buffer.
    Buffer#buffer {
        size = (BytesWritten + Buffer#buffer.size)
    }.

%% Return the number of results under this IFT.
info(Index, Field, Term, Buffer) ->
    Spec = [{{{Index, Field, Term, '_'}, '_', '_'}, [], [true]}],
    ets:select_count(Buffer#buffer.table, Spec).

%% Return an iterator that traverses the entire buffer.
iterator(Buffer) ->
    Table = Buffer#buffer.table,
    Keys = lists:sort(mi_utils:ets_keys(Table)),
    fun() -> iterate_keys(Table, Keys) end.
    
iterate_keys(Table, [Key|Keys]) ->
    Results1 = ets:lookup(Table, Key),
    Results2 = [{I,F,T,V,P,K} || {{I,F,T},V,P,K} <- Results1],
    Results3 = lists:sort(fun term_compare_fun/2, Results2),
    iterate_keys_1(Table, Keys, Results3);
iterate_keys(_, []) ->
    eof.
iterate_keys_1(Table, Keys, [Result|Results]) ->
    {Result, fun() -> iterate_keys_1(Table, Keys, Results) end};
iterate_keys_1(Table, Keys, []) ->
    iterate_keys(Table, Keys).
    
%% Return an iterator that traverses a range of the buffer.
iterator(Index, Field, Term, Buffer) ->
    Table = Buffer#buffer.table,
    List1 = ets:lookup(Table, {Index, Field, Term}),
    List2 = [{V,P,K} || {_Key,V,P,K} <- List1],
    List3 = lists:sort(fun value_compare_fun/2, List2),
    fun() -> iterate_list(List3) end.

%% Return a list of iterators over a range.
iterators(Index, Field, StartTerm, EndTerm, Size, Buffer) ->
    Table = Buffer#buffer.table,
    Keys = mi_utils:ets_keys(Table),
    Filter = fun(Key) ->
                     Key >= {Index, Field, StartTerm} 
                         andalso 
                         Key =< {Index, Field, EndTerm}
                         andalso
                         (Size == all orelse erlang:size(element(3, Key)) == Size)
        end,
    MatchingKeys = lists:filter(Filter, Keys),
    [iterator(I,F,T, Buffer) || {I,F,T} <- MatchingKeys].

%% Turn a list into an iterator.
iterate_list([]) ->
    eof;
iterate_list([H|T]) ->
    {H, fun() -> iterate_list(T) end}.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% Used by mi_server.erl to compare two terms, for merging
%% segments. Return true if items are in order. 
term_compare_fun({I1, F1, T1, V1, _, TS1}, {I2, F2, T2, V2, _, TS2}) ->
    (I1 < I2 orelse
        (I1 == I2 andalso
            (F1 < F2 orelse
                (F1 == F2 andalso
                    (T1 < T2 orelse
                        (T1 == T2 andalso
                            (V1 < V2 orelse
                                (V1 == V2 andalso
                                    (TS1 > TS2))))))))).

%% Used by mi_server.erl to compare two values, for streaming ordered
%% results back to a caller. Return true if items are in order.
value_compare_fun({V1, _, TS1}, {V2, _, TS2}) ->
    (V1 < V2 orelse
        (V1 == V2 andalso
            (TS1 > TS2))).

read_value(FH) ->
    case file:read(FH, 2) of
        {ok, <<Size:16/integer>>} ->
            {ok, B} = file:read(FH, Size),
            {ok, binary_to_term(B)};
        eof ->
            eof
    end.

write_to_file(FH, Terms) when is_list(Terms) ->
    %% Convert all values to binaries, count the bytes.
    F = fun(X, {SizeAcc, IOAcc}) ->
                B1 = term_to_binary(X),
                Size = erlang:size(B1),
                B2 = <<Size:16/integer, B1/binary>>,
                {SizeAcc + Size + 2, [B2|IOAcc]}
        end,
    {Size, ReverseIOList} = lists:foldl(F, {0, []}, Terms),
    ok = file:write(FH, lists:reverse(ReverseIOList)),
    Size.

write_to_ets(Table, Postings) ->
    %% Convert to {Key, Value, Props, Tstamp}
    F = fun({Index, Field, Term, Value, Props, Tstamp}) ->
                {{Index, Field, Term}, Value, Props, Tstamp}
        end,
    Postings1 = [F(X) || X <- Postings],
    ets:insert(Table, Postings1).

%% %% ===================================================================
%% %% EUnit tests
%% %% ===================================================================
%% -ifdef(TEST).

%% -ifdef(EQC).

%% -define(QC_OUT(P),
%%         eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%% -define(POW_2(N), trunc(math:pow(2, N))).

%% -define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

%% g_iftv() ->
%%     non_empty(binary()).

%% g_props() ->
%%     list({oneof([word_pos, offset]), choose(0, ?POW_2(31))}).

%% g_tstamp() ->
%%     choose(0, ?POW_2(31)).

%% %% g_ift_range(IFTs) ->
%% %%     ?SUCHTHAT({Start, End}, {oneof(IFTs), oneof(IFTs)}, End >= Start).

%% fold_iterator(Itr, Fn, Acc0) ->
%%     fold_iterator_inner(Itr(), Fn, Acc0).

%% fold_iterator_inner(eof, _Fn, Acc) ->
%%     lists:reverse(Acc);
%% fold_iterator_inner({Term, NextItr}, Fn, Acc0) ->
%%     Acc = Fn(Term, Acc0),
%%     fold_iterator_inner(NextItr(), Fn, Acc).


%% prop_basic_test(Root) ->
%%     ?FORALL(Entries, list({g_iftv(), g_iftv(), g_iftv(), g_iftv(), g_props(), g_tstamp()}),
%%             begin
%%                 %% Delete old files
%%                 [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

%%                 %% Create a buffer
%%                 Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),

%%                 %% Filter the generated entries such that each {IFT, Value} is only present
%%                 %% once and has the latest timestamp for that key
%%                 F = fun({Index, Field, Term, Value, Props, Tstamp}, Acc) ->
%%                             Key = {Index, Field, Term, Value},
%%                             case orddict:find(Key, Acc) of
%%                                 {ok, {_, ExistingTstamp}} when Tstamp >= ExistingTstamp ->
%%                                     orddict:store({Key, {Props, Tstamp}}, Acc);
%%                                 error ->
%%                                     orddict:store({Key, {Props, Tstamp}}, Acc);
%%                                 _ ->
%%                                     Acc
%%                             end
%%                     end,
%%                 ExpectedEntries = [{Index, Field, Term, Value, Props, Tstamp} ||
%%                                       {{Index, Field, Term, Value}, {Props, Tstamp}}
%%                                           <- lists:foldl(F, [], Entries)],

%%                 %% Build a list of what was stored in the buffer
%%                 ActualEntries = fold_iterator(mi_buffer:iterator(Buffer),
%%                                               fun(Item, Acc0) -> [Item | Acc0] end, []),
%%                 ?assertEqual(ExpectedEntries, ActualEntries),
%%                 true
%%             end).

%% %% prop_iter_range_test(Root) ->
%% %%     ?LET(IFTs, non_empty(list(g_iftv())),
%% %%          ?FORALL({Entries, Range}, {list({oneof(IFTs), g_value(), g_props(), g_tstamp()}), g_ift_range(IFTs)},
%% %%             begin
%% %%                 %% Delete old files
%% %%                 [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

%% %%                 %% Create a buffer
%% %%                 Buffer = make_buffer(Entries, mi_buffer:new(Root ++ "_buffer")),

%% %%                 %% Identify those values in the buffer that are in the generated range
%% %%                 {Start, End} = Range,
%% %%                 RangeEntries = fold_iterator(iterator(Start, End, Buffer),
%% %%                                              fun(Item, Acc0) -> [Item | Acc0] end, []),

%% %%                 %% Verify that all IFTs within the actual entries satisfy the constraint
%% %%                 ?assertEqual([], [IFT || {IFT, _, _, _} <- RangeEntries,
%% %%                                          IFT < Start, IFT > End]),

%% %%                 %% Check that the count for the range matches the length of the returned
%% %%                 %% range entries list
%% %%                 ?assertEqual(length(RangeEntries), info(Start, End, Buffer)),
%% %%                 true
%% %%             end)).


%% prop_basic_test_() ->
%%     test_spec("/tmp/test/mi_buffer_basic", fun prop_basic_test/1).

%% %% prop_iter_range_test_() ->
%% %%     test_spec("/tmp/test/mi_buffer_iter", fun prop_iter_range_test/1).

%% test_spec(Root, PropertyFn) ->
%%     {timeout, 60, fun() ->      
%%                           application:load(merge_index),
%%                           os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
%%                           ?assert(eqc:quickcheck(eqc:numtests(250, ?QC_OUT(PropertyFn(Root ++ "/t1")))))
%%                   end}.



%% -endif. %EQC
%% -endif.
