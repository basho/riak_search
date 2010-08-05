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
    write/5,
    info/2, info/3,
    iterator/1, iterator/3
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
    ReadBuffer = 1024 * 1024,
    {ok, WriteOpts} = application:get_env(merge_index, buffer_write_options),
    {ok, FH} = file:open(Filename, [read, write, raw, binary,
                                    {read_ahead, ReadBuffer}] ++ WriteOpts),

    %% Read into an ets table...
    Table = ets:new(buffer, [ordered_set, public]),
    open_inner(FH, Table),
    {ok, Size} = file:position(FH, cur),

    %% Return the buffer.
    #buffer { filename=Filename, handle=FH, table=Table, size=Size }.

open_inner(FH, Table) ->
    case mi_utils:read_value(FH) of
        {ok, {IFT, Value, Props, TS}} ->
            write_ets(IFT, Value, Props, TS, Table),
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
write(IFT, Value, Props, TS, Buffer) ->
    %% Write to file...
    FH = Buffer#buffer.handle,
    BytesWritten = mi_utils:write_value(FH, {IFT, Value, Props, TS}),

    %% Return a new buffer with a new tree and size...
    write_ets(IFT, Value, Props, TS, Buffer#buffer.table),

    %% Return the new buffer.
    Buffer#buffer {
        size = (BytesWritten + Buffer#buffer.size)
    }.

%% Return the number of results under this IFT.
info(IFT, Buffer) ->
    Spec = [{{{IFT, '_'}, '_', '_'}, [], [true]}],
    ets:select_count(Buffer#buffer.table, Spec).


%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(StartIFT, EndIFT, Buffer) ->
    Spec = [{{{'$1', '_'}, '_', '_'},
             [{'=<', StartIFT, '$1'}, {'=<', '$1', EndIFT}],
             [true]}],
    ets:select_count(Buffer#buffer.table, Spec).

%%
%% Return an iterator that traverses the entire buffer
%%
iterator(Buffer) ->
    Table = Buffer#buffer.table,
    fun() -> iterate_ets(ets:first(Table), undefined, Table) end.

%%
%% Return an iterator that traverses a range of the buffer
%%
iterator(StartIFT, StopIFT, Buffer) ->
    Table = Buffer#buffer.table,
    fun() -> iterate_ets(ets:next(Table, {StartIFT, <<>>}), StopIFT, Table) end.


%% ===================================================================
%% Internal functions
%% ===================================================================

write_ets(IFT, Value, Props, Tstamp, Table) ->
    case ets:lookup(Table, {IFT, Value}) of
        [] ->
            ets:insert_new(Table, {{IFT, Value}, Props, Tstamp});
        [{{IFT, Value}, _, ExistingTstamp}] ->
            case ExistingTstamp > Tstamp of
                true ->
                    %% Keep existing tstamp; clearly newer
                    ok;
                false ->
                    %% New value is >= existing value; take more recent write
                    ets:insert(Table, {{IFT, Value}, Props, Tstamp})
            end
    end.

iterate_ets('$end_of_table', _StopIFT, _Table) ->
    eof;
iterate_ets(CurrKey, StopIFT, Table) ->
    case ets:lookup(Table, CurrKey) of
        [{{IFT, Value} = Key, Props, Tstamp}] when IFT =< StopIFT ->
            {{IFT, Value, Props, Tstamp},
             fun() -> iterate_ets(ets:next(Table, Key), StopIFT, Table) end};
        _->
            eof
    end.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

g_ift() ->
    choose(0, ?POW_2(62)).

g_value() ->
    non_empty(binary()).

g_props() ->
    list({oneof([word_pos, offset]), choose(0, ?POW_2(31))}).

g_tstamp() ->
    choose(0, ?POW_2(31)).

g_ift_range(IFTs) ->
    ?SUCHTHAT({Start, End}, {oneof(IFTs), oneof(IFTs)}, End >= Start).

make_buffer([], B) ->
    B;
make_buffer([{Ift, Value, Props, Tstamp} | Rest], B0) ->
    B = mi_buffer:write(Ift, Value, Props, Tstamp, B0),
    make_buffer(Rest, B).

fold_iterator(Itr, Fn, Acc0) ->
    fold_iterator_inner(Itr(), Fn, Acc0).

fold_iterator_inner(eof, _Fn, Acc) ->
    lists:reverse(Acc);
fold_iterator_inner({Term, NextItr}, Fn, Acc0) ->
    Acc = Fn(Term, Acc0),
    fold_iterator_inner(NextItr(), Fn, Acc).


prop_basic_test(Root) ->
    ?FORALL(Entries, list({g_ift(), g_value(), g_props(), g_tstamp()}),
            begin
                %% Delete old files
                [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

                %% Create a buffer
                Buffer = make_buffer(Entries, mi_buffer:new(Root ++ "_buffer")),

                %% Filter the generated entries such that each {IFT, Value} is only present
                %% once and has the latest timestamp for that key
                F = fun({IFT, Value, Props, Tstamp}, Acc) ->
                            case orddict:find({IFT, Value}, Acc) of
                                {ok, {_, ExistingTstamp}} when Tstamp >= ExistingTstamp ->
                                    orddict:store({IFT, Value}, {Props, Tstamp}, Acc);
                                error ->
                                    orddict:store({IFT, Value}, {Props, Tstamp}, Acc);
                                _ ->
                                    Acc
                            end
                    end,
                ExpectedEntries = [{IFT, Value, Props, Tstamp} ||
                                      {{IFT, Value}, {Props, Tstamp}}
                                          <- lists:foldl(F, [], Entries)],

                %% Build a list of what was stored in the buffer
                ActualEntries = fold_iterator(mi_buffer:iterator(Buffer),
                                              fun(Item, Acc0) -> [Item | Acc0] end, []),
                ?assertEqual(ExpectedEntries, ActualEntries),
                true
            end).

prop_iter_range_test(Root) ->
    ?LET(IFTs, non_empty(list(g_ift())),
         ?FORALL({Entries, Range}, {list({oneof(IFTs), g_value(), g_props(), g_tstamp()}), g_ift_range(IFTs)},
            begin
                %% Delete old files
                [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

                %% Create a buffer
                Buffer = make_buffer(Entries, mi_buffer:new(Root ++ "_buffer")),

                %% Identify those values in the buffer that are in the generated range
                {Start, End} = Range,
                RangeEntries = fold_iterator(iterator(Start, End, Buffer),
                                             fun(Item, Acc0) -> [Item | Acc0] end, []),

                %% Verify that all IFTs within the actual entries satisfy the constraint
                ?assertEqual([], [IFT || {IFT, _, _, _} <- RangeEntries,
                                         IFT < Start, IFT > End]),

                %% Check that the count for the range matches the length of the returned
                %% range entries list
                ?assertEqual(length(RangeEntries), info(Start, End, Buffer)),
                true
            end)).


prop_basic_test_() ->
    test_spec("/tmp/test/mi_buffer_basic", fun prop_basic_test/1).

prop_iter_range_test_() ->
    test_spec("/tmp/test/mi_buffer_iter", fun prop_iter_range_test/1).

test_spec(Root, PropertyFn) ->
    {timeout, 60, fun() ->
                          application:load(merge_index),
                          os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
                          ?assert(eqc:quickcheck(eqc:numtests(250, ?QC_OUT(PropertyFn(Root ++ "/t1")))))
                  end}.



-endif. %EQC
-endif.
