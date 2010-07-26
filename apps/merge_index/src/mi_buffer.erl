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
    open/2,
    filename/1,
    close_filehandle/1,
    delete/1,
    filesize/1,
    size/1,
    write/5,
    info/2, info/3,
    iterator/1, iterator/3,
    test/0
]).

-record(buffer, {
    filename,
    handle,
    table,
    size
}).

%%% Creates a disk-based append-mode buffer file with support for a
%%% sorted iterator.

%% Open a new buffer. Returns a buffer structure.
open(Filename, Options) ->
    %% Open the existing buffer file...
    filelib:ensure_dir(Filename),
    ReadBuffer = 1024 * 1024,
    WriteInterval = proplists:get_value(write_interval, Options, 2 * 1000),
    WriteBuffer = proplists:get_value(write_buffer, Options, 1024 * 1024),
    {ok, FH} = file:open(Filename, [read, {read_ahead, ReadBuffer}, write,
                                    {delayed_write, WriteBuffer, WriteInterval}, raw, binary]),

    %% Read into an ets table...
    Table = ets:new(buffer, [ordered_set, public]),
    open_inner(FH, Table),
    {ok, Size} = file:position(FH, cur),

    %% Return the buffer.
    #buffer { filename=Filename, handle=FH, table=Table, size=Size }.

open_inner(FH, Table) ->
    case mi_utils:read_value(FH) of
        {ok, {IFT, Value, Props, TS}} ->
            write_1(IFT, Value, Props, TS, Table),
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
    write_1(IFT, Value, Props, TS, Buffer#buffer.table),

    %% Return the new buffer.
    Buffer#buffer {
        size = (BytesWritten + Buffer#buffer.size)
    }.

write_1(IFT, Value, Props, TS, Table) ->
    %% Update and return the tree...
    Item = {Value, {Props, TS}},
    case ets:lookup(Table, IFT) of
        [] ->
            ets:insert(Table, {IFT, [Item]});
        [{IFT, Items}] ->
            case lists:keytake(Value, 1, Items) of
                false ->
                    %% Not found, insert the item.
                    ets:insert(Table, {IFT, [Item|Items]});
                {value, {_, {_, OldTS}}, NewItems} when OldTS < TS ->
                    %% Found, existing item removed, insert the new item.
                    ets:insert(Table, {IFT, [Item|NewItems]});
                {value, {_, {_, OldTS}}, _NewItems} when OldTS >= TS ->
                    %% Found, but we want to keep the existing item.
                    ok
            end
    end.

%% Return the number of results under this IFT.
info(IFT, Buffer) ->
    Table = Buffer#buffer.table,
    case ets:lookup(Table, IFT) of
        [{IFT, Values}] ->
            length(Values);
        [] ->
            0
    end.



%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(StartIFT, EndIFT, Buffer) ->
    Table = Buffer#buffer.table,
    IFT = mi_utils:ets_next(Table, StartIFT),
    info_1(Table, IFT, EndIFT, 0).
info_1(_Table, IFT, EndIFT, Count)
when IFT == '$end_of_table' orelse (EndIFT /= all andalso IFT > EndIFT) ->
    Count;
info_1(Table, IFT, EndIFT, Count) ->
    [{IFT, Values}] = ets:lookup(Table, IFT),
    NextIFT = ets:next(Table, IFT),
    info_1(Table, NextIFT, EndIFT, Count + length(Values)).

%% Return an iterator function.
%% Returns Fun/0, which then returns {Term, NewFun} or eof.
iterator(Buffer) ->
    iterator(undefined, undefined, Buffer).

%% Return an iterator function.
%% Returns Fun/0, which then returns {Term, NewFun} or eof.
iterator(StartIFT, EndIFT, Buffer) ->
    Table = Buffer#buffer.table,
    IFT = mi_utils:ets_next(Table, StartIFT),
    Iterator = {Table, IFT, EndIFT},
    fun() -> iterator_1(Iterator) end.

%% Iterate through IFTs...
iterator_1({_Table, IFT, EndIFT})
when IFT == '$end_of_table' orelse (EndIFT /= all andalso IFT > EndIFT) ->
    eof;
iterator_1({Table, IFT, EndIFT}) ->
    [{IFT, Values}] = ets:lookup(Table, IFT),
    SortedValues = lists:sort(Values),
    NextIFT = ets:next(Table, IFT),
    iterator_2(IFT, SortedValues, {Table, NextIFT, EndIFT}).

%% Iterate through values. at a values level...
iterator_2(IFT, [{Value, {Props, TS}}|Values], Continuation) ->
    Term = {IFT, Value, Props, TS},
    F = fun() -> iterator_2(IFT, Values, Continuation) end,
    {Term, F};
iterator_2(_IFT, [], Continuation) ->
    iterator_1(Continuation).

test() ->
    %% Write some stuff into the buffer...
    file:delete("/tmp/test_buffer"),
    Buffer = open("/tmp/test_buffer", []),
    Buffer1 = write(3, 11, [], 1, Buffer),
    Buffer2 = write(3, 11, [], 2, Buffer1),
    Buffer3 = write(1, 12, [], 1, Buffer2),
    Buffer4 = write(2, 13, [], 1, Buffer3),
    Buffer5 = write(2, 14, [], 1, Buffer4),

    %% Test iterator...
    FA = iterator(Buffer5),
    {{1, 12, [], 1}, FA1} = FA(),
    {{2, 13, [], 1}, FA2} = FA1(),
    {{2, 14, [], 1}, FA3} = FA2(),
    {{3, 11, [], 2}, FA4} = FA3(),
    eof = FA4(),

    %% Test partial iterator...
    FB = iterator(2, 2, Buffer5),
    {{2, 13, [], 1}, FB1} = FB(),
    {{2, 14, [], 1}, FB2} = FB1(),
    eof = FB2(),

    all_tests_passed.
