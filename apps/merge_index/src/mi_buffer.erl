%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at

%%   http://www.apache.org/licenses/LICENSE-2.0

%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(mi_buffer).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
    open/1,
    filename/1,
    close_filehandle/1,
    delete/1,
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
open(Filename) ->
    %% Make sure the file exists...
    case filelib:is_file(Filename) of
        true -> ok;
        false -> mi_utils:create_empty_file(Filename)
    end,
    
    %% Read existing buffer from disk...
    {ok, FH} = file:open(Filename, [read, write, {read_ahead, 1024 * 1024}, {delayed_write, 1024 * 1024, 10 * 1000}, raw, binary]),
    Table = ets:new(buffer, [ordered_set, public]),
    open_inner(FH, Table),
    {ok, Size} = file:position(FH, cur),
    io:format("Loaded Buffer: ~p~n", [Filename]),
    
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
    close_filehandle(Buffer),
    file:delete(Buffer#buffer.filename),
    ets:delete(Buffer#buffer.table),
    ok.

close_filehandle(Buffer) ->
    file:close(Buffer#buffer.handle).

%% Return the current size of the buffer file.
size(Buffer) ->
    Buffer#buffer.size.


%% Write the value to the buffer.
%% Returns the new buffer structure.
write(IFT, Value, Props, TS, Buffer) ->
    %% Write to file...
    FH = Buffer#buffer.handle,
    mi_utils:write_value(FH, {IFT, Value, Props, TS}),

    %% Return a new buffer with a new tree and size...
    write_1(IFT, Value, Props, TS, Buffer#buffer.table),
    {ok, NewSize} = file:position(FH, cur),
    Buffer#buffer {
        size = NewSize
    }.

write_1(IFT, Value, Props, TS, Table) ->
    %% Update and return the tree...
    case ets:lookup(Table, IFT) of
        [{IFT, Values}] ->
            case gb_trees:lookup(Value, Values) of
                {value, {_, OldTS}} when OldTS < TS ->
                    NewValues = gb_trees:update(Value, {Props, TS}, Values),
                    ets:insert(Table, {IFT, NewValues});
                {value, {_, OldTS}} when OldTS >= TS ->
                    ok;
                none ->
                    NewValues = gb_trees:insert(Value, {Props, TS}, Values),
                    ets:insert(Table, {IFT, NewValues})
            end;
        [] ->
            NewValues = gb_trees:from_orddict([{Value, {Props, TS}}]),
            ets:insert(Table, {IFT, NewValues})
    end.
    
%% Return the number of results under this IFT.
info(IFT, Buffer) ->
    Table = Buffer#buffer.table,
    case ets:lookup(Table, IFT) of
        [{IFT, Values}] -> 
            gb_trees:size(Values);
        [] ->
            0
    end.

%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(StartIFT, EndIFT, Buffer) ->
    Table = Buffer#buffer.table,
    MatchHead = {'$1', '$2'},
    MatchGuard = [{'>=', $1, StartIFT}, {'=<', $1, EndIFT}],
    Result = ['$_'],
    MatchSpec = [{MatchHead, MatchGuard, Result}],
    Results = ets:select(Table, MatchSpec),
    lists:sum([gb_trees:size(X) || {_, X} <- Results]).

%% Return an iterator function.
%% Returns Fun/0, which then returns {Term, NewFun} or eof.
iterator(Buffer) ->
    iterator(undefined, undefined, Buffer).

%% Return an iterator function.
%% Returns Fun/0, which then returns {Term, NewFun} or eof.
iterator(StartIFT, EndIFT, Buffer) ->
    Table = Buffer#buffer.table,
    case StartIFT == undefined of 
        true ->
            IFT = ets:first(Table),
            Iterator = {Table, IFT, EndIFT};
        false ->
            case ets:lookup(Table, StartIFT) of
                [{IFT, _Values}] ->
                    Iterator = {Table, IFT, EndIFT};
                [] ->
                    IFT = ets:next(Table, StartIFT),
                    Iterator = {Table, IFT, EndIFT}
            end
    end,
    fun() -> iterator_1(Iterator) end.

%% Iterate through IFTs...
iterator_1({_Table, IFT, EndIFT}) 
when IFT == '$end_of_table' orelse (EndIFT /= undefined andalso IFT > EndIFT) ->
    eof;
iterator_1({Table, IFT, EndIFT}) ->
    [{IFT, Values}] = ets:lookup(Table, IFT),
    ValuesIterator = gb_trees:next(gb_trees:iterator(Values)),
    NextIFT = ets:next(Table, IFT),
    iterator_2(IFT, ValuesIterator, {Table, NextIFT, EndIFT});
iterator_1('$end_of_table') ->
    eof.

%% Iterate through values. at a values level...
iterator_2(IFT, {Value, {Props, TS}, Iter}, Continuation) ->
    Term = {IFT, Value, Props, TS},
    F = fun() -> iterator_2(IFT, gb_trees:next(Iter), Continuation) end,
    {Term, F};
iterator_2(_IFT, none, Continuation) ->
    iterator_1(Continuation).

test() ->
    %% Write some stuff into the buffer...
    file:delete("/tmp/test_buffer"),
    Buffer = open("/tmp/test_buffer"),
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
