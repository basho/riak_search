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
    close/1,
    clear/1,
    size/1,
    write/5,
    info/2, info/3,
    iterator/1, iterator/3,
    test/0
]).

-record(buffer, {
    filename,
    handle,
    tree,
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
    Tree = open_inner(FH, gb_trees:empty()),
    {ok, Size} = file:position(FH, cur),
    io:format("Loaded Buffer: ~p~n", [Filename]),
    
    %% Return the buffer.
    #buffer { filename=Filename, handle=FH, tree=Tree, size=Size }.

open_inner(FH, Tree) ->
    case mi_utils:read_value(FH) of
        {ok, {IFT, Value, Props, TS}} ->
            NewTree = write_1(IFT, Value, Props, TS, Tree),
            open_inner(FH, NewTree);
        eof ->
            Tree
    end.

clear(Buffer) ->
    close(Buffer),
    mi_utils:create_empty_file(Buffer#buffer.filename),
    open(Buffer#buffer.filename).
    

close(Buffer) ->
    file:close(Buffer#buffer.handle),
    ok.

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
    NewTree = write_1(IFT, Value, Props, TS, Buffer#buffer.tree),
    {ok, NewSize} = file:position(FH, cur),
    Buffer#buffer {
        tree = NewTree,
        size = NewSize
    }.

write_1(IFT, Value, Props, TS, Tree) ->
    %% Update and return the tree...
    case gb_trees:lookup(IFT, Tree) of
        {value, Values} ->
            case gb_trees:lookup(Value, Values) of
                {value, {_, OldTS}} when OldTS < TS ->
                    NewValues = gb_trees:update(Value, {Props, TS}, Values),
                    gb_trees:update(IFT, NewValues, Tree);
                {value, {_, OldTS}} when OldTS >= TS ->
                    Tree;
                none ->
                    NewValues = gb_trees:insert(Value, {Props, TS}, Values),
                    gb_trees:update(IFT, NewValues, Tree)
            end;
        none ->
            NewValues = gb_trees:from_orddict([{Value, {Props, TS}}]),
            gb_trees:insert(IFT, NewValues, Tree)
    end.
    
%% Return the number of results under this IFT.
info(IFT, Buffer) ->
    Tree = Buffer#buffer.tree,
    case gb_trees:lookup(IFT, Tree) of
        {value, Values} -> 
            gb_trees:size(Values);
        none ->
            0
    end.

%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(StartIFT, EndIFT, Buffer) ->
    Tree = Buffer#buffer.tree,
    KeyValues = mi_utils:select(StartIFT, EndIFT, Tree),
    lists:sum([gb_trees:size(X) || {_, X} <- KeyValues]).

%% Return an iterator function.
%% Returns Fun/0, which then returns {Term, NewFun} or eof.
iterator(Buffer) ->
    iterator(undefined, undefined, Buffer).

%% Return an iterator function.
%% Returns Fun/0, which then returns {Term, NewFun} or eof.
iterator(StartIFT, EndIFT, Buffer) ->
    Tree = Buffer#buffer.tree,
    Iterator = iterator_1(StartIFT, EndIFT, element(2, Tree), []),
    fun() -> iterator_2(Iterator) end.

%% Create an iterator structure from a tree...
iterator_1(StartIFT, EndIFT, {IFT, Values, Left, Right}, Acc) ->
    LBound = (StartIFT =< IFT orelse StartIFT == undefined),
    RBound = (EndIFT >= IFT orelse EndIFT == undefined),
    
    %% Possibly go right...
    NewAcc1 = case RBound of 
        true  -> iterator_1(StartIFT, EndIFT, Right, Acc);
        false -> Acc
    end,

    %% Possibly use the current pos...
    NewAcc2 = case LBound andalso RBound of
        true  -> [{IFT, Values}|NewAcc1];
        false -> NewAcc1
    end,

    %% Possibly go left...
    NewAcc3 = case LBound of
        true  -> iterator_1(StartIFT, EndIFT, Left, NewAcc2);
        false -> NewAcc2
    end,
    NewAcc3;
iterator_1(_, _, nil, Acc) ->
    Acc.

%% Iterate through IFTs...
iterator_2([{IFT, Values}|Rest]) ->
    ValuesIterator = gb_trees:next(gb_trees:iterator(Values)),
    iterator_3(IFT, ValuesIterator, Rest);
iterator_2([]) ->
    eof.

%% Iterate through values. at a values level...
iterator_3(IFT, {Value, {Props, TS}, Iter}, Rest) ->
    Term = {IFT, Value, Props, TS},
    F = fun() -> iterator_3(IFT, gb_trees:next(Iter), Rest) end,
    {Term, F};
iterator_3(_IFT, none, Rest) ->
    iterator_2(Rest).

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
