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
-module(mi_incdex).
-include("merge_index.hrl").
-author("Rusty Klophaus <rusty@basho.com>").
-export([
    open/2,
    clear/1,
    size/1,
    lookup/2,
    lookup_nocreate/2,
    lookup/3,
    select/3, 
    select/4,
    invert/1,
    test/0
]).

-record(incdex, {
    filename,
    options,
    handle,
    last_id,
    table
}).

%%% A memory-cached, disk-backed, sequentially-incrementing
%%% index. Upon a lookup, if the incdex does not already have the
%%% specified term, then the term is appended to the incdex file along
%%% with a new integer id.  IDs are sequential integers.

%% Open an incdex on the specified file.
%% Returns a new incdex structure.
open(Filename, Options) ->
    %% Open the existing incdex file...
    filelib:ensure_dir(Filename),
    ReadBuffer = 1024 * 1024,
    WriteInterval = proplists:get_value(write_interval, Options, 2 * 1000),
    WriteBuffer = proplists:get_value(write_buffer, Options, 1024 * 1024),
    {ok, FH} = file:open(Filename, [read, write, {read_ahead, ReadBuffer}, {delayed_write, WriteBuffer, WriteInterval}, raw, binary]),

    %% Read into an ets table...
    Table = ets:new(incdex, [ordered_set, public]),
    LastID = open_inner(FH, Table, 0),
    
    %% Return the incdex...
    #incdex { filename=Filename, options=Options, handle=FH, table=Table, last_id=LastID }.

%% Called by open. Puts each value of the incdex file into the
%% #incdex.tree.
open_inner(FH, Table, LastID) ->
    case mi_utils:read_value(FH) of
        {ok, {Key, ID}} ->
            ets:insert(Table, {Key, ID}),
            NewLastID = lists:max([ID, LastID]),
            open_inner(FH, Table, NewLastID);
        eof ->
            LastID
    end.

clear(Incdex) ->
    mi_utils:create_empty_file(Incdex#incdex.filename),
    open(Incdex#incdex.filename, Incdex#incdex.options).

size(Incdex) ->
    Table = Incdex#incdex.table,
    ets:info(Table, size).

%% Same as lookup(Key, Incdex, true).
lookup(Key, Incdex) ->
    lookup(Key, Incdex, true).

lookup_nocreate(Key, Incdex) ->
    Result = lookup(Key, Incdex, false),
    element(1, Result).

%% Returns the ID associated with a key, or creates it if it doesn't
%% exist. Writes out to the incdex file if a creation is needed.
lookup(Key, Incdex, true) ->
    %% Look up the entry...
    Table = Incdex#incdex.table,
    case ets:lookup(Table, Key) of 
        [{Key, ID}] ->
            {ID, Incdex};
        [] ->
            %% Write to table...
            ID = Incdex#incdex.last_id + 1,
            ets:insert(Table, {Key, ID}),

            %% Write to disk...
            FH = Incdex#incdex.handle,
            mi_utils:write_value(FH, {Key, ID}),

            %% Return...
            {ID, Incdex#incdex { last_id=ID }}
    end;

%% Returns the ID of the provided key, or 0 if it doesn't exist.
lookup(Key, Incdex, false) ->
    Table = Incdex#incdex.table,
    case ets:lookup(Table, Key) of
        [{Key, ID}] -> {ID, Incdex};
        [] -> {0, Incdex}
    end.

select(StartKey, EndKey, Incdex) ->
    select(StartKey, EndKey, all, Incdex).

select(StartKey, EndKey, Size, Incdex) ->
    Table = Incdex#incdex.table,
    Key = mi_utils:ets_next(Table, StartKey),
    Iterator = {Table, Key, EndKey},
    select_1(Iterator, Size, []).

select_1({_Table, Key, EndKey}, _Size, Acc) 
when Key == '$end_of_table' orelse (EndKey /= all andalso Key > EndKey) ->
    lists:reverse(Acc);
select_1({Table, Key, EndKey}, Size, Acc) ->
    NextKey = ets:next(Table, Key),
    case Size == all orelse typesafe_size(Key) == Size of
        true -> 
            [{Key, ID}] = ets:lookup(Table, Key),
            select_1({Table, NextKey, EndKey}, Size, [{Key, ID}|Acc]);
        false ->
            select_1({Table, NextKey, EndKey}, Size, Acc)
    end.

%% Normally, an incdex maps a key to some sequentially incremented ID
%% value. invert/1 inverts the index, returning a gb_tree where the
%% key and value are swapped. In other words, mapping the ID value to
%% the key.
invert(Incdex) ->
    Table = Incdex#incdex.table,
    F = fun({Key, ID}, AccIn) ->
        gb_trees:insert(Key, ID, AccIn)
    end,
    ets:foldl(F, gb_trees:empty(), Table).

typesafe_size(Term) when is_binary(Term) -> erlang:size(Term);
typesafe_size(Term) when is_list(Term) -> length(Term).

test() ->
    IncdexA = open("/tmp/test_incdex", []),
    
    %% Return 0 if something is not found...
    0 = lookup_nocreate("missing", IncdexA),

    %% Start incrementing at 1...
    {1, IncdexA1} = lookup("found1", IncdexA),
    {1, IncdexA2} = lookup("found1", IncdexA1, true),
    {1, IncdexA3} = lookup("found1", IncdexA2, false),

    %% Get the next value...
    {2, IncdexA4} = lookup("found2", IncdexA3),
    {2, IncdexA5} = lookup("found2", IncdexA4, true),
    {2, IncdexA6} = lookup("found2", IncdexA5, false),
    
    %% Missing should still be missing...
    0 = lookup_nocreate("missing", IncdexA6),

    %% Now, open another one...
    IncdexB = open("/tmp/test_incdex", []),
    1 = lookup_nocreate("found1", IncdexB),
    2 = lookup_nocreate("found2", IncdexB),
    all_tests_passed.
    
    
