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
    open/1,
    lookup/2,
    lookup_nocreate/2,
    lookup/3,
    select/3, 
    select/4,
    test/0
]).

-record(incdex, {
    last_id=0,
    tree=gb_trees:empty(),
    filename
}).

%%% A memory-cached, disk-backed, sequentially-incrementing
%%% index. Upon a lookup, if the incdex does not already have the
%%% specified term, then the term is appended to the incdex file along
%%% with a new integer id.  IDs are sequential integers.

%% Open an incdex on the specified file.
%% Returns a new incdex structure.
open(Filename) ->
    filelib:ensure_dir(Filename),
    Incdex = #incdex { filename=Filename },

    case filelib:is_file(Filename) of
        true ->
            {ok, FH} = file:open(Filename, [read, read_ahead, raw, binary]),
            {ok, NewIncdex} = open_inner(FH, Incdex),
            file:close(FH),
            NewIncdex;
        false -> 
            Incdex
    end.

%% Called by open. Puts each value of the incdex file into the
%% #incdex.tree.
open_inner(FH, Incdex) ->
    Tree = Incdex#incdex.tree,
    case mi_utils:read_value(FH) of
        {ok, {Key, ID}} ->
            LastID = Incdex#incdex.last_id,
            NewIncdex = Incdex#incdex { 
                last_id=lists:max([ID, LastID]),
                tree=gb_trees:insert(Key, ID, Tree)
            },
            open_inner(FH, NewIncdex);
        eof ->
            {ok, Incdex}
    end.

%% Same as lookup(Key, Incdex, true).
lookup(Key, Incdex) ->
    lookup(Key, Incdex, true).

lookup_nocreate(Key, Incdex) ->
    Result = lookup(Key, Incdex, false),
    element(1, Result).

%% Returns the ID associated with a key, or creates it if it doesn't
%% exist. Writes out to the incdex file if a creation is needed.
lookup(Key, Incdex, true) ->
    Tree = Incdex#incdex.tree,
    Filename = Incdex#incdex.filename,
    case gb_trees:lookup(Key, Tree) of 
        {value, ID} -> 
            {ID, Incdex};
        none ->
            ID = Incdex#incdex.last_id + 1,
            NewTree = gb_trees:enter(Key, ID, Tree),
            {ok, FH} = file:open(Filename, [append, raw, binary]),
            mi_utils:write_value(FH, {Key, ID}),
            file:close(FH),
            {ID, Incdex#incdex { last_id=ID, tree=NewTree }}
    end;

%% Returns the ID of the provided key, or 0 if it doesn't exist.
lookup(Key, Incdex, false) ->
    Tree = Incdex#incdex.tree,
    case gb_trees:lookup(Key, Tree) of
        {value, ID} -> {ID, Incdex};
        none -> {0, Incdex}
    end.

select(StartKey, EndKey, Incdex) ->
    select(StartKey, EndKey, undefined, Incdex).
select(StartKey, EndKey, Size, Incdex) ->
    Tree = Incdex#incdex.tree,
    select_1(StartKey, EndKey, Size, element(2, Tree), []).
select_1(StartKey, StopKey, Size, {Key, Value, Left, Right}, Acc) ->
    LBound = (StartKey =< Key orelse StartKey == undefined),
    RBound = (StopKey >= Key orelse StopKey == undefined),
    SizeBound = Size == undefined orelse Size == typesafe_size(Key),

    %% Possibly go right...
    NewAcc1 = case RBound of
        true -> select_1(StartKey, StopKey, Size, Right, Acc);
        false -> Acc
    end,
    
    %% See if we match...
    NewAcc2 = case LBound andalso RBound andalso SizeBound of
        true  -> [{Key, Value}|NewAcc1];
        false -> NewAcc1
    end,

    %% Possibly go left...
    NewAcc3 = case LBound of
        true  -> select_1(StartKey, StopKey, Size, Left, NewAcc2);
        false -> NewAcc2
    end,
    NewAcc3;
select_1(_, _, _, nil, Acc) -> 
    Acc.

typesafe_size(Term) when is_binary(Term) -> size(Term);
typesafe_size(Term) when is_list(Term) -> length(Term).

test() ->
    IncdexA = open("/tmp/test_incdex"),
    
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
    IncdexB = open("/tmp/test_incdex"),
    1 = lookup_nocreate("found1", IncdexB),
    2 = lookup_nocreate("found2", IncdexB),
    all_tests_passed.
    
    
