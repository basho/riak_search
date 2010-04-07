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

-module(mi_segment).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
    exists/1,
    open/1,
    from_buffer/2,
    info/2,
    info/3,
    iterator/3,
    test/0
]).

-record(segment, {
    root,
    tree
}).

%%% Creates a segment file, which is an disk-based ordered set with a
%%% gb_tree based index. Supports merging new values into the index
%%% (returns a new index) getting count information based on a key
%%% range, and streaming a key range. 
%%%
%%% Format is:
%%% 
%%% - A Key  - <<Size:16/integer, -1:64/integer, Key:Size/binary>>
%%% - Values - <<Size:16/integer, Timestamp:64/integer, Value:Size/binary>>
%%% - Repeat.
%%%

exists(Root) ->
    filelib:is_file(data_file(Root)).

%% Create and return a new segment structure.
open(Root) ->
    %% Create the file if it doesn't exist...
    case filelib:is_file(data_file(Root)) of
        true  -> ok;
        false -> mi_utils:create_empty_file(data_file(Root))
    end,

    %% Read the offsets...
    Offsets = read_offsets(Root),

    %% Return the new segment...
    #segment { 
        root=Root, 
        tree=Offsets
    }.


%% Create a segment from a Buffer (see mi_buffer.erl)
from_buffer(Root, Buffer) ->
    %% Open the iterator...
    Iterator = mi_buffer:iterator(Buffer),
    
    %% Write to buffer file in order...
    {ok, FH} = file:open(data_file(Root), [write, {delayed_write, 1 * 1024 * 1024, 10 * 1000}, raw, binary]),
    NewOffsets = from_buffer_inner(FH, 0, 0, 0, undefined, Iterator(), gb_trees:empty()),
    file:close(FH),

    %% Write the offsets file...
    offsets_write(Root, NewOffsets),

    %% Return the new segment.
    #segment { root=Root, tree=NewOffsets }.

from_buffer_inner(FH, Offset, Pos, Count, LastIFT, {{IFT, Value, Props, TS}, Iterator}, Offsets)
when LastIFT /= IFT ->
    %% Close the last keyspace if it existed...
    NewOffsets = case Count > 0 of
        true -> gb_trees:enter(LastIFT, {Offset, Count}, Offsets);
        false -> Offsets
    end,

    %% Start a new keyspace...
    NewOffset = Pos,
    BytesWritten1 = write_key(FH, IFT),
    BytesWritten2 = write_seg_value(FH, Value, Props, TS),
    NewPos = Pos + BytesWritten1 + BytesWritten2,
    from_buffer_inner(FH, NewOffset, NewPos, 1, IFT, Iterator(), NewOffsets);

from_buffer_inner(FH, Offset, Pos, Count, LastIFT, {{IFT, Value, Props, TS}, Iterator}, Offsets)
when LastIFT == IFT ->
    %% Write the new value...
    BytesWritten = write_seg_value(FH, Value, Props, TS),
    NewPos = Pos + BytesWritten,
    NewCount = Count + 1,
    from_buffer_inner(FH, Offset, NewPos, NewCount, LastIFT, Iterator(), Offsets);

from_buffer_inner(_FH, 0, 0, 0, undefined, eof, Offsets) ->
    %% No input, so just finish.
    Offsets;

from_buffer_inner(_FH, Offset, _, Count, LastIFT, eof, Offsets) ->
    gb_trees:enter(LastIFT, {Offset, Count}, Offsets).


%% Return the number of results under this IFT.
info(IFT, Segment) ->
    Tree = Segment#segment.tree,
    case gb_trees:lookup(IFT, Tree) of
        {value, {_Offset, Count}} -> 
            Count;
        none ->
            0
    end.

%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(StartIFT, EndIFT, Segment) ->
    Tree = Segment#segment.tree,
    KeyValues = select(StartIFT, EndIFT, Tree),
    lists:sum([Count || {_, {_Offset, Count}} <- KeyValues]).
    

%% Create a value iterator starting at StartIFT and stopping at
%% EndIFT. Returns an IteratorFun of arity zero that returns {Term,
%% NewIteratorFun} or 'eof' when called.
iterator(StartIFT, EndIFT, Segment) ->
    %% Lookup the nearest key after StartIFT.
    FH = case closest(StartIFT, Segment) of
        {value, {Offset, _}} ->
            {ok, Handle} = file:open(data_file(Segment), [read, raw, read_ahead, binary]),
            file:position(Handle, Offset),
            Handle;
        none ->
            undefined
    end,
    fun() -> iterator_fun(FH, undefined, EndIFT) end.

iterator_fun(FH, CurrentKey, EndIFT) ->
    case read_seg_value(FH) of
        {key, Key} when EndIFT == all orelse Key =< EndIFT ->
            iterator_fun(FH, Key, EndIFT);
        {key, Key} when Key > EndIFT ->
            eof;
        {value, {Value, Props}, TS} ->
            F = fun() -> iterator_fun(FH, CurrentKey, EndIFT) end,
            {{CurrentKey, Value, Props, TS}, F};
        eof ->
            file:close(FH),
            eof
    end.


%% Get the position of the provided key, or the next one up.  The big
%% change from gb_trees:lookup_1 is if we are going to the right side
%% of the tree, but the key is smaller than our last best key, then
%% "save" it. When we hit a dead end, return the last best key/value.
closest(Key, Segment) ->    
    Tree = Segment#segment.tree,
    closest_1(Key, undefined, undefined, element(2, Tree)).
closest_1(Key, BestKey, BestValue, {Key1, Value1, Smaller, _}) when Key =< Key1 ->
    case BestKey == undefined orelse Key1 < BestKey of
        true  -> closest_1(Key, Key1, Value1, Smaller);
        false -> closest_1(Key, BestKey, BestValue, Smaller)
    end;
closest_1(Key, BestKey, BestValue, {Key1, _, _, Bigger}) when Key > Key1 ->
    closest_1(Key, BestKey, BestValue, Bigger);
closest_1(_, _, _, {_, Value, _, _}) ->
    {value, Value};
closest_1(_, undefined, undefined, nil) ->
    none;
closest_1(_, _, BestValue, nil) ->
    {value, BestValue}.


%% Get the [{key, value}] list that falls within the provided range in a gb_tree.
%% Walk through a gb_tree, selecting values between a range. Special
%% case for a wildcard, where we convert a prefix to the values that
%% would come right before and right after it by adding -1 and 1,
%% respectively.
select(StartIFT, EndIFT, Tree) ->
    select_1(StartIFT, EndIFT, element(2, Tree), []).
select_1(StartIFT, EndIFT, {Key, Value, Left, Right}, Acc) ->
    LBound = (StartIFT =< Key),
    RBound = (EndIFT >= Key),

    %% Possibly go right...
    NewAcc1 = case RBound of
        true -> select_1(StartIFT, EndIFT, Right, Acc);
        false -> Acc
    end,
    
    %% See if we match...
    NewAcc2 = case LBound andalso RBound of
        true  -> [{Key, Value}|NewAcc1];
        false -> NewAcc1
    end,

    %% Possibly go left...
    NewAcc3 = case LBound of
        true  -> select_1(StartIFT, EndIFT, Left, NewAcc2);
        false -> NewAcc2
    end,
    NewAcc3;
select_1(_, _, nil, Acc) -> 
    Acc.


%% Read the offsets file from disk. If it's not found, then recreate
%% it from the data file. Return the offsets tree.
read_offsets(Root) ->
    case filelib:is_file(offsets_file(Root)) of
        true ->
            {ok, FH} = file:open(offsets_file(Root), [read, read_ahead, raw, binary]),
            {ok, Tree} = read_offsets_inner(FH, gb_trees:empty()),
            file:close(FH),
            Tree;
        false ->
            repair_offsets(Root)
    end.

read_offsets_inner(FH, Acc) ->
    case mi_utils:read_value(FH) of
        {ok, {IFT, Offset}} ->
            NewAcc = gb_trees:enter(IFT, Offset, Acc),
            read_offsets_inner(FH, NewAcc);
        eof ->
            {ok, Acc}
    end.

repair_offsets(Root) ->
    case filelib:is_file(data_file(Root)) of
        true -> 
            {ok, FH} = file:open(data_file(Root), [read, read_ahead, raw, binary]),
            {ok, Tree} = repair_offsets_inner(FH, 0, 0, undefined, gb_trees:empty()),
            file:close(FH),
            Tree;
        false ->
            gb_trees:empty()
    end.

repair_offsets_inner(FH, Offset, Count, LastIFT, Acc) ->
    Pos = file:position(FH, cur),
    case read_seg_value(FH) of
        {key, IFT} when LastIFT == undefined ->
            repair_offsets_inner(FH, Pos, 0, IFT, Acc);
        {key, IFT} when LastIFT /= undefined ->
            NewAcc = gb_trees:enter(LastIFT, {Offset, Count}, Acc),
            repair_offsets_inner(FH, Pos, 0, IFT, NewAcc);
        {value, _, _} ->
            repair_offsets_inner(FH, Offset, Count + 1, LastIFT, Acc);
        eof when LastIFT == undefined ->
            {ok, Acc};
        eof when LastIFT /= undefined ->
            NewAcc = gb_trees:enter(LastIFT, {Offset, Count}, Acc),
            {ok, NewAcc}
    end.
                        
offsets_write(Root, Offsets) ->
    {ok, FH} = file:open(offsets_file(Root), [write, {delayed_write, 1 * 1024 * 1024, 10 * 1000}, raw, binary]),
    Iter = gb_trees:next(gb_trees:iterator(Offsets)),
    offsets_write_inner(Iter, FH),
    file:close(FH).

offsets_write_inner({IFT, Offset, Iter}, FH) ->
    mi_utils:write_value(FH, {IFT, Offset}),
    offsets_write_inner(gb_trees:next(Iter), FH);
offsets_write_inner(none, _FH) ->
    ok.


read_seg_value(undefined) ->
    eof;
read_seg_value(FH) ->
    case file:read(FH, 2) of
        {ok, <<1:1/integer, Size:15/integer>>} ->
            {ok, B} = file:read(FH, Size),
            {key, B};
        {ok, <<0:1/integer, Size:15/integer>>} ->
            {ok, <<TS:64/integer, B/binary>>} = file:read(FH, Size),
            {value, binary_to_term(B), TS};
        eof ->
            eof
    end.

write_key(FH, Key) ->
    Size = erlang:size(Key),
    file:write(FH, <<1:1/integer, Size:15/integer, Key/binary>>),
    Size + 2.

write_seg_value(FH, Value, Props, TS) ->
    B1 = term_to_binary({Value, Props}),
    B2 = <<TS:64/integer, B1/binary>>,
    Size = erlang:size(B2),
    file:write(FH, <<0:1/integer, Size:15/integer, B2/binary>>),
    Size + 2.

data_file(Segment) when is_record(Segment, segment) ->
    data_file(Segment#segment.root);
data_file(Root) ->
    Root ++ ".data".

offsets_file(Segment) when is_record(Segment, segment) ->
    offsets_file(Segment#segment.root);
offsets_file(Root) ->
    Root ++ ".offsets".

test() ->
    %% Clean up old files...
    [file:delete(X) || X <- filelib:wildcard("/tmp/test_merge_index_*")],

    %% Create a buffer...
    BufferA = mi_buffer:open("/tmp/test_merge_index_bufferA"),
    BufferA1 = mi_buffer:write(<<1>>, 1, [], 1, BufferA),
    BufferA2 = mi_buffer:write(<<2>>, 2, [], 1, BufferA1),
    BufferA3 = mi_buffer:write(<<3>>, 3, [], 1, BufferA2),
    BufferA4 = mi_buffer:write(<<4>>, 4, [], 1, BufferA3),
    BufferA5 = mi_buffer:write(<<4>>, 5, [], 1, BufferA4),

    %% Merge into the segment...
    SegmentA = from_buffer("/tmp/test_merge_index_segment", BufferA5),
    
    %% Check the results...
    SegmentIteratorA = iterator(all, all, SegmentA),
    {{<<1>>, 1, [], 1}, SegmentIteratorA1} = SegmentIteratorA(),
    {{<<2>>, 2, [], 1}, SegmentIteratorA2} = SegmentIteratorA1(),
    {{<<3>>, 3, [], 1}, SegmentIteratorA3} = SegmentIteratorA2(),
    {{<<4>>, 4, [], 1}, SegmentIteratorA4} = SegmentIteratorA3(),
    {{<<4>>, 5, [], 1}, SegmentIteratorA5} = SegmentIteratorA4(),
    eof = SegmentIteratorA5(),

    %% Do a partial iterator...
    SegmentIteratorB = iterator(<<2>>, <<3>>, SegmentA),
    {{<<2>>, 2, [], 1}, SegmentIteratorB1} = SegmentIteratorB(),
    {{<<3>>, 3, [], 1}, SegmentIteratorB2} = SegmentIteratorB1(),
    eof = SegmentIteratorB2(),

    %% Check out the infos...
    1 = info(<<2>>, SegmentA),
    2 = info(<<4>>, SegmentA),
    4 = info(<<2>>, <<4>>, SegmentA),

    %% Read from an existing segment...
    SegmentB = open(SegmentA#segment.root),
    1 = info(<<2>>, SegmentB),
    2 = info(<<4>>, SegmentB),
    4 = info(<<2>>, <<4>>, SegmentB),

    %% Test offset repair...
    file:delete(offsets_file(SegmentA)),
    SegmentC = open(SegmentA#segment.root),
    1 = info(<<2>>, SegmentC),
    2 = info(<<4>>, SegmentC),
    4 = info(<<2>>, <<4>>, SegmentC),

    all_tests_passed.
