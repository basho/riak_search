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
    filename/1,
    delete/1,
    data_file/1,
    offsets_file/1,
    from_buffer/2,
    from_iterator/2,
    info/2,
    info/3,
    iterator/3,
    test/0
]).

-record(segment, {
    root,
    table
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
    Table = read_offsets(Root),

    %% Return the new segment...
    #segment { 
        root=Root, 
        table=Table
    }.

filename(Segment) ->
    Segment#segment.root.

delete(Segment) ->
    file:delete(data_file(Segment)),
    file:delete(offsets_file(Segment)),
    ets:delete(Segment#segment.table),
    ok.

%% Create a segment from a Buffer (see mi_buffer.erl)
from_buffer(Buffer, Segment) ->
    %% Open the iterator...
    Iterator = mi_buffer:iterator(Buffer),
    from_iterator(Iterator, Segment).

from_iterator(Iterator, Segment) ->
    %% Write to segment file in order...
    Table = Segment#segment.table,
    {ok, FH} = file:open(data_file(Segment), [write, {delayed_write, 1 * 1024 * 1024, 10 * 1000}, raw, binary]),
    from_iterator_inner(FH, 0, 0, 0, undefined, Iterator(), Table),
    file:close(FH),

    %% Write the offsets file...
    write_offsets(Segment).
    

from_iterator_inner(FH, Offset, Pos, Count, LastIFT, {{IFT, Value, Props, TS}, Iterator}, Table)
when LastIFT /= IFT ->
    %% Close the last keyspace if it existed...
    case Count > 0 of
        true -> ets:insert(Table, {LastIFT, {Offset, Count}});
        false -> ignore
    end,

    %% Start a new keyspace...
    NewOffset = Pos,
    BytesWritten1 = write_key(FH, IFT),
    BytesWritten2 = write_seg_value(FH, Value, Props, TS),
    NewPos = Pos + BytesWritten1 + BytesWritten2,
    from_iterator_inner(FH, NewOffset, NewPos, 1, IFT, Iterator(), Table);

from_iterator_inner(FH, Offset, Pos, Count, LastIFT, {{IFT, Value, Props, TS}, Iterator}, Table)
when LastIFT == IFT ->
    %% Write the new value...
    BytesWritten = write_seg_value(FH, Value, Props, TS),
    NewPos = Pos + BytesWritten,
    NewCount = Count + 1,
    from_iterator_inner(FH, Offset, NewPos, NewCount, LastIFT, Iterator(), Table);

from_iterator_inner(_FH, 0, 0, 0, undefined, eof, _Table) ->
    %% No input, so just finish.
    ok;

from_iterator_inner(_FH, Offset, _, Count, LastIFT, eof, Table) ->
    ets:insert(Table, {LastIFT, {Offset, Count}}).


%% return the number of results under this IFT.
info(IFT, Segment) ->
    Table = Segment#segment.table,
    case ets:lookup(Table, IFT) of
        [{IFT, {_Offset, Count}}] -> 
            Count;
        [] ->
            0
    end.

%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(StartIFT, EndIFT, Segment) ->
    Table = Segment#segment.table,
    IFT = mi_utils:ets_next(Table, StartIFT),
    info_1(Table, IFT, EndIFT, 0).
info_1(_Table, IFT, EndIFT, Count)
when IFT == '$end_of_table' orelse (EndIFT /= all andalso IFT > EndIFT) ->
    Count;
info_1(Table, IFT, EndIFT, Count) ->
    [{IFT, {_, NewCount}}] = ets:lookup(Table, IFT),
    NextIFT = ets:next(Table, IFT),
    info_1(Table, NextIFT, EndIFT, Count + NewCount).
    

%% Create a value iterator starting at StartIFT and stopping at
%% EndIFT. Returns an IteratorFun of arity zero that returns {Term,
%% NewIteratorFun} or 'eof' when called.
iterator(StartIFT, EndIFT, Segment) ->
    %% Lookup the nearest key after StartIFT.
    Table = Segment#segment.table,
    case mi_utils:ets_next(Table, StartIFT) of
        IFT when IFT /= '$end_of_table' ->
            [{IFT, {Offset, _}}] = ets:lookup(Table, IFT),
            {ok, FH} = file:open(data_file(Segment), [read, raw, read_ahead, binary]),
            file:position(FH, Offset),
            fun() -> iterator_fun(FH, undefined, EndIFT, []) end;
        '$end_of_table' ->
            fun() -> eof end
    end.
   
%% This works by reading a batch of 1000 results into a list, and then
%% serving those results up before reading the next 1000. This is done
%% to balance the tradeoff between open file handles and memory. If we didn't read
%% ahead and close the file handle, then all file handles involved in a stream
%% would remain open until the stream ended.
iterator_fun(_FH, _CurrentKey, _EndIFT, [eof]) ->
    eof;
iterator_fun(FH, CurrentKey, EndIFT, [H|T]) ->
    {H, fun() -> iterator_fun(FH, CurrentKey, EndIFT, T) end};
iterator_fun(FH, CurrentKey, EndIFT, []) ->
    {NewCurrentKey, Results} = iterator_fun_1(FH, CurrentKey, EndIFT, []),
    iterator_fun(FH, NewCurrentKey, EndIFT, Results).

%% Read the next batch of 1000 entries, or up to the end of the file, whichever
%% comes sooner.
iterator_fun_1(_, CurrentKey, _, Acc) when length(Acc) > 1000 ->
    {CurrentKey, lists:reverse(Acc)};
iterator_fun_1(FH, CurrentKey, EndIFT, Acc) ->
    case read_seg_value(FH) of
        {key, Key} when EndIFT == all orelse Key =< EndIFT ->
            iterator_fun_1(FH, Key, EndIFT, Acc);
        {key, Key} when Key > EndIFT ->
            file:close(FH),
            {CurrentKey, lists:reverse([eof|Acc])};
        {value, {Value, Props}, TS} ->
            Term = {CurrentKey, Value, Props, TS},
            iterator_fun_1(FH, CurrentKey, EndIFT, [Term|Acc]);
        eof ->
            file:close(FH),
            {CurrentKey, lists:reverse([eof|Acc])}
    end.




%% Read the offsets file from disk. If it's not found, then recreate
%% it from the data file. Return the offsets tree.
read_offsets(Root) ->
    case filelib:is_file(offsets_file(Root)) of
        true ->
            {ok, Table} = ets:file2tab(offsets_file(Root)),
            Table;
        false ->
            repair_offsets(Root)
    end.

repair_offsets(Root) ->
    case filelib:is_file(data_file(Root)) of
        true -> 
            {ok, FH} = file:open(data_file(Root), [read, read_ahead, raw, binary]),
            Table = ets:new(segment, [ordered_set, public]),
            repair_offsets_inner(FH, 0, 0, undefined, Table),
            file:close(FH),
            Table;
        false ->
            ets:new(segment, [ordered_set, public])
    end.

repair_offsets_inner(FH, Offset, Count, LastIFT, Table) ->
    Pos = file:position(FH, cur),
    case read_seg_value(FH) of
        {key, IFT} when LastIFT == undefined ->
            repair_offsets_inner(FH, Pos, 0, IFT, Table);
        {key, IFT} when LastIFT /= undefined ->
            ets:insert(Table, {LastIFT, {Offset, Count}}),
            repair_offsets_inner(FH, Pos, 0, IFT, Table);
        {value, _, _} ->
            repair_offsets_inner(FH, Offset, Count + 1, LastIFT, Table);
        eof when LastIFT == undefined ->
            ok;
        eof when LastIFT /= undefined ->
            ets:insert(Table, {LastIFT, {Offset, Count}}),
            ok
    end.
                        
write_offsets(Segment) ->
    Table = Segment#segment.table,
    ok = ets:tab2file(Table, offsets_file(Segment)).

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
