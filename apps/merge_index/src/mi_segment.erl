%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(mi_segment).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
    exists/1,
    open_read/1,
    open_write/3,
    filename/1,
    size/1,
    delete/1,
    data_file/1,
    offsets_file/1,
    from_buffer/2,
    from_iterator/2,
    info/2,
    info/3,
    iterator/1,
    iterator/3,
    test/0
]).

-record(segment, {
    root,
    table
}).

-define(TABLENAME, {now(), make_ref()}).

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
open_read(Root) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    case DataFileExists of
        true  -> 
            Table = read_offsets(Root),
            #segment { 
                root=Root, 
                table=Table
            };
        false ->
            throw({?MODULE, missing__file, Root})
    end.

open_write(Root, EstimatedMin, EstimatedMax) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    OffsetsFileExists = filelib:is_file(offsets_file(Root)),
    case DataFileExists orelse OffsetsFileExists of
        true  -> 
            throw({?MODULE, segment_already_exists, Root});
        false -> 
            mi_utils:create_empty_file(data_file(Root)),
            TableName = ?TABLENAME,
            Options = [{access, read_write}, {min_no_slots, EstimatedMin}, {max_no_slots, EstimatedMax}],
            Response = dets:open_file(TableName, [{file, offsets_file(Root)}] ++ Options),
            {ok, Table} = Response,
            #segment { 
                root=Root, 
                table=Table
            }
    end.

filename(Segment) ->
    Segment#segment.root.

size(Segment) ->
    dets:info(Segment#segment.table, size).

delete(Segment) ->
    ok = dets:close(Segment#segment.table),
    [ok = file:delete(X) || X <- filelib:wildcard(Segment#segment.root ++ ".*")],
    ok.

%% Create a segment from a Buffer (see mi_buffer.erl)
from_buffer(Buffer, Segment) ->
    %% Open the iterator...
    Iterator = mi_buffer:iterator(Buffer),
    from_iterator(Iterator, Segment).

from_iterator(Iterator, Segment) ->
    %% Write to segment file in order...
    Table = Segment#segment.table,
    WriteInterval = 5 * 1000,
    WriteBuffer = 5 * 1024 * 1024,
    {ok, FH} = file:open(data_file(Segment), [write, {delayed_write, WriteBuffer, WriteInterval}, raw, binary]),
    from_iterator_inner(FH, 0, 0, 0, undefined, undefined, Iterator(), Table),
    file:close(FH),

    %% Write the offsets file...
    write_offsets(Segment).
    

from_iterator_inner(FH, Offset, Pos, Count, LastIFT, _LastValue, {{IFT, Value, Props, TS}, Iterator}, Table)
when LastIFT /= IFT ->
    %% Close the last keyspace if it existed...
    case Count > 0 of
        true -> ok = dets:insert(Table, {LastIFT, {Offset, Count}});
        false -> ignore
    end,

    %% Start a new keyspace...
    NewOffset = Pos,
    BytesWritten1 = write_key(FH, IFT),
    BytesWritten2 = write_seg_value(FH, Value, Props, TS),
    NewPos = Pos + BytesWritten1 + BytesWritten2,
    from_iterator_inner(FH, NewOffset, NewPos, 1, IFT, Value, Iterator(), Table);

from_iterator_inner(FH, Offset, Pos, Count, LastIFT, LastValue, {{IFT, Value, Props, TS}, Iterator}, Table)
when LastIFT == IFT andalso LastValue /= Value ->
    %% Write the new value...
    BytesWritten = write_seg_value(FH, Value, Props, TS),
    NewPos = Pos + BytesWritten,
    NewCount = Count + 1,
    from_iterator_inner(FH, Offset, NewPos, NewCount, LastIFT, Value, Iterator(), Table);

from_iterator_inner(FH, Offset, Pos, Count, LastIFT, LastValue, {{IFT, Value, _Props, _TS}, Iterator}, Table)
when LastIFT == IFT andalso LastValue == Value ->
    %% Skip...
    from_iterator_inner(FH, Offset, Pos, Count, LastIFT, LastValue, Iterator(), Table);

from_iterator_inner(_FH, 0, 0, 0, undefined, undefined, eof, _Table) ->
    %% No input, so just finish.
    ok;

from_iterator_inner(_FH, Offset, _, Count, LastIFT, _LastValue, eof, Table) ->
    ok = dets:insert(Table, {LastIFT, {Offset, Count}}).


%% return the number of results under this IFT.
info(IFT, Segment) ->
    Table = Segment#segment.table,
    case dets:lookup(Table, IFT) of
        [{IFT, {_Offset, Count}}] -> 
            Count;
        [] ->
            0
    end.

%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(IFT, IFT, Segment) ->
    Table = Segment#segment.table,
    case dets:lookup(Table, IFT) of
        [{IFT, {_, Count}}] -> Count;
        _ -> 0
    end.
    
%% info(StartIFT, EndIFT, Segment) ->
%%     Table = Segment#segment.table,
%%     IFT = mi_utils:ets_next(Table, StartIFT),
%%     info_1(Table, IFT, EndIFT, 0).
%% info_1(_Table, IFT, EndIFT, Count)
%% when IFT == '$end_of_table' orelse (EndIFT /= all andalso IFT > EndIFT) ->
%%     Count;
%% info_1(Table, IFT, EndIFT, Count) ->
%%     %% TODO - Simplify this code. This is a holdover from previous
%%     %% versions of merge-index. We no longer need to be able to
%%     %% iterate across the backend.
%%     throw({?MODULE, unexpected_path, info_1}).
%%     [{IFT, {_, NewCount}}] = dets:lookup(Table, IFT),
%%     throw
%%     NextIFT = ets:next(Table, IFT),
%%     info_1(Table, NextIFT, EndIFT, Count + NewCount).
    

%% Create an iterator over the entire segment.
iterator(Segment) ->
    ReadBuffer = 5 * 1024 * 1024,
    {ok, FH} = file:open(data_file(Segment), [read, {read_ahead, ReadBuffer}, raw, binary]),
    fun() -> iterator_fun(FH, undefined, all, []) end.

%% Create a value iterator starting at StartIFT and stopping at
%% EndIFT. Returns an IteratorFun of arity zero that returns {Term,
%% NewIteratorFun} or 'eof' when called.

%% TODO - Clean this up. This is a holdover from legacy versions of
%% merge-index. We now only iterate in two modes, either the whole
%% table, or a single IFT.
iterator(_IFT, all, Segment) ->
    {ok, FH} = file:open(data_file(Segment), [read, raw, binary]),
    file:position(FH, 0),
    fun() -> iterator_fun(FH, undefined, all, []) end;
iterator(IFT, IFT, Segment) ->
    %% Lookup the nearest key after StartIFT.
    Table = Segment#segment.table,
    case dets:lookup(Table, IFT) of
        [{IFT, {Offset, _}}] ->
            {ok, FH} = file:open(data_file(Segment), [read, raw, binary]),
            file:position(FH, Offset),
            fun() -> iterator_fun(FH, undefined, IFT, []) end;
        [] ->
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
        {value, {Value, Props, TS}} ->
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
            {ok, Table} = dets:open_file(?TABLENAME, [{file, offsets_file(Root)}, {access, read}]),
            Table;
        false ->
            repair_offsets(Root)
    end.

repair_offsets(Root) ->
    case filelib:is_file(data_file(Root)) of
        true -> 
            ReadBuffer = 1024 * 1024,
            {ok, FH} = file:open(data_file(Root), [read, {read_ahead, ReadBuffer}, raw, binary]),
            {ok, Table} = dets:open_file(?TABLENAME, [{file, offsets_file(Root)}, {access, read_write}]),
            repair_offsets_inner(FH, 0, 0, undefined, Table),
            file:close(FH),
            dets:close(Table),
            read_offsets(Root);
        false ->
            throw({?MODULE, repair_offsets, not_found, data_file(Root)})
    end.

repair_offsets_inner(FH, Offset, Count, LastIFT, Table) ->
    Pos = file:position(FH, cur),
    case read_seg_value(FH) of
        {key, IFT} when LastIFT == undefined ->
            repair_offsets_inner(FH, Pos, 0, IFT, Table);
        {key, IFT} when LastIFT /= undefined ->
            ok = dets:insert(Table, {LastIFT, {Offset, Count}}),
            repair_offsets_inner(FH, Pos, 0, IFT, Table);
        {value, _} ->
            repair_offsets_inner(FH, Offset, Count + 1, LastIFT, Table);
        eof when LastIFT == undefined ->
            ok;
        eof when LastIFT /= undefined ->
            ok = dets:insert(Table, {LastIFT, {Offset, Count}}),
            ok
    end.
                        
write_offsets(Segment) ->
    Table = Segment#segment.table,
    ok = dets:close(Table).

%% Dialyzer says this clause is impossible.
%% read_seg_value(undefined) ->
%%     eof;
read_seg_value(FH) ->
    case file:read(FH, 2) of
        {ok, <<1:1/integer, Size:15/integer>>} ->
            {ok, B} = file:read(FH, Size),
            {key, B};
        {ok, <<0:1/integer, Size:15/integer>>} ->
            {ok, <<B/binary>>} = file:read(FH, Size),
            {value, binary_to_term(B)};
        eof ->
            eof
    end.

write_key(FH, Key) ->
    Size = erlang:size(Key),
    file:write(FH, <<1:1/integer, Size:15/integer, Key/binary>>),
    Size + 2.

write_seg_value(FH, Value, Props, TS) ->
    B = term_to_binary({Value, Props, TS}),
    Size = erlang:size(B),
    file:write(FH, <<0:1/integer, Size:15/integer, B/binary>>),
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
    BufferA = mi_buffer:open("/tmp/test_merge_index_bufferA", [write]),
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
    SegmentB = open_read(SegmentA#segment.root),
    1 = info(<<2>>, SegmentB),
    2 = info(<<4>>, SegmentB),
    4 = info(<<2>>, <<4>>, SegmentB),

    %% Test offset repair...
    file:delete(offsets_file(SegmentA)),
    SegmentC = open_read(SegmentA#segment.root),
    1 = info(<<2>>, SegmentC),
    2 = info(<<4>>, SegmentC),
    4 = info(<<2>>, <<4>>, SegmentC),

    all_tests_passed.
