%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_segment).
-author("Rusty Klophaus <rusty@basho.com>").

-export([
    exists/1,
    open_read/1,
    open_write/3,
    filename/1,
    filesize/1,
    size/1,
    delete/1,
    data_file/1,
    offsets_file/1,
    from_buffer/2,
    from_iterator/2,
    info/2,
    info/3,
    iterator/1,
    iterator/3
]).
%% Private/debugging/useful export.
-export([fold_iterator/3]).

-include("merge_index.hrl").
-include_lib("kernel/include/file.hrl").

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.


-record(segment, { root,
                   segidx }).

-record(writer, { data_file,
                  offsets_file,
                  offsets_file_crc = 0,
                  offset,
                  pos = 0,
                  count = 0,
                  last_ift,
                  last_value }).


exists(Root) ->
    filelib:is_file(data_file(Root)).

%% Create and return a new segment structure.
open_read(Root) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    case DataFileExists of
        true  ->
            SegIdx = read_offsets(Root),
            #segment {
                       root=Root,
                       segidx=SegIdx
                     };
        false ->
            throw({?MODULE, missing__file, Root})
    end.

open_write(Root, _EstimatedMin, _EstimatedMax) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    OffsetsFileExists = filelib:is_file(offsets_file(Root)),
    case DataFileExists orelse OffsetsFileExists of
        true  ->
            throw({?MODULE, segment_already_exists, Root});
        false ->
            %% TODO: Do we really need to go through the trouble of writing empty files here?
            mi_utils:create_empty_file(data_file(Root)),
            mi_utils:create_empty_file(offsets_file(Root)),
            #segment { root = Root }
    end.

filename(Segment) ->
    Segment#segment.root.

filesize(Segment) ->
    {ok, FileInfo} = file:read_file_info(data_file(Segment)),
    FileInfo#file_info.size.

size(Segment) ->
    mi_nif:segidx_entry_count(Segment#segment.segidx).

delete(Segment) ->
    [ok = file:delete(X) || X <- filelib:wildcard(Segment#segment.root ++ ".*")],
    ok.

%% Create a segment from a Buffer (see mi_buffer.erl)
from_buffer(Buffer, Segment) ->
    %% Open the iterator...
    Iterator = mi_buffer:iterator(Buffer),
    from_iterator(Iterator, Segment).

from_iterator(Iterator, Segment) ->
    %% Write to segment file in order...
    {ok, WriteOpts} = application:get_env(merge_index, segment_write_options),
    Opts = [write, raw, binary] ++ WriteOpts,
    {ok, DataFile} = file:open(data_file(Segment), Opts),
    mi_write_cache:setup(DataFile),
    {ok, OffsetsFile} = file:open(offsets_file(Segment), Opts),
    mi_write_cache:setup(OffsetsFile),
    W = #writer { data_file = DataFile,
                  offsets_file = OffsetsFile },
    try
        Wfinal = from_iterator_inner(W, Iterator()),
        mi_write_cache:flush(Wfinal#writer.data_file),
        mi_write_cache:flush(Wfinal#writer.offsets_file),

        %% Write the final CRC out on the offsets file
        file:write(Wfinal#writer.offsets_file,
                   <<(Wfinal#writer.offsets_file_crc):32/native-unsigned>>)

    after
        mi_write_cache:purge(W#writer.data_file),
        mi_write_cache:purge(W#writer.offsets_file),
        file:close(W#writer.data_file),
        file:close(W#writer.offsets_file)
    end,
    Segment.


from_iterator_inner(W, {{IFT, Value, Props, TS}, Iterator})
  when (W#writer.last_ift /= IFT) ->

    %% Close the last keyspace if it existed...
    case (W#writer.count) > 0 of
        true ->
            OffsetInfo = <<(W#writer.last_ift):64/native-unsigned,
                           (W#writer.offset):64/native-unsigned,
                           (W#writer.count):32/native-unsigned>>,
            OffsetCrc = erlang:crc32(W#writer.offsets_file_crc, OffsetInfo),
            ok = mi_write_cache:write(W#writer.offsets_file, OffsetInfo);
        false ->
            OffsetCrc = W#writer.offsets_file_crc
    end,

    %% Start a new keyspace...
    BytesWritten1 = write_key(W#writer.data_file, IFT),
    BytesWritten2 = write_seg_value(W#writer.data_file, Value, Props, TS),
    W2 = W#writer { offsets_file_crc = OffsetCrc,
                    offset = W#writer.pos,
                    pos = W#writer.pos + BytesWritten1 + BytesWritten2,
                    count = 1,
                    last_ift = IFT,
                    last_value = Value},
    from_iterator_inner(W2, Iterator());

from_iterator_inner(W, {{IFT, Value, Props, TS}, Iterator})
  when W#writer.last_ift == IFT andalso W#writer.last_value /= Value ->
    %% Write the new value...
    BytesWritten = write_seg_value(W#writer.data_file, Value, Props, TS),
    W2 = W#writer { pos = W#writer.pos + BytesWritten,
                    count = W#writer.count + 1,
                    last_value = Value },
    from_iterator_inner(W2, Iterator());

from_iterator_inner(W, {{IFT, Value, _Props, _TS}, Iterator})
  when W#writer.last_ift == IFT andalso W#writer.last_value == Value ->
    %% Skip...
    from_iterator_inner(W, Iterator());

from_iterator_inner(#writer { pos = 0, count = 0, last_ift = undefined } = W, eof) ->
    %% No input, so just finish.
    W;

from_iterator_inner(W, eof) ->
    OffsetInfo = <<(W#writer.last_ift):64/native-unsigned,
                   (W#writer.offset):64/native-unsigned,
                   (W#writer.count):32/native-unsigned>>,
    OffsetCrc = erlang:crc32(W#writer.offsets_file_crc, OffsetInfo),
    ok = mi_write_cache:write(W#writer.offsets_file, OffsetInfo),
    W#writer { offsets_file_crc = OffsetCrc }.


%% return the number of results under this IFT.
info(IFT, Segment) ->
    mi_nif:segidx_ift_count(Segment#segment.segidx, IFT).

%% Return the number of results for IFTs between the StartIFT and
%% StopIFT, inclusive.
info(StartIFT, StopIFT, Segment) ->
    mi_nif:segidx_ift_count(Segment#segment.segidx, StartIFT, StopIFT).

%% Create an iterator over the entire segment.
iterator(Segment) ->
    {ok, ReadOpts} = application:get_env(merge_index, segment_read_options),
    {ok, FH} = file:open(data_file(Segment), [read, raw, binary] ++ ReadOpts),
    fun() -> iterate_segment(FH, undefined, undefined) end.

%% Create an iterator over an inclusive range of IFTs
iterator(StartIFT, EndIFT, Segment) ->
    case mi_nif:segidx_lookup_nearest(Segment#segment.segidx, StartIFT) of
        {ok, IFT0, Offset, _Count} ->
            %% Seek to the proper offset and start iteration
            {ok, ReadOpts} = application:get_env(merge_index, segment_read_options),
            {ok, FH} = file:open(data_file(Segment), [read, raw, binary] ++ ReadOpts),
            file:position(FH, Offset),
            fun() -> iterate_segment(FH, IFT0, EndIFT) end;
        not_found ->
            fun() -> eof end
    end.

%% Iterate over a segment file, starting at whatever the current position
%% of the provided file handle is. Release the file handle on completion.
iterate_segment(File, CurrIFT, StopIFT) ->
    case read_seg_value(File) of
        {key, IFT} when IFT > StopIFT ->
            file:close(File),
            eof;
        {key, IFT} ->
            iterate_segment(File, IFT, StopIFT);
        {value, {Value, Props, Ts}} ->
            {{CurrIFT, Value, Props, Ts},
             fun() -> iterate_segment(File, CurrIFT, StopIFT) end};
        eof ->
            file:close(File),
            eof
    end.

%% Read the offsets file from disk. If it's not found, then recreate
%% it from the data file. Return the offsets tree.
read_offsets(Root) ->
    case file:read_file(offsets_file(Root)) of
        {ok, Bin} ->
            %% Loaded the blob from disk; check the CRC
            DataSz = erlang:size(Bin) - 4,
            <<Data:DataSz/binary, Crc:32/native-unsigned>> = Bin,
            case erlang:crc32(Data) == Crc of
                true ->
                    %% CRC check passed -- initialize segidx
                    {ok, SegIdx} = mi_nif:segidx_new(Data),
                    SegIdx;
                false ->
                    %% CRC failed -- rebuild offsets file
                    throw({?MODULE, offsets_file_corrupt})
            end;
        {error, Reason} ->
            %% File doesn't exist -- rebuild it
            throw({?MODULE, {offsets_file_error, Reason}})
    end.


read_seg_value(FH) ->
    case file:read(FH, 4) of
        {ok, <<1:1/integer, Size:31/integer>>} ->
            {ok, <<IFT:64/unsigned>>} = file:read(FH, Size),
            {key, IFT};
        {ok, <<0:1/integer, Size:31/integer>>} ->
            {ok, <<B/binary>>} = file:read(FH, Size),
            {value, binary_to_term(B)};
        eof ->
            eof
    end.

write_key(FH, IFT) ->
    ok = mi_write_cache:write(FH, <<1:1/integer, 8:31/integer, IFT:64/unsigned>>),
    12.

write_seg_value(FH, Value, Props, TS) ->
    B = term_to_binary({Value, Props, TS}),
    Size = erlang:size(B),
    ok = mi_write_cache:write(FH, <<0:1/integer, Size:31/integer, B/binary>>),
    Size + 4.

data_file(Segment) when is_record(Segment, segment) ->
    data_file(Segment#segment.root);
data_file(Root) ->
    Root ++ ".data".

offsets_file(Segment) when is_record(Segment, segment) ->
    offsets_file(Segment#segment.root);
offsets_file(Root) ->
    Root ++ ".offsets".

fold_iterator(Itr, Fn, Acc0) ->
    fold_iterator_inner(Itr(), Fn, Acc0).

fold_iterator_inner(eof, _Fn, Acc) ->
    lists:reverse(Acc);
fold_iterator_inner({Term, NextItr}, Fn, Acc0) ->
    Acc = Fn(Term, Acc0),
    fold_iterator_inner(NextItr(), Fn, Acc).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

g_ift() ->
    choose(0, ?POW_2(62)).

g_value() ->
    non_empty(binary()).

g_props() ->
    list({oneof([word_pos, offset]), choose(0, ?POW_2(31))}).

g_tstamp() ->
    choose(0, ?POW_2(31)).

make_buffer([], B) ->
    B;
make_buffer([{Ift, Value, Props, Tstamp} | Rest], B0) ->
    B = mi_buffer:write(Ift, Value, Props, Tstamp, B0),
    make_buffer(Rest, B).


prop_basic_test(Root) ->
    ?FORALL(Entries, list({g_ift(), g_value(), g_props(), g_tstamp()}),
            begin
                %% Delete old files
                [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

                %% Setup a buffer
                Buffer = make_buffer(Entries, mi_buffer:new(Root ++ "_buffer")),

                %% Build a list of what was actually stored in the buffer -- this is what
                %% we expect to be present in the segment
                BufferEntries = fold_iterator(mi_buffer:iterator(Buffer),
                                              fun(Item, Acc0) -> [Item | Acc0] end, []),

                %% Merge the buffer into a segment
                from_buffer(Buffer, open_write(Root ++ "_segment", 0, 0)),
                Segment = open_read(Root ++ "_segment"),

                %% Fold over the entire segment
                SegEntries = fold_iterator(iterator(Segment), fun(Item, Acc0) -> [Item | Acc0] end, []),

                ?assertEqual(BufferEntries, SegEntries),
                true
            end).


prop_basic_test_() ->
    {timeout, 60, fun() ->
                          os:cmd("rm -rf /tmp/test_mi; mkdir -p /tmp/test_mi"),
                          ?assert(eqc:quickcheck(?QC_OUT(prop_basic_test("/tmp/test_mi/t1"))))
                  end}.


-endif. % EQC

-endif.

%% test() ->
%%     %% Clean up old files...
%%     [file:delete(X) || X <- filelib:wildcard("/tmp/test_merge_index_*")],

%%     %% Create a buffer...
%%     BufferA = mi_buffer:open("/tmp/test_merge_index_bufferA", [write]),
%%     BufferA1 = mi_buffer:write(<<1>>, 1, [], 1, BufferA),
%%     BufferA2 = mi_buffer:write(<<2>>, 2, [], 1, BufferA1),
%%     BufferA3 = mi_buffer:write(<<3>>, 3, [], 1, BufferA2),
%%     BufferA4 = mi_buffer:write(<<4>>, 4, [], 1, BufferA3),
%%     BufferA5 = mi_buffer:write(<<4>>, 5, [], 1, BufferA4),

%%     %% Merge into the segment...
%%     SegmentA = from_buffer("/tmp/test_merge_index_segment", BufferA5),
    
%%     %% Check the results...
%%     SegmentIteratorA = iterator(SegmentA),
%%     {{<<1>>, 1, [], 1}, SegmentIteratorA1} = SegmentIteratorA(),
%%     {{<<2>>, 2, [], 1}, SegmentIteratorA2} = SegmentIteratorA1(),
%%     {{<<3>>, 3, [], 1}, SegmentIteratorA3} = SegmentIteratorA2(),
%%     {{<<4>>, 4, [], 1}, SegmentIteratorA4} = SegmentIteratorA3(),
%%     {{<<4>>, 5, [], 1}, SegmentIteratorA5} = SegmentIteratorA4(),
%%     eof = SegmentIteratorA5(),

%%     %% Do a partial iterator...
%%     SegmentIteratorB = iterator(<<2>>, <<3>>, SegmentA),
%%     {{<<2>>, 2, [], 1}, SegmentIteratorB1} = SegmentIteratorB(),
%%     {{<<3>>, 3, [], 1}, SegmentIteratorB2} = SegmentIteratorB1(),
%%     eof = SegmentIteratorB2(),

%%     %% Check out the infos...
%%     1 = info(<<2>>, SegmentA),
%%     2 = info(<<4>>, SegmentA),
%%     4 = info(<<2>>, <<4>>, SegmentA),

%%     %% Read from an existing segment...
%%     SegmentB = open_read(SegmentA#segment.root),
%%     1 = info(<<2>>, SegmentB),
%%     2 = info(<<4>>, SegmentB),
%%     4 = info(<<2>>, <<4>>, SegmentB),

%%     %% Test offset repair...
%%     file:delete(offsets_file(SegmentA)),
%%     SegmentC = open_read(SegmentA#segment.root),
%%     1 = info(<<2>>, SegmentC),
%%     2 = info(<<4>>, SegmentC),
%%     4 = info(<<2>>, <<4>>, SegmentC),

%%     all_tests_passed.
