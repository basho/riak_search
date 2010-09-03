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
    open_write/1,
    filename/1,
    filesize/1,
    delete/1,
    data_file/1,
    offsets_file/1,
    from_buffer/2,
    from_iterator/2,
    info/4,
    iterator/1,
    iterator/4
]).
%% Private/debugging/useful export.
-export([fold_iterator/3]).

-include("merge_index.hrl").
-include_lib("kernel/include/file.hrl").
-define(BLOCK_SIZE, 65536).
-define(BLOOM_CAPACITY, 512).
-define(BLOOM_ERROR, 0.01).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.


-record(segment, { root,
                   offsets_table }).

-record(writer, { data_file,
                  offsets_table,
                  start_pos = 0,
                  pos = 0,
                  last_offset = 0,
                  offsets = [],
                  count = 0,
                  last_index,
                  last_field,
                  last_term,
                  last_value,
                  bloom}).


exists(Root) ->
    filelib:is_file(data_file(Root)).

%% Create and return a new segment structure.
open_read(Root) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    case DataFileExists of
        true  ->
            OffsetsTable = read_offsets(Root),
            #segment {
                       root=Root,
                       offsets_table=OffsetsTable
                     };
        false ->
            throw({?MODULE, missing__file, Root})
    end.

open_write(Root) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    OffsetsFileExists = filelib:is_file(offsets_file(Root)),
    case DataFileExists orelse OffsetsFileExists of
        true  ->
            throw({?MODULE, segment_already_exists, Root});
        false ->
            %% TODO: Do we really need to go through the trouble of writing empty files here?
            file:write_file(data_file(Root), <<"">>),
            file:write_file(offsets_file(Root), <<"">>),
            #segment {
                       root = Root,     
                       offsets_table = ets:new(segment_offsets, [ordered_set, public])
                     }
    end.

filename(Segment) ->
    Segment#segment.root.

filesize(Segment) ->
    {ok, FileInfo} = file:read_file_info(data_file(Segment)),
    FileInfo#file_info.size.

delete(Segment) ->
    [ok = file:delete(X) || X <- filelib:wildcard(Segment#segment.root ++ ".*")],
    ets:delete(Segment#segment.offsets_table),
    ok.

%% Create a segment from a Buffer (see mi_buffer.erl)
from_buffer(Buffer, Segment) ->
    %% Open the iterator...
    Iterator = mi_buffer:iterator(Buffer),
    from_iterator(Iterator, Segment).

from_iterator(Iterator, Segment) ->
    %% Open the datafile...
    {ok, WriteOpts} = application:get_env(merge_index, segment_write_options),
    Opts = [write, raw, binary] ++ WriteOpts,
    {ok, DataFile} = file:open(data_file(Segment), Opts),
    mi_write_cache:setup(DataFile),

    %% Open the offset table...
    W = #writer { count = 0,
                  start_pos = 0,
                  last_offset = 0,
                  offsets = [],
                  data_file = DataFile,
                  bloom = mi_bloom:bloom(?BLOOM_CAPACITY, ?BLOOM_ERROR),
                  offsets_table = Segment#segment.offsets_table},
    try
        Wfinal = from_iterator_inner(W, Iterator()),
        mi_write_cache:flush(Wfinal#writer.data_file),
        ok = ets:tab2file(Wfinal#writer.offsets_table, offsets_file(Segment))
    after
        mi_write_cache:purge(W#writer.data_file),
        file:close(W#writer.data_file),
        ets:delete(W#writer.offsets_table)
    end,
    Segment.



from_iterator_inner(W, {{Index, Field, Term, Value, Props, TS}, Iterator})
  when (W#writer.last_term /= Term) orelse (W#writer.last_field /= Field) orelse (W#writer.last_index /= Index) ->
    %% If we've exceeded the block size, write an entry to the offsets
    %% file. The offsets entry contains entries less than (but not
    %% equal to) the Key.
    Key = {Index, Field, Term},
    case (W#writer.pos - W#writer.start_pos > ?BLOCK_SIZE) of
        true ->
            Offsets = tl(lists:reverse(W#writer.offsets)),
            ets:insert(W#writer.offsets_table, {Key, W#writer.start_pos, Offsets, W#writer.bloom}),
            W1 = W#writer { count = 1,
                            start_pos = W#writer.pos,
                            last_offset = W#writer.pos,
                            offsets = [],
                            bloom=mi_bloom:bloom(?BLOOM_CAPACITY, ?BLOOM_ERROR)};
        false ->
            W1 = W
    end,

    %% Start a new keyspace...
    BytesWritten1 = write_key(W#writer.data_file, Index, Field, Term),
    BytesWritten2 = write_seg_value(W#writer.data_file, Value, Props, TS),
    W2 = W1#writer { last_offset = W1#writer.pos + BytesWritten1,
                     offsets = [(W1#writer.pos-W1#writer.last_offset)|W1#writer.offsets],
                     pos = W1#writer.pos + BytesWritten1 + BytesWritten2,
                     last_index = Index,
                     last_field = Field,
                     last_term = Term,
                     last_value = Value,
                     bloom = mi_bloom:add_element(Key, W1#writer.bloom)},
    IteratorResult = Iterator(),
    from_iterator_inner(W2, IteratorResult);

from_iterator_inner(W, {{Index, Field, Term, Value, Props, TS}, Iterator})
  when (W#writer.last_term == Term) andalso (W#writer.last_field == Field) andalso (W#writer.last_index == Index) andalso (W#writer.last_value /= Value) ->
    %% Write the new value...
    BytesWritten = write_seg_value(W#writer.data_file, Value, Props, TS),
    W1 = W#writer { pos = W#writer.pos + BytesWritten,
                    count = W#writer.count + 1,
                    last_value = Value},
    from_iterator_inner(W1, Iterator());

from_iterator_inner(W, {{Index, Field, Term, Value, _Props, _TS}, Iterator})
  when (W#writer.last_term == Term) andalso (W#writer.last_field == Field) andalso (W#writer.last_index == Index) andalso (W#writer.last_value == Value) ->
    %% Skip...
    from_iterator_inner(W, Iterator());

from_iterator_inner(#writer { pos = 0, count=0, last_index = undefined, last_field=undefined, last_term=undefined } = W, eof) ->
    %% No input, so just finish.
    W;

from_iterator_inner(W, eof) ->
    Key = {W#writer.last_index, W#writer.last_field, W#writer.last_term},
    Offsets = tl(lists:reverse(W#writer.offsets)),
    ets:insert(W#writer.offsets_table, {Key, W#writer.start_pos, Offsets, W#writer.bloom}),
    W#writer { count=0 }.

%% return the number of results under this IFT.
%% TODO - Fix this.
info(Index, Field, Term, Segment) ->
    Key = {Index, Field, Term},
    case ets:next(Segment#segment.offsets_table, Key) of
        '$end_of_table' -> 
            0;
        OffsetKey ->
            [{_, _, _, Bloom}] = ets:lookup(Segment#segment.offsets_table, OffsetKey),
            case mi_bloom:is_element(Key, Bloom) of
                true  -> 1;
                false -> 0
            end
    end.

%% %% Return the number of results for IFTs between the StartIFT and
%% %% StopIFT, inclusive.
%% info(StartIFT, StopIFT, Segment) ->
%%     mi_nif:segidx_ift_count(Segment#segment.segidx, StartIFT, StopIFT).

%% Create an iterator over the entire segment.
iterator(Segment) ->
    {ok, ReadOpts} = application:get_env(merge_index, segment_read_options),
    {ok, FH} = file:open(data_file(Segment), [read, raw, binary] ++ ReadOpts),
    fun() -> iterate_all(FH, undefined) end.

%%% Create an iterator over the entire segment.
iterate_all(File, LastKey) ->
    case read_seg_value(File) of
        {key, Key} ->
            iterate_all(File, Key);
        {value, {Value, Props, Ts}} ->
            {Index, Field, Term} = LastKey,
            {{Index, Field, Term, Value, Props, Ts},
             fun() -> iterate_all(File, LastKey) end};
        _ ->
            file:close(File),
            eof
    end.


%% Create an iterator over a single Term.
iterator(Index, Field, Term, Segment) ->
    Key = {Index, Field, Term},
    case ets:next(Segment#segment.offsets_table, Key) of
        '$end_of_table' -> 
            fun() -> eof end;
        OffsetKey ->
            [{_, StartPos, Offsets, Bloom}] = ets:lookup(Segment#segment.offsets_table, OffsetKey),
            case mi_bloom:is_element(Key, Bloom) of
                true ->
                    {ok, ReadOpts} = application:get_env(merge_index, segment_read_options),
                    {ok, FH} = file:open(data_file(Segment), [read, raw, binary] ++ ReadOpts),
                    file:position(FH, StartPos),
                    fun() -> iterate_term(FH, Offsets, Key) end;
                false ->
                    fun() -> eof end
            end
    end.

%% Iterate over the segment file until we find the start of the values
%% section we want.
iterate_term(File, [Offset|Offsets], Key) ->
    case read_seg_value(File) of
        {key, CurrKey} ->
            %% Value should be a key, otherwise error. If the key is
            %% smaller than the one we need, keep jumping. If it's the
            %% one we need, then iterate values. Otherwise, it's too
            %% big, so close the file and return.
            if 
                CurrKey < Key ->
                    file:read(File, Offset),
                    iterate_term(File, Offsets, Key);
                CurrKey == Key ->
                    iterate_term_values(File, Key);
                CurrKey > Key ->
                    file:close(File),
                    eof
            end;
        _ ->
            %% Shouldn't get here. If we're here, then the Offset
            %% values are broken in some way.
            throw({iterate_term, offset_fail})
    end;
iterate_term(File, [], Key) ->
    %% We're at the last key in the file. If it matches
    %% our key, then iterate the values, otherwise close the file
    %% handle.
    case read_seg_value(File) of
        {key, CurrKey} when CurrKey == Key ->
            iterate_term_values(File, Key);
        _ ->
            file:close(File),
            eof
    end.

iterate_term_values(File, Key) ->
    case read_seg_value(File) of
        {value, {Value, Props, Ts}} ->
            {{Value, Props, Ts},
             fun() -> iterate_term_values(File, Key) end};
        _ ->
            file:close(File),
            eof
    end.


%% Read the offsets file from disk. If it's not found, then recreate
%% it from the data file. Return the offsets tree.
read_offsets(Root) ->
    case ets:file2tab(offsets_file(Root)) of
        {ok, OffsetsTable} ->
            OffsetsTable;
        {error, Reason} ->
            %% File doesn't exist -- rebuild it
            throw({?MODULE, {offsets_file_error, Reason}})
    end.


read_seg_value(FH) ->
    case file:read(FH, 4) of
        {ok, <<1:1/integer, Size:31/integer>>} ->
            {ok, B} = file:read(FH, Size),
            {key, binary_to_term(B)};
        {ok, <<0:1/integer, Size:31/integer>>} ->
            {ok, <<B/binary>>} = file:read(FH, Size),
            {value, binary_to_term(B)};
        eof ->
            eof
    end.

write_key(FH, Index, Field, Term) ->
    B = term_to_binary({Index, Field, Term}),
    Size = size(B),
    ok = mi_write_cache:write(FH, <<1:1/integer, Size:31/unsigned, B/binary>>),
    Size + 4.

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
                from_buffer(Buffer, open_write(Root ++ "_segment")),
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
