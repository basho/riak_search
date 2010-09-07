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
    iterator/4,

    %% Used by QC tests, this is here to make compiler happy.
    fold_iterator/3
]).

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
    mi_segment_writer:from_iterator(Iterator, Segment).

from_iterator(Iterator, Segment) ->
    mi_segment_writer:from_iterator(Iterator, Segment).

%% Return the number of results under this IFT.
info(Index, Field, Term, Segment) ->
    Key = {Index, Field, Term},
    case ets:next(Segment#segment.offsets_table, Key) of
        '$end_of_table' -> 
            0;
        OffsetKey ->
            [{_, _, Bloom, _, _}] = ets:lookup(Segment#segment.offsets_table, OffsetKey),
            case mi_bloom:is_element(Key, Bloom) of
                true  -> 1;
                false -> 0
            end
    end.

%% Create an iterator over the entire segment.
iterator(Segment) ->
    {ok, ReadOpts} = application:get_env(merge_index, segment_read_options),
    {ok, FH} = file:open(data_file(Segment), [read, raw, binary] ++ ReadOpts),
    ZStream = zlib:open(),
    zlib:inflateInit(ZStream),
    fun() -> 
            iterate_all(FH, ZStream, undefined) 
    end.

%%% Iterate over the entire segment.
iterate_all(File, ZStream, {key, KeyBytes}) ->
    {I,F,T} = binary_to_term(KeyBytes),
    Transform = fun({V,P,K}) -> {I,F,T,V,P,K} end,
    WhenDone = fun(NextEntry) -> iterate_all(File, ZStream, NextEntry) end,
    iterate_values(File, ZStream, Transform, WhenDone);
iterate_all(File, ZStream, undefined) ->
    iterate_all(File, ZStream, read_seg_entry(File));
iterate_all(File, ZStream, eof) ->
    file:close(File),
    zlib:close(ZStream),
    eof.

%% Create an iterator over a single Term.
iterator(Index, Field, Term, Segment) ->
    %% Find the Key containing the offset information we need.
    Key = {Index, Field, Term},
    case get_offset_entry(Key, Segment) of
        undefined ->
            fun() -> eof end;
        {_, StartPos, Offsets, Bloom} ->
            %% If the Key exists in the bloom filter, then continue
            %% reading from the segment, otherwise, end.
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
    %% To simplify logic in this function, each Offsets list starts
    %% with 0. So for the first set of entries, this statement really
    %% does nothing. Also, a file:read/N operation is much much faster
    %% than a file:pseek/N operation, probably because read is using
    %% the read buffer.
    file:read(File, Offset),

    %% Read the next entry in the segment file.  Value should be a
    %% key, otherwise error. 
    case read_seg_entry(File) of
        {key, CurrKeyBytes} ->
            CurrKey = binary_to_term(CurrKeyBytes),
            %% If the key is smaller than the one we need, keep
            %% jumping. If it's the one we need, then iterate
            %% values. Otherwise, it's too big, so close the file and
            %% return.
            if 
                CurrKey < Key ->
                    iterate_term(File, Offsets, Key);
                CurrKey == Key ->
                    Transform = fun(Value) -> Value end,
                    ZStream = zlib:open(),
                    zlib:inflateInit(ZStream),
                    WhenDone = fun(_) -> zlib:close(ZStream), file:close(File), eof end,
                    iterate_values(File, ZStream, Transform, WhenDone);
                CurrKey > Key ->
                    file:close(File),
                    eof
            end;
        _ ->
            %% Shouldn't get here. If we're here, then the Offset
            %% values are broken in some way.
            file:close(File),
            throw({iterate_term, offset_fail})
    end.


%% iterators(Index, Field, StartTerm, EndTerm, Size, Segment) ->
%%     %% Find the Key containing the offset information we need.
%%     StartKey = {Index, Field, StartTerm},
%%     EndKey = {Index, Field, EndTerm},
%%     case get_offset_entry(StartKey, Segment) of
%%         undefined ->
%%             [fun() -> eof end];
%%         {_, StartPos, Offsets, Bloom} ->
%%             {ok, ReadOpts} = application:get_env(merge_index, segment_read_options),
%%             {ok, FH} = file:open(data_file(Segment), [read, raw, binary] ++ ReadOpts),
%%             file:position(FH, StartPos),
%%             fun() -> iterate_range(FH, Offsets, StartKey) end
%%     end.
    

%% PRIVATE FUNCTIONS


%% Given a key, look up the entry in the offsets table and return
%% {OffsetKey, StartPos, Offsets, Bloom} or 'undefined'.
get_offset_entry(Key, Segment) ->
    case ets:next(Segment#segment.offsets_table, Key) of
        '$end_of_table' -> 
            undefined;
        OffsetKey ->
            %% Look up the offset information.
            hd(ets:lookup(Segment#segment.offsets_table, OffsetKey))
    end.

iterate_values(File, ZStream, TransformFun, WhenDoneFun) ->
    iterate_values_1(File, ZStream, TransformFun, WhenDoneFun, false).
iterate_values_1(File, ZStream, TransformFun, WhenDoneFun, WasCompressed) ->
    %% Read the next value, expose as iterator.
    case read_seg_entry(File) of
        {values, <<>>} ->
            iterate_values_1(File, ZStream, TransformFun, WhenDoneFun, WasCompressed);
        {values, ValueBytes} ->
            Results = binary_to_term(ValueBytes),
            iterate_values_2(Results, File, ZStream, TransformFun, WhenDoneFun, WasCompressed);
        {compressed_values, <<>>} ->
            iterate_values_1(File, ZStream, TransformFun, WhenDoneFun, true);
        {compressed_values, CompressedValueBytes} ->
            ValueBytes = zlib:inflate(ZStream, CompressedValueBytes),
            Results = binary_to_term(list_to_binary([ValueBytes])),
            iterate_values_2(Results, File, ZStream, TransformFun, WhenDoneFun, true);
        Other when (WasCompressed == true) ->
            zlib:inflateReset(ZStream),
            WhenDoneFun(Other);
        Other when not WasCompressed ->
            WhenDoneFun(Other)
    end.
iterate_values_2([Result|Results], File, ZStream, TransformFun, WhenDoneFun, WasCompressed) ->
    {TransformFun(Result), fun() -> iterate_values_2(Results, File, ZStream, TransformFun, WhenDoneFun, WasCompressed) end};
iterate_values_2([], File, ZStream, TransformFun, WhenDoneFun, WasCompressed) ->
    iterate_values_1(File, ZStream, TransformFun, WhenDoneFun, WasCompressed).



%% Read the offsets file from disk. If it's not found, then recreate
%% it from the data file. Return the offsets tree.
read_offsets(Root) ->
    case ets:file2tab(offsets_file(Root)) of
        {ok, OffsetsTable} ->
            OffsetsTable;
        {error, Reason} ->
            %% TODO - File doesn't exist -- Rebuild it.
            throw({?MODULE, {offsets_file_error, Reason}})
    end.


read_seg_entry(FH) ->
    case file:read(FH, 1) of
        {ok, <<0:1/integer, 0:1/integer, Size1:6/integer>>} ->
            {ok, <<Size2:24/integer>>} = file:read(FH, 3),
            <<TotalSize:30/integer>> = <<Size1:6/integer, Size2:24/integer>>,
            {ok, B} = file:read(FH, TotalSize),
            {values, B};
        {ok, <<0:1/integer, 1:1/integer, Size1:6/integer>>} ->
            {ok, <<Size2:24/integer>>} = file:read(FH, 3),
            <<TotalSize:30/integer>> = <<Size1:6/integer, Size2:24/integer>>,
            {ok, B} = file:read(FH, TotalSize),
            {compressed_values, B};
        {ok, <<1:1/integer, Size1:7/integer>>} ->
            {ok, <<Size2:8/integer>>} = file:read(FH, 1),
            <<TotalSize:15/integer>> = <<Size1:7/integer, Size2:8/integer>>,
            {ok, B} = file:read(FH, TotalSize),
            {key, B};
        eof ->
            eof
    end.

    %% case file:read(FH, 4) of
    %%     {ok, <<1:1/integer, Size:31/integer>>} ->
    %%         {ok, B} = file:read(FH, Size),
    %%         {key, binary_to_term(B)};
    %%     {ok, <<0:1/integer, Size:31/integer>>} ->
    %%         {ok, <<B/binary>>} = file:read(FH, Size),
    %%         {value, binary_to_term(B)};
    %%     eof ->
    %%         eof
    %% end.

%% write_seg_key(FH, Key) ->
%%     B = term_to_binary(Key),
%%     Size = size(B),
%%     ok = mi_write_cache:write(FH, <<1:1/integer, Size:31/unsigned, B/binary>>),
%%     Size + 4.

%% write_seg_key(FH, Index, Field, Term) ->
%%     B = term_to_binary({Index, Field, Term}),
%%     Size = size(B),
%%     ok = mi_write_cache:write(FH, <<1:1/integer, Size:31/unsigned, B/binary>>),
%%     Size + 4.

%% write_seg_value(FH, Value, Props, TS) ->
%%     B = term_to_binary({Value, Props, TS}),
%%     Size = erlang:size(B),
%%     ok = mi_write_cache:write(FH, <<0:1/integer, Size:31/integer, B/binary>>),
%%     Size + 4.

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
