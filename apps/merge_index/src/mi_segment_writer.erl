%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_segment_writer).
-author("Rusty Klophaus <rusty@basho.com>").

-export([
    from_iterator/2,
    edit_signature/2,
    longest_prefix/2
]).

-include("merge_index.hrl").

-include_lib("kernel/include/file.hrl").
-define(BLOCK_SIZE, 65536).
-define(BUFFER_SIZE, 10485760).
-define(VALUES_STAGING_SIZE, 10000).
-define(VALUES_COMPRESSION_THRESHOLD, 5).
-define(BLOOM_CAPACITY, 512).
-define(BLOOM_ERROR, 0.01).
-define(INDEX_FIELD_TERM(X), {element(1, X), element(2, X), element(3, X)}).
-define(VALUE(X), element(4, X)).
-define(VALUE_PROPS_TSTAMP(X), {element(4, X), element(5, X), element(6, X)}).

-compile({inline,[edit_signature/2, edit_signature_1/2, 
                  longest_prefix/2, longest_prefix_1/2]}).

-record(segment, { root,
                   offsets_table }).

-record(writer, {data_file,
                 offsets_table,
                 zstream,
                 pos=0,
                 block_start=0,
                 keys=[],
                 key_start=0,
                 values_start=0,
                 values_count=0,
                 values_staging=[],
                 compressed_values=false,
                 buffer=[],
                 buffer_size=0
         }).

from_iterator(Iterator, Segment) ->
    %% Open the data file...
    {ok, WriteOpts} = application:get_env(merge_index, segment_write_options),
    Opts = [write, raw, binary] ++ WriteOpts,
    {ok, DataFile} = file:open(data_file(Segment), Opts),

    %% Open the zlib stream...
    ZStream=zlib:open(),
    ok = zlib:deflateInit(ZStream, best_speed),

    W = #writer {
      data_file=DataFile,
      offsets_table=Segment#segment.offsets_table,
      zstream=ZStream
     },

    try
        from_iterator(Iterator(), undefined, undefined, W),
        ok = ets:tab2file(W#writer.offsets_table, offsets_file(Segment))
    after
        zlib:close(W#writer.zstream),
        file:close(W#writer.data_file),
        ets:delete(W#writer.offsets_table)
    end,    
    ok.
    

data_file(Segment) when is_record(Segment, segment) ->
    data_file(Segment#segment.root);
data_file(Root) ->
    Root ++ ".data".

offsets_file(Segment) when is_record(Segment, segment) ->
    offsets_file(Segment#segment.root);
offsets_file(Root) ->
    Root ++ ".offsets".

%% from_iterator_inner/4 - responsible for taking an iterator,
%% removing duplicate values, and creating start_segment, start_term,
%% value, end_term, and end_segment directives. Fun(Directive) is
%% called and should return NewFun. Function clauses are ordered for
%% speed, most common clauses first.
from_iterator({Entry, Iterator}, StartIFT, LastValue, W) 
  when ?INDEX_FIELD_TERM(Entry) == StartIFT andalso
       ?VALUE(Entry) /= LastValue ->
    %% Add the next value to the segment.
    W1 = from_iterator_process_value(?VALUE_PROPS_TSTAMP(Entry), W),
    from_iterator(Iterator(), StartIFT, ?VALUE(Entry), W1);

from_iterator({Entry, Iterator}, StartIFT, _LastValue, W) 
  when ?INDEX_FIELD_TERM(Entry) /= StartIFT andalso StartIFT /= undefined ->
    %% Start a new term.
    W1 = from_iterator_process_end_term(StartIFT, W),
    W2 = from_iterator_process_start_term(?INDEX_FIELD_TERM(Entry), W1),
    W3 = from_iterator_process_value(?VALUE_PROPS_TSTAMP(Entry), W2),
    from_iterator(Iterator(), ?INDEX_FIELD_TERM(Entry), ?VALUE(Entry), W3);

from_iterator({Entry, Iterator}, StartIFT, LastValue, W) 
  when ?INDEX_FIELD_TERM(Entry) == StartIFT andalso
       ?VALUE(Entry) == LastValue ->
    %% Eliminate a duplicate value.
    from_iterator(Iterator(), StartIFT, LastValue, W);

from_iterator({Entry, Iterator}, undefined, undefined, W) ->
    %% Start of segment.
    W1 = from_iterator_process_start_segment(W),
    W2 = from_iterator_process_start_term(?INDEX_FIELD_TERM(Entry), W1),
    W3 = from_iterator_process_value(?VALUE_PROPS_TSTAMP(Entry), W2),
    from_iterator(Iterator(), ?INDEX_FIELD_TERM(Entry), ?VALUE(Entry), W3);

from_iterator(eof, StartIFT, _LastValue, W) ->
    %% End of segment.
    W1 = from_iterator_process_end_term(StartIFT, W),
    W2 = from_iterator_process_end_segment(W1),
    W2.


%% One method below for each different stage of writing a segment: start_segment, start_block, start_term, value, end_term, end_block, end_segment.

from_iterator_process_start_segment(W) -> 
    from_iterator_process_start_block(W).

from_iterator_process_start_block(W) -> 
    %% Set the block_start position, and zero out keys for this block..
    W#writer {
      block_start = W#writer.pos,
      keys = []
     }.

from_iterator_process_start_term(Key, W) -> 
    %% Reset the zlib stream...
    ok = zlib:deflateReset(W#writer.zstream),

    %% Write the key entry to the data file...
    from_iterator_write_key(W, Key).

from_iterator_process_value(Value, W) ->
    %% Build up the set of values...
    W1 = W#writer { 
             values_staging=[Value|W#writer.values_staging],
             values_count = W#writer.values_count + 1
            },

    %% If we have enough, feed values through the ZStream and write to
    %% file. Return the updated #writer record...
    case length(W1#writer.values_staging) > ?VALUES_STAGING_SIZE of
        true -> 
            W2 = from_iterator_write_values(W1),
            W2;
        false ->
            W1
    end.

from_iterator_process_end_term(Key, W) -> 
    %% Write our remaining values...
    W1 = from_iterator_write_values_end(W),

    %% Add the key to state...
    KeySize = W#writer.values_start - W#writer.key_start,
    ValuesSize = W#writer.pos - W#writer.values_start,
    W2 = W1#writer {
           keys = [{Key, KeySize, ValuesSize, W#writer.values_count}|W1#writer.keys]
          },

    %% If this block is big enough, then close the old block and open
    %% a new block...
    case W2#writer.pos - W2#writer.block_start > ?BLOCK_SIZE of
        true ->
            W3 = from_iterator_process_end_block(W2),
            from_iterator_process_start_block(W3);
        false ->
            W2
    end.


from_iterator_process_end_block(W) when W#writer.keys /= [] -> 
    %% Calculate the bloom filter...
    F1 = fun({Key, _, _, _}, {BloomAcc, LongestPrefixAcc}) ->
                 {_, _, Term} = Key,
                 {mi_bloom:add_element(Key, BloomAcc), longest_prefix(LongestPrefixAcc, Term)}
        end,
    NewBloom = mi_bloom:bloom(?BLOOM_CAPACITY, ?BLOOM_ERROR),
    {Bloom, LongestPrefix} = lists:foldl(F1, {NewBloom, undefined}, W#writer.keys),

    %% Calculate offset table entries...
    {FinalKey, _, _, _} = hd(W#writer.keys),
    {_, _, FinalTerm} = FinalKey,
    F2 = fun({Key, KeySize, ValuesSize, Count}) ->
                 {_, _, Term} = Key,
                 {edit_signature(FinalTerm, Term), KeySize, ValuesSize, Count}
        end,
    KeyInfoList = [F2(X) || X <- W#writer.keys],
    
    %% Add entry to offsets table...
    Entry = {FinalKey, W#writer.block_start, Bloom, LongestPrefix, KeyInfoList},
    true = ets:insert(W#writer.offsets_table, Entry),
    W;
from_iterator_process_end_block(W) when W#writer.keys == [] -> 
    W.

from_iterator_process_end_segment(W) -> 
    W1 = from_iterator_process_end_block(W),
    from_iterator_flush_buffer(W1, true).


%% Write a key to the data file.
from_iterator_write_key(W, Key) ->
    Bytes = term_to_binary(Key),
    Size = erlang:size(Bytes),
    Output = [<<1:1/integer, Size:15/unsigned-integer>>, Bytes],
    W1 = W#writer {
           key_start = W#writer.pos,
           values_start = W#writer.pos + Size + 2,
           pos = W#writer.pos + Size + 2,
           buffer_size = W#writer.buffer_size + Size + 2,
           buffer = [Output|W#writer.buffer],
           compressed_values = false
          },
    from_iterator_flush_buffer(W1, false).

%% Write compressed values to the data file, don't flush zlib.
from_iterator_write_values(W) ->
    from_iterator_write_values_1(W, none).

%% Write compressed values to the data file, flush zlib.
from_iterator_write_values_end(W) ->
    from_iterator_write_values_1(W, finish).

%% Write compressed values to the data file.
from_iterator_write_values_1(W, Flush) 
  when (length(W#writer.values_staging) > ?VALUES_COMPRESSION_THRESHOLD) orelse 
       (W#writer.compressed_values == true) ->
    %% Run the values through compression.
    UncompressedBytes = term_to_binary(W#writer.values_staging),
    Bytes = zlib:deflate(W#writer.zstream, [UncompressedBytes], Flush),

    %% Write to the disk, get bytes written.
    Size = erlang:iolist_size(Bytes),
    Output = [<<0:1/integer, 1:1/integer, Size:30/unsigned-integer>>, Bytes],
    
    %% Update file position, and reset the values_staging.
    W1 = W#writer {
           pos = W#writer.pos + Size + 4,
           values_staging = [],
           compressed_values = true,
           buffer_size = W#writer.buffer_size + Size + 2,
           buffer = [Output|W#writer.buffer]
     },
    from_iterator_flush_buffer(W1, false);
from_iterator_write_values_1(W, true) 
  when length(W#writer.values_staging) == 0 ->
    W;
from_iterator_write_values_1(W, _Flush) ->
    %% Write to the disk, get bytes written.
    Bytes = term_to_binary(W#writer.values_staging),
    Size = erlang:iolist_size(Bytes),
    Output = [<<0:1/integer, 0:1/integer, Size:30/unsigned-integer>>, Bytes],
    
    %% Update file position, and reset the values_staging.
    W1 = W#writer {
           pos = W#writer.pos + Size + 4,
           values_staging = [],
           buffer_size = W#writer.buffer_size + Size + 2,
           buffer = [Output|W#writer.buffer]
     },
    from_iterator_flush_buffer(W1, false).

from_iterator_flush_buffer(W, Force) ->
    case (W#writer.buffer_size > ?BUFFER_SIZE) orelse Force of
        true ->
            ok = file:write(W#writer.data_file, lists:reverse(W#writer.buffer)),
            W#writer {
              buffer_size = 0,
              buffer = []
             };
        false ->
            W
    end.

%% longest_prefix/2 - Given two terms, calculate the longest common
%% prefix of the terms.
longest_prefix(A, B) ->
    list_to_binary(longest_prefix_1(A, B)).
longest_prefix_1(<<C, A/binary>>, <<C, B/binary>>) ->
    [C|longest_prefix_1(A, B)];
longest_prefix_1(undefined, B) ->
    [B];
longest_prefix_1(_, _) ->
    [].


%% edit_signature/2 - Given an A term and a B term, return a bitstring
%% consisting of a 0 bit for each matching char and a 1 bit for each
%% non-matching char.
edit_signature(A, B) ->
    list_to_bitstring(edit_signature_1(A, B)).
edit_signature_1(<<C, A/binary>>, <<C, B/binary>>) ->
    [<<0:1/integer>>|edit_signature_1(A, B)];
edit_signature_1(<<_, A/binary>>, <<_, B/binary>>) ->
    [<<1:1/integer>>|edit_signature_1(A, B)];
edit_signature_1(<<>>, <<_, B/binary>>) ->
    [<<1:1/integer>>|edit_signature_1(<<>>, B)];
edit_signature_1(<<_/binary>>, <<>>) ->
    [];
edit_signature_1(<<>>, <<>>) ->
    [].    

