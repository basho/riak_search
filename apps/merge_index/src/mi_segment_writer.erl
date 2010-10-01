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
    from_iterator/2
]).

-include("merge_index.hrl").

-include_lib("kernel/include/file.hrl").
-define(BLOCK_SIZE(W), W#writer.segment_block_size).
-define(FILE_BUFFER_SIZE(W), W#writer.segment_file_buffer_size).
-define(VALUES_STAGING_SIZE(W), W#writer.segment_values_staging_size).
-define(VALUES_COMPRESS_THRESHOLD(W), W#writer.segment_values_compression_threshold).
-define(VALUES_COMPRESS_LEVEL(W), W#writer.segment_values_compression_level).
-define(INDEX_FIELD_TERM(X), {element(1, X), element(2, X), element(3, X)}).
-define(VALUE(X), element(4, X)).
-define(VALUE_TSTAMP_PROPS(X), {element(4, X), element(5, X), element(6, X)}).

-record(writer, {data_file,
                 offsets_table,
                 pos=0,
                 block_start=0,
                 keys=[],
                 last_key=undefined,
                 key_start=0,
                 values_start=0,
                 values_count=0,
                 values_staging=[],
                 compressed_values=false,
                 buffer=[],
                 buffer_size=0,
                 
                 %% We are caching these settings in the #writer state
                 %% to avoid looking them up repeatedly. This saves
                 %% quite a bit of time, as 100,000 lookups on my
                 %% computer is about 1/2 second.
                 segment_block_size=element(2,application:get_env(merge_index, segment_block_size)),
                 segment_file_buffer_size=element(2,application:get_env(merge_index, segment_file_buffer_size)),
                 segment_values_staging_size=element(2,application:get_env(merge_index, segment_values_staging_size)),
                 segment_values_compression_threshold=element(2,application:get_env(merge_index, segment_values_compression_threshold)),
                 segment_values_compression_level=element(2,application:get_env(merge_index, segment_values_compression_level))
         }).

from_iterator(Iterator, Segment) ->
    %% Open the data file...
    {ok, DelayedWriteSize} = application:get_env(merge_index, segment_delayed_write_size),
    {ok, DelayedWriteMS} = application:get_env(merge_index, segment_delayed_write_ms),
    {ok, DataFile} = file:open(mi_segment:data_file(Segment), [write, raw, binary, {delayed_write, DelayedWriteSize, DelayedWriteMS}]),

    W = #writer {
      data_file=DataFile,
      offsets_table=Segment#segment.offsets_table
     },

    try
        from_iterator(Iterator(), undefined, undefined, W),
        ok = ets:tab2file(W#writer.offsets_table, mi_segment:offsets_file(Segment))
    after
        file:close(W#writer.data_file),
        ets:delete(W#writer.offsets_table)
    end,    
    ok.
    

%% from_iterator_inner/4 - responsible for taking an iterator,
%% removing duplicate values, and then calling the correct
%% from_iterator_process_*/N functions, which should update the state variable.
from_iterator({Entry, Iterator}, StartIFT, LastValue, W) 
  when ?INDEX_FIELD_TERM(Entry) == StartIFT andalso
       ?VALUE(Entry) /= LastValue ->
    %% Add the next value to the segment.
    W1 = from_iterator_process_value(?VALUE_TSTAMP_PROPS(Entry), W),
    from_iterator(Iterator(), StartIFT, ?VALUE(Entry), W1);

from_iterator({Entry, Iterator}, StartIFT, _LastValue, W) 
  when ?INDEX_FIELD_TERM(Entry) /= StartIFT andalso StartIFT /= undefined ->
    %% Start a new term.
    W1 = from_iterator_process_end_term(StartIFT, W),
    W2 = from_iterator_process_start_term(?INDEX_FIELD_TERM(Entry), W1),
    W3 = from_iterator_process_value(?VALUE_TSTAMP_PROPS(Entry), W2),
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
    W3 = from_iterator_process_value(?VALUE_TSTAMP_PROPS(Entry), W2),
    from_iterator(Iterator(), ?INDEX_FIELD_TERM(Entry), ?VALUE(Entry), W3);

from_iterator(eof, StartIFT, _LastValue, W) ->
    %% End of segment.
    W1 = from_iterator_process_end_term(StartIFT, W),
    W2 = from_iterator_process_end_segment(W1),
    W2.


%% One method below for each different stage of writing a segment: start_segment, start_block, start_term, value, end_term, end_block, end_segment.

from_iterator_process_start_segment(W) -> 
    W.

from_iterator_process_start_block(W) -> 
    %% Set the block_start position, and zero out keys for this block..
    W#writer {
      block_start = W#writer.pos,
      keys = []
     }.

from_iterator_process_start_term(Key, W) -> 
    %% If the Index or Field value for this key is different from the
    %% last one, then close the last block and start a new block. This
    %% makes bookkeeping simpler all around.
    LastKey = W#writer.last_key,
    ShouldStartBlock = 
        LastKey == undefined orelse
        element(1, Key) /= element(1, LastKey) orelse
        element(2, Key) /= element(2, LastKey),
    case ShouldStartBlock of
        true ->
            W1 = from_iterator_process_end_block(W),
            W2 = from_iterator_process_start_block(W1);
        false ->
            W2 = W
    end,

    %% Write the key entry to the data file.
    from_iterator_write_key(W2, Key).

from_iterator_process_value(Value, W) ->
    %% Build up the set of values...
    W1 = W#writer { 
             values_staging=[Value|W#writer.values_staging],
             values_count = W#writer.values_count + 1
            },

    %% If we have accumulated enough values, then "write" the values,
    %% which may or may not compress them and then adds the result to
    %% the file buffer.
    case length(W1#writer.values_staging) > ?VALUES_STAGING_SIZE(W1) of
        true -> 
            W2 = from_iterator_write_values(W1),
            W2;
        false ->
            W1
    end.

from_iterator_process_end_term(Key, W) -> 
    W1 = from_iterator_write_values(W),

    %% Add the key to state...
    KeySize = W1#writer.values_start - W1#writer.key_start,
    ValuesSize = W1#writer.pos - W1#writer.values_start,
    W2 = W1#writer {
           keys = [{Key, KeySize, ValuesSize, W#writer.values_count}|W#writer.keys]
          },

    %% If this block is big enough, then close the old block and open
    %% a new block...
    case W2#writer.pos - W2#writer.block_start > ?BLOCK_SIZE(W2) of
        true ->
            W3 = from_iterator_process_end_block(W2),
            from_iterator_process_start_block(W3);
        false ->
            W2
    end.


from_iterator_process_end_block(W) when W#writer.keys /= [] -> 
    {FinalKey, _, _, _} = hd(W#writer.keys),

    %% Calculate the bloom filter...
    Bloom = mi_bloom:new([X || {X, _, _, _} <- W#writer.keys]),

    %% Calculate the longest prefix...
    F1 = fun({{_, _, Term}, _, _, _}, Acc) ->
                 mi_utils:longest_prefix(Acc, Term)
         end,
    LongestPrefix = lists:foldl(F1, undefined, W#writer.keys),

    %% Calculate offset table entries...
    {_, _, FinalTerm} = FinalKey,
    F2 = fun({Key, KeySize, ValuesSize, Count}) ->
                 {_, _, Term} = Key,
                 {mi_utils:edit_signature(FinalTerm, Term), mi_utils:hash_signature(Term), KeySize, ValuesSize, Count}
        end,
    KeyInfoList = [F2(X) || X <- lists:reverse(W#writer.keys)],
    
    %% Add entry to offsets table...
    Value = term_to_binary({W#writer.block_start, Bloom, LongestPrefix, KeyInfoList}, [{compressed, 1}]),
    Entry = {FinalKey, Value},
    true = ets:insert(W#writer.offsets_table, Entry),
    W;
from_iterator_process_end_block(W) when W#writer.keys == [] -> 
    W.

from_iterator_process_end_segment(W) -> 
    W1 = from_iterator_process_end_block(W),
    from_iterator_flush_buffer(W1, true).


%% Write a key to the data file.
from_iterator_write_key(W, Key) ->
    ShrunkenKey = shrink_key(W#writer.last_key, Key),
    Bytes = term_to_binary(ShrunkenKey),
    Size = erlang:size(Bytes),
    Output = [<<1:1/integer, Size:15/unsigned-integer>>, Bytes],
    W1 = W#writer {
           last_key = Key,
           key_start = W#writer.pos,
           values_start = W#writer.pos + Size + 2,
           pos = W#writer.pos + Size + 2,
           values_count = 0,
           buffer_size = W#writer.buffer_size + Size + 2,
           buffer = [Output|W#writer.buffer],
           compressed_values = false
          },
    from_iterator_flush_buffer(W1, false).

%% Write a block of values to the data file buffer. An early version
%% of this used zlib, but after some investigation there weren't any
%% noticable gains in size or speed compared to using
%% term_to_binary/compressed, plus using term_to_binary makes the code
%% simpler.
from_iterator_write_values(W) ->
    %% Serialize and compress the values.
    ValuesStaging = lists:reverse(W#writer.values_staging),
    case length(W#writer.values_staging) =< ?VALUES_COMPRESS_THRESHOLD(W) of
        true ->
            Bytes = term_to_binary(ValuesStaging);
        false ->
            Bytes = term_to_binary(ValuesStaging, [{compressed, ?VALUES_COMPRESS_LEVEL(W)}])
    end,
    
    %% Figure out what we want to write to disk.
    Size = erlang:iolist_size(Bytes),
    Output = [<<0:1/integer, Size:31/unsigned-integer>>, Bytes],

    %% Add output to file buffer, update file position, and reset
    %% values_staging.
    W1 = W#writer {
           pos = W#writer.pos + Size + 4,
           values_staging = [],
           compressed_values = true,
           buffer_size = W#writer.buffer_size + Size + 4,
           buffer = [Output|W#writer.buffer]
     },
    from_iterator_flush_buffer(W1, false).

from_iterator_flush_buffer(W, Force) ->
    case (W#writer.buffer_size > ?FILE_BUFFER_SIZE(W)) orelse Force of
        true ->
            ok = file:write(W#writer.data_file, lists:reverse(W#writer.buffer)),
            W#writer {
              buffer_size = 0,
              buffer = []
             };
        false ->
            W
    end.

%% compact_key/2 - Given the LastKey and CurrentKey, return a smaller
%% CurrentKey by removing the field and term if possible. Clauses
%% ordered by most common first.
shrink_key({I,F,_}, {I,F,Term}) ->
    Term;
shrink_key({I,_,_}, {I,Field,Term}) ->
    {Field, Term};
shrink_key(_, {Index,Field,Term}) ->
    {Index, Field, Term}.

