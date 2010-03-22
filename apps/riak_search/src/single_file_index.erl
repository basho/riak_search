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

-module(single_file_index).
-include_lib("kernel/include/file.hrl").
-behaviour(gen_server).

-define(MERGE_COUNT, 1000000).
-define(MERGE_INTERVAL, 100000000).

%% API
-export([
    start/1, put/3, stream/3,
    start_link/1,
    put/4,
    stream/4
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).
-define(SERVER, ?MODULE).
-record(state,  { buffers, blocks, freeblocks, buckets, rootfile, datafilesize, datafilehandle }).
-record(block,  { offset, size, is_used }).
-record(bucket, { offset, size, count }).
-record(buffer, { values, last_merge }).

%%% DEBUGGING - Single Process

start(Rootfile) ->
    gen_server:start({local, ?SERVER}, ?MODULE, [Rootfile], [{timeout, infinity}]).

put(BucketName, Value, Props) ->
    put(?SERVER, BucketName, Value, Props).

stream(BucketName, Pid, Ref) ->
    stream(?SERVER, BucketName, Pid, Ref).

%%% END DEBUGGING

start_link(Rootfile) ->
    gen_server:start_link(?MODULE, [Rootfile], [{timeout, infinity}]).

put(ServerPid, BucketName, Value, Props) ->
    gen_server:call(ServerPid, {put, BucketName, Value, Props}, infinity).

stream(ServerPid, BucketName, Pid, Ref) ->
    gen_server:cast(ServerPid, {stream, BucketName, Pid, Ref}).

init([Rootfile]) ->
    %% Open the file.
    {ok, FH} = file:open(Rootfile ++ ".data", [raw, read, read_ahead, write, delayed_write, binary]),

    %% Read in the buckets and blocks.
    Buckets = case file:read_file(Rootfile ++ ".buckets") of
        {ok, B1} -> binary_to_term(B1);
        {error, _} -> gb_trees:empty()
    end,
    Blocks = case file:read_file(Rootfile ++ ".blocks") of
        {ok, B2} -> binary_to_term(B2);
        {error, _} -> gb_trees:empty()
    end,
    FreeBlocks = case file:read_file(Rootfile ++ ".freeblocks") of
        {ok, B3} -> binary_to_term(B3);
        {error, _} -> []
    end,
    Buffers = case file:read_file(Rootfile ++ ".buffers") of
        {ok, B4} -> binary_to_term(B4);
        {error, _} -> gb_trees:empty()
    end,

    %% Merge every so often
    timer:apply_interval(1000, gen_server, cast, [self(), interval]),
    
    State = #state { 
        buckets=Buckets,
        blocks=Blocks,
        freeblocks=FreeBlocks,
        buffers=Buffers,
        rootfile = Rootfile,
        datafilesize=0,
        datafilehandle=FH
    },
    {ok, State}.

handle_call({put, BucketName, Value, Props}, _From, State) ->
    %% Get the bucket. If it exists, add this new value.
    %% If not, then create a new bucket.
    Buffers = State#state.buffers,
    case gb_trees:lookup(BucketName, Buffers) of
        {value, Buffer} ->
            NewBuffer = Buffer#buffer { 
                values = gb_trees:enter(Value, Props, Buffer#buffer.values)
            },
            case gb_trees:size(NewBuffer#buffer.values) > ?MERGE_COUNT of
                true ->
                    gen_server:cast(self(), {merge, BucketName});
                false ->
                    ignore
            end,
            NewBuffers = gb_trees:update(BucketName, NewBuffer, Buffers),
            {reply, ok, State#state { buffers=NewBuffers }};
        none ->
            NewBuffer = #buffer {
                values = gb_trees:from_orddict([{Value, Props}]),
                last_merge=now()
            },
            NewBuffers = gb_trees:insert(BucketName, NewBuffer, Buffers),
            {reply, ok, State#state { buffers=NewBuffers }}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% handle_cast(merge_interval, State) ->
%%     List = gb_trees:to_list(State#state.buffers),
%%     Now = now(),
%%     F = fun({BucketName, Buffer}) ->
%%         IsTooBig = (gb_trees:size(Buffer#buffer.values) > ?MERGE_COUNT),
%%         IsTooOld = (timer:now_diff(Now, Buffer#buffer.last_merge) > ?MERGE_INTERVAL),
%%         case IsTooBig orelse IsTooOld of
%%             true -> 
%%                 gen_server:cast(self(), {merge, BucketName});
%%             false -> 
%%                 ignore
%%         end
%%     end,
%%     [F(X) || X <- List],
%%     gen_server:cast(self(), merge_interval_done),
%%     {noreply, State};

handle_cast(interval, State) ->
    Rootfile = State#state.rootfile,
    file:write_file(Rootfile ++ ".buffers", term_to_binary(State#state.buffers)),
    {noreply, State};

handle_cast({merge, BucketName}, State) ->
    %% Get the bucket...
    Bucket = case gb_trees:lookup(BucketName, State#state.buckets) of
        {value, B} -> B;
        none -> #bucket { offset=undefined, size=0, count=0 }
    end,

    %% Get the buffer...
    {value, Buffer} = gb_trees:lookup(BucketName, State#state.buffers),
    SortedBufferValues = gb_trees:to_list(Buffer#buffer.values),
    NewBuffers = gb_trees:delete(BucketName, State#state.buffers),
    
    %% Get the FromBlock...
    Offset = Bucket#bucket.offset,
    FromBlock = case gb_trees:lookup(Offset, State#state.blocks) of
        {value, FromBlockT} -> FromBlockT;
        none -> undefined
    end,
%%     ?PRINT(FromBlock),
    
    %% Read the Data...
    FH = State#state.datafilehandle,
    BinValues = case Bucket#bucket.offset /= undefined of
        true -> 
            {ok, BinValuesT} = file:pread(FH, Bucket#bucket.offset, Bucket#bucket.size),
            zlib:unzip(BinValuesT);
        false ->
            <<>>
    end,

    %% Merge the values in memory.
    F = fun(X, {Size, Count, List}) -> 
        BinValue = term_to_binary(X),
        ValueSize = size(BinValue),
        BinValueSize = <<ValueSize:32/integer>>,
        {Size + ValueSize + 4, Count + 1, [<<BinValueSize/binary, BinValue/binary>>|List]}
    end,
    {MergedSize, MergedCount, MergedValues} = merge(F, {0, 0, []}, BinValues, SortedBufferValues),

    %% Get the ToBlock...
    FileSize = State#state.datafilesize,
    FreeBlocks = case FromBlock /= undefined of 
        true -> [FromBlock#block { is_used=false }|State#state.freeblocks];
        false -> State#state.freeblocks
    end,
    {ToBlock, NewFreeBlocks} = find_free_block(FreeBlocks, MergedSize, FileSize),
%%     ?PRINT(ToBlock),

    %% Possibly push out the file...
    BlockEnd = ToBlock#block.offset + ToBlock#block.size,
    NewFileSize = case BlockEnd > FileSize of
        true ->
            expand_file(FH, FileSize, BlockEnd),
            BlockEnd;
        false -> 
            FileSize
    end,

    %% Write the new block...
    Compressed = zlib:zip(list_to_binary(lists:reverse(MergedValues))),
    ok = file:pwrite(FH, ToBlock#block.offset, Compressed),
    ok = file:sync(FH),

    %% Update the bucket location...
    NewBucket = Bucket#bucket { 
        offset=ToBlock#block.offset,
        size=size(Compressed),
        count=MergedCount
    },
    NewBuckets = gb_trees:enter(BucketName, NewBucket, State#state.buckets),

    %% Update the block tree...
    Blocks = State#state.blocks,
    NewBlocks1 = gb_trees:enter(ToBlock#block.offset, ToBlock, Blocks),
    NewBlocks2 = case FromBlock /= undefined of
        true  -> gb_trees:delete(FromBlock#block.offset, NewBlocks1);
        false -> NewBlocks1
    end,

    %% Write out the buckets and blocks.
    Rootfile = State#state.rootfile,
    ok = file:write_file(Rootfile ++ ".buckets", term_to_binary(NewBuckets)),
    ok = file:write_file(Rootfile ++ ".blocks", term_to_binary(NewBlocks2)),
    ok = file:write_file(Rootfile ++ ".freeblocks", term_to_binary(NewFreeBlocks)),

    {noreply, State#state {
        buffers=NewBuffers, 
        buckets = NewBuckets,
        blocks = NewBlocks2,
        freeblocks = NewFreeBlocks,
        datafilesize = NewFileSize
    }};
    
handle_cast({stream, BucketName, Pid, Ref}, State) ->
    %% Get the bucket...
    Bucket = case gb_trees:lookup(BucketName, State#state.buckets) of
        {value, B1} -> B1;
        none -> #bucket { offset=undefined, size=0, count=0 }
    end,

    %% Get the buffer...
    Buffer = case gb_trees:lookup(BucketName, State#state.buffers) of
        {value, B2} -> B2;
        none -> #buffer { values=gb_trees:empty() }
    end,
    SortedBufferValues = gb_trees:to_list(Buffer#buffer.values),
    
    %% Read the Data...
    FH = State#state.datafilehandle,
    BinValues = case Bucket#bucket.offset /= undefined of
        true -> 
            {ok, BinValuesT} = file:pread(FH, Bucket#bucket.offset, Bucket#bucket.size),
            BinValuesT;
        false ->
            <<>>
    end,

    %% Merge and send the values in memory.
    F = fun(X, _) -> 
        Pid!{result, X, Ref}
    end,
    merge(F, undefined, BinValues, SortedBufferValues),
    Pid!{result, '$end_of_table', Ref},
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

merge(_F, Acc, <<>>, []) ->
    Acc;
merge(F, Acc, <<>>, [H|T]) ->
    NewAcc = F(H, Acc),
    merge(F, NewAcc, <<>>, T);
merge(F, Acc, <<Size:32/integer, B:Size/binary, Rest/binary>>, []) ->
    NewAcc = F(binary_to_term(B), Acc),
    merge(F, NewAcc, Rest, []);
merge(F, Acc, <<Size:32/integer, B:Size/binary, Rest1/binary>>=All1, [{Key2, Props2}|Rest2]=All2) ->
    {Key1, Props1} = binary_to_term(B),
    if 
        Key1 < Key2 -> 
            %% Merge Key1...
            NewAcc = F({Key1, Props1}, Acc),
            merge(F, NewAcc, Rest1, All2);

        Key1 > Key2 ->
            %% Merge Key2...
            NewAcc = F({Key2, Props2}, Acc),
            merge(F, NewAcc, All1, Rest2);

        Key1 == Key2 -> 
            %% Equal, so Merge Key2 and advance both...
            NewAcc = F({Key2, Props2}, Acc),
            merge(F, NewAcc, Rest1, Rest2)
    end.

find_free_block(FreeBlocks, Size, FileSize) ->
    FreeBlocks1 = lists:sort(FreeBlocks),
    FreeBlocks2 = combine_free_blocks(FreeBlocks1),

    %% Find the first block that will fit.  If we can't find one, then
    %% create a new block, at the end of the list.
    F = fun(Block) -> 
        Block#block.size < Size 
    end,
    ExtraSpace = 2 * 1024,
    case lists:splitwith(F, FreeBlocks2) of
        {[], []} ->
            NewBlock = #block { offset=FileSize, size=Size + ExtraSpace, is_used=true },
            {NewBlock, []};

        {_Pre, []} ->
            %% Create a new block.
            NewBlock = #block { offset=FileSize, size=Size + ExtraSpace, is_used = true },
            {NewBlock, FreeBlocks2};

        {Pre, Post} ->
            %% Get the first block big enough to hold our data.
            %% Split off a block just big enough.
            NewBlock = hd(Post),
            NewBlockUsed = NewBlock#block { 
                is_used = true, 
                size=Size 
            },
            NewBlockUnused = NewBlock#block { 
                is_used=false, 
                offset = NewBlock#block.offset + Size,
                size = NewBlock#block.size - Size
            },
            {NewBlockUsed, [NewBlockUnused|Pre] ++ tl(Post)}
    end.
    
%% Smush empty blocks together.
combine_free_blocks([H1 = #block { is_used=false }, H2 = #block { is_used = false }|Rest]) 
  when H1#block.offset + H1#block.size == H2#block.offset ->
    NewBlock = #block { 
        offset = H1#block.offset,
        size = H1#block.size + H2#block.size,
        is_used = false
    },
    combine_free_blocks([NewBlock|Rest]);
combine_free_blocks([H|Rest]) ->
    [H|combine_free_blocks(Rest)];
combine_free_blocks([]) -> 
    [].

%% Expand our index file is at least this many bytes.
expand_file(FH, FileSize, NewSize) ->
    {ok, FileSize} = file:position(FH, FileSize),
    Bits = (NewSize - FileSize) * 8,
    file:write(FH, <<0:Bits/integer>>).
