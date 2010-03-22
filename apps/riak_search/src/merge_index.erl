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

-module(merge_index).
-include_lib("kernel/include/file.hrl").
-behaviour(gen_server).

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
-define(MERGE_INTERVAL, 5000).
-define(SERVER, ?MODULE).
-record(state,  { rootfile, buckets, rawfiles, buffer, last_merge }).
-record(bucket, { offset, count, size }).

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
    random:seed(),

    %% Checkpoint every so often.
    timer:apply_interval(100, gen_server, cast, [self(), checkpoint]),
    timer:apply_interval(1000, gen_server, cast, [self(), merge]),

    %% Open the file.
    State = #state { 
        rootfile = Rootfile,
        buckets=gb_trees:empty(),
        rawfiles=[],
        buffer=[],
        last_merge=now()
    },
    {ok, State}.

handle_call({put, BucketName, Value, Props}, _From, State) ->
    NewBuffer = [{BucketName, now(), Value, Props}|State#state.buffer],
    {reply, ok, State#state { buffer=NewBuffer }};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(checkpoint, State) ->
    %% Write everything in the buffer to a new rawfile, and add it to
    %% the list of existing rawfiles.
    Rootfile = State#state.rootfile,
    Buffer = State#state.buffer,
    case length(Buffer) > 0 of
        true ->
            TempFileName = write_temp_file(Rootfile, length(State#state.rawfiles), Buffer),
            NewRawfiles = [TempFileName|State#state.rawfiles],
            case timer:now_diff(now(), State#state.last_merge) > ?MERGE_INTERVAL of
                true -> gen_server:cast(self(), merge);
                false -> ignore
            end,
            {noreply, State#state { 
                buffer=[],
                rawfiles = NewRawfiles
            }};
        false ->
            {noreply, State}
    end;

handle_cast(merge, State) when length(State#state.rawfiles) > 0 ->
    %% Merge all rawfiles into the main data file, and then re-index.
    Rootfile = State#state.rootfile,
    Rawfiles = State#state.rawfiles,
    ok = file_sorter:sort([Rootfile ++ ".data"|Rawfiles], Rootfile ++ ".tmp"), 
    Buckets = create_bucket_index(Rootfile ++ ".tmp"),
    file:write_file(Rootfile ++ ".buckets", term_to_binary(Buckets)),
    file:rename(Rootfile ++ ".data", Rootfile ++ ".tmp2"),
    file:rename(Rootfile ++ ".tmp", Rootfile ++ ".data"),
    file:rename(Rootfile ++ ".tmp2", Rootfile ++ ".tmp"),
    [file:delete(X) || X <- State#state.rawfiles],
    {noreply, State#state { buckets=Buckets, rawfiles=[], last_merge=now() }};

handle_cast(merge, State) ->
%%     ?PRINT(State#state.buckets),
    {noreply, State};

handle_cast({stream, BucketName, Pid, Ref}, State) ->
    %% Read bytes from the file...
    Rootfile = State#state.rootfile,    
    Bytes = case gb_trees:lookup(BucketName, State#state.buckets) of
        {value, Bucket} -> 
            {ok, FH} = file:open(Rootfile ++ ".data", [raw, read, binary]),
            {ok, B} = file:pread(FH, Bucket#bucket.offset, Bucket#bucket.size),
            B;
        none -> 
            <<>>
    end,
    stream_bytes(Bytes, undefined, Pid, Ref),
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

create_bucket_index(Filename) ->
    {ok, FH} = file:open(Filename, [raw, read, read_ahead, binary]),
    EmptyBucket = #bucket { offset=0, size=0 },
    create_bucket_index(0, FH, undefined, EmptyBucket, gb_trees:empty()).

create_bucket_index(Pos, FH, LastBucketName, Bucket, Acc) ->
    case read_value(FH) of
        {ValueSize, {LastBucketName, _, _, _}} ->
            %% Keep adding to the old bucket...
            NewSize = Bucket#bucket.size + ValueSize + 4,
            NewBucket = Bucket#bucket { size=NewSize },
            create_bucket_index(Pos + ValueSize + 4, FH, LastBucketName, NewBucket, Acc);

        {ValueSize, {BucketName, _, _, _}} ->
            %% Save the old bucket, create new bucket, continue...
            NewAcc = gb_trees:enter(LastBucketName, Bucket, Acc),
            NewBucket = #bucket { offset=Pos, size=ValueSize + 4 },
            create_bucket_index(Pos + ValueSize + 4, FH, BucketName, NewBucket, NewAcc);

        eof ->
            %% Done, so save the old bucket...
            gb_trees:enter(LastBucketName, Bucket, Acc)
    end.
            
read_value(FH) ->
    case file:read(FH, 4) of
        {ok, <<Size:32/integer>>} ->
            {ok, Bytes} = file:read(FH, Size),
            {Size, binary_to_term(Bytes)};
        eof ->
            eof
    end.


write_temp_file(Rootfile, N, Buffer) ->
    TempFileName = Rootfile ++ ".raw." ++ integer_to_list(N),
    ?PRINT({TempFileName, length(Buffer)}),
    {ok, FH} = file:open(TempFileName, [raw, append, delayed_write, binary]),
    F = fun(X) ->
        BinX = term_to_binary(X),
        SizeX = size(BinX),
        BinSizeX = <<SizeX:32/integer>>,
        file:write(FH, [BinSizeX, BinX])
    end,
    [F(X) || X <- Buffer],
    file:close(FH),
    TempFileName.

stream_bytes(<<>>, _, _, _) -> 
    ok;
stream_bytes(<<Size:32/integer, B:Size/binary, Rest/binary>>, LastValue, Pid, Ref) ->
    case binary_to_term(B) of
        {_, _, LastValue, _} -> 
            % Skip duplicates.
            stream_bytes(Rest, LastValue, Pid, Ref);
        {_, _, Value, Props} ->
            Pid!{result, {Value, Props}, Ref},
            stream_bytes(Rest, Value, Pid, Ref)
    end.
