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
-define(MERGE_INTERVAL, 10 * 1000 * 1000).
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

    %% Ensure that the file exists...
    filelib:ensure_dir(Rootfile ++ ".data"),
    case filelib:is_file(Rootfile ++ ".data") of
        true -> ok;
        false -> file:write_file(Rootfile ++ ".data", <<>>)
    end,

    Buckets = case file:read_file(Rootfile ++ ".buckets") of
        {ok, B} -> binary_to_term(B);
        {error, _} -> gb_trees:empty()
    end,

    %% Checkpoint every so often.
    timer:apply_interval(100, gen_server, cast, [self(), checkpoint]),

    %% Open the file.
    State = #state { 
        rootfile = Rootfile,
        buckets=Buckets,
        rawfiles=[],
        buffer=[],
        last_merge=now()
    },
    {ok, State}.

handle_call({put, BucketName, Value, Props}, _From, State) ->
    NewBuffer = [{BucketName, Value, now(), Props}|State#state.buffer],
    {reply, ok, State#state { buffer=NewBuffer }};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(checkpoint, State) ->
    %% Check if we should do some merging...
    case timer:now_diff(now(), State#state.last_merge) > ?MERGE_INTERVAL of
        true -> gen_server:cast(self(), merge);
        false -> ignore
    end,
    
    %% Write everything in the buffer to a new rawfile, and add it to
    %% the list of existing rawfiles.
    Rootfile = State#state.rootfile,
    Buffer = State#state.buffer,
    case length(Buffer) > 0 of
        true ->
            TempFileName = write_temp_file(Rootfile, length(State#state.rawfiles), Buffer),
            NewRawfiles = [TempFileName|State#state.rawfiles],
            {noreply, State#state { 
                buffer=[],
                rawfiles = NewRawfiles
            }};
        false ->
            {noreply, State}
    end;

handle_cast(merge, State) when length(State#state.rawfiles) > 0 ->
    RF = State#state.rootfile,

    %% Sort the partial files...
    Rawfiles = State#state.rawfiles,
    ok = file_sorter:sort(Rawfiles, RF ++ ".rawtmp"),
    
    %% Merge all files into a main file, and create the buckets tree...
    {ok, FH} = file:open(RF ++ ".tmp", [raw, write, delayed_write, binary]),
    InitialBucket = #bucket { offset=0, size=0 },
    F = create_index_fun(0, FH, undefined, undefined, InitialBucket, gb_trees:empty()),
    Buckets = file_sorter:merge([RF ++ ".data", RF ++ ".rawtmp"], F),
    file:close(FH),

    %% Persist the buckets...
    file:write_file(RF ++ ".buckets", term_to_binary(Buckets)),
    
    %% Cleanup...
    file:rename(RF ++ ".data", RF ++ ".tmp2"),
    file:rename(RF ++ ".tmp", RF ++ ".data"),
    file:rename(RF ++ ".tmp2", RF ++ ".tmp"),
    [file:delete(X) || X <- State#state.rawfiles],
    file:delete(RF ++ ".rawtmp"),
    {noreply, State#state { buckets=Buckets, rawfiles=[], last_merge=now() }};

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

create_index_fun(Pos, FH, LastValue, BucketName, Bucket, Buckets) ->
    fun(L) ->
        create_index(Pos, FH, LastValue, BucketName, Bucket, Buckets, L)
    end.

create_index(Pos, FH, LastValue, LastBucketName, Bucket, Buckets, L) ->
    case L of
        [H|T] ->
            case binary_to_term(H) of
                {LastBucketName, LastValue, _, _} ->
                    %% Remove duplicates...
                    create_index(Pos, FH, LastValue, LastBucketName, Bucket, Buckets, T);
                {LastBucketName, Value, _, _} ->
                    %% Keep adding to the old bucket...
                    write_value(FH, H),
                    NewSize = Bucket#bucket.size + size(H) + 4,
                    NewBucket = Bucket#bucket { size=NewSize },
                    create_index(Pos + size(H) + 4, FH, Value, LastBucketName, NewBucket, Buckets, T);
                {BucketName, Value, _, _} ->
                    %% Save the old bucket, create new bucket, continue...
                    write_value(FH, H),
                    NewBuckets = gb_trees:enter(LastBucketName, Bucket, Buckets),
                    NewBucket = #bucket { offset=Pos, size=size(H) + 4 },
                    create_index(Pos + size(H) + 4, FH, Value, BucketName, NewBucket, NewBuckets, T)
            end;
        [] ->
            %% End of list, return a callback function...
            create_index_fun(Pos, FH, LastValue, LastBucketName, Bucket, Buckets);
        close ->
            gb_trees:enter(LastBucketName, Bucket, Buckets)
    end.
            
write_temp_file(Rootfile, N, Buffer) ->
    TempFileName = Rootfile ++ ".raw." ++ integer_to_list(N),
    {ok, FH} = file:open(TempFileName, [raw, append, delayed_write, binary]),
    [write_value(FH, term_to_binary(X)) || X <- Buffer],
    file:close(FH),
    TempFileName.

write_value(FH, B) ->
    Size = size(B),
    ok = file:write(FH, <<Size:32/integer, B/binary>>).

stream_bytes(<<>>, _, _, _) -> 
    ok;
stream_bytes(<<Size:32/integer, B:Size/binary, Rest/binary>>, LastValue, Pid, Ref) ->
    case binary_to_term(B) of
        {_, LastValue, _, _} -> 
            % Skip duplicates.
            stream_bytes(Rest, LastValue, Pid, Ref);
        {_, Value, _, Props} ->
            Pid!{result, {Value, Props}, Ref},
            stream_bytes(Rest, Value, Pid, Ref)
    end.
