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
    start_link/2,
    put/4,
    put/5,
    info/2,
    info_range/4,
    stream/5,
    fold/3,
    is_empty/1,
    drop/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).


-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).
-define(SERVER, ?MODULE).
-define(DEFAULT_MERGE_INTERVAL, 10 * 1000).
-define(DEFAULT_SORT_BUFFER_SIZE, 1 * 1024 * 1024).
-define(DEFAULT_FILE_BUFFER_SIZE, 1 * 1024 * 1024).
-define(DEFAULT_FILE_BUFFER_DURATION, 2 * 1000).
-record(state,  { rootfile, config, buckets, rawfiles, buffer, last_merge, merge_pid, discard_next_merge=false }).
-record(bucket, { offset, count, size }).

start_link(Rootfile, Config) ->
    gen_server:start_link(?MODULE, [Rootfile, Config], [{timeout, infinity}]).

put(ServerPid, BucketName, Timestamp, Value, Props) ->
    gen_server:call(ServerPid, {put, BucketName, Timestamp, Value, Props}, infinity).

put(ServerPid, BucketName, Value, Props) ->
    gen_server:call(ServerPid, {put, BucketName, Value, Props}, infinity).

info(ServerPid, BucketName) ->
    gen_server:call(ServerPid, {info, BucketName}).

info_range(ServerPid, Start, End, Inclusive) ->
    gen_server:call(ServerPid, {info_range, Start, End, Inclusive}).

stream(ServerPid, BucketName, Pid, Ref, FilterFun) ->
    gen_server:cast(ServerPid, {stream, BucketName, Pid, Ref, FilterFun}).

fold(ServerPid, Function, Acc) ->
    gen_server:call(ServerPid, {fold, Function, Acc}).

is_empty(ServerPid) ->
    gen_server:call(ServerPid, is_empty).

drop(ServerPid) ->
    gen_server:call(ServerPid, drop).

init([Rootfile, Config]) ->
    random:seed(),

    %% Ensure that the data file exists...
    filelib:ensure_dir(Rootfile ++ ".data"),
    case filelib:is_file(Rootfile ++ ".data") of
        true -> 
            ok;
        false -> 
            file:write_file(Rootfile ++ ".data", <<>>)
    end,

    %% Ensure that the buckets file exists...
    Buckets = case file:read_file(Rootfile ++ ".buckets") of
        {ok, B} -> 
            binary_to_term(B);
        {error, _} -> 
            EmptyBin = term_to_binary(gb_trees:empty()),
            file:write_file(Rootfile ++ ".buckets", EmptyBin),
            gb_trees:empty()
    end,

    %% Checkpoint every so often.
    timer:apply_interval(100, gen_server, cast, [self(), checkpoint]),

    %% Open the file.
    State = #state { 
        rootfile = Rootfile,
        config=Config,
        buckets=Buckets,
        rawfiles=[],
        buffer=[],
        last_merge=now(),
        merge_pid=undefined
    },
    {ok, State}.

handle_call({put, BucketName, Value, Props}, From, State) ->
    Now = now(),
    handle_call({put, BucketName, Now, Value, Props}, From, State);

handle_call({put, BucketName, Timestamp, Value, Props}, _From, State) ->
    %% Hackish - Values are sorted in ascending order on disk, but in
    %% order to correctly allow property/facet updates we need them to
    %% be sorted according to descending time. Achieve this by
    %% inverting the timestamp. Make it better later.
    InverseTimestamp = invert_timestamp(Timestamp),

    %% Add the new value to the buffer.
    NewBuffer = [{BucketName, Value, InverseTimestamp, Props}|State#state.buffer],
    {reply, ok, State#state { buffer=NewBuffer }};

handle_call({info, BucketName}, _From, State) ->
    Buckets = State#state.buckets,
    case gb_trees:lookup(BucketName, Buckets) of
        {value, Bucket} ->
            {reply, {ok, {BucketName, node(), Bucket#bucket.count}}, State};
        none ->
            {reply, {ok, {BucketName, node(), 0}}, State}
    end;

handle_call({info_range, Start, End, Inclusive}, _From, State) ->
    Buckets = State#state.buckets,
    Results = gb_trees_select(Start, End, Inclusive, Buckets),
    Node = node(),
    Results1 = [{ITF, Node, Bucket#bucket.count} || {ITF, Bucket} <- Results],
    {reply, {ok, Results1}, State};

handle_call({fold, Fun, Acc}, _From, State) ->
    Rootfile = State#state.rootfile,    
    {ok, FH} = file:open(Rootfile ++ ".data", [raw, read, read_ahead, binary]),
    NewAcc = do_fold(FH, Fun, Acc),
    file:close(FH),
    {reply, {ok, NewAcc}, State};

handle_call(is_empty, _From, State) ->
    % Hackish - the buckets gb_tree will always have one 'undefined'
    % placeholder bucket.
    IsEmpty = gb_trees:size(State#state.buckets) =< 1,
    {reply, IsEmpty, State};

handle_call(drop, _From, State) ->
    % Zero out the data file.
    Rootfile = State#state.rootfile,    
    file:write(Rootfile ++ ".data", ""),

    % Empty buckets gbtree.
    NewState = State#state { 
        buckets=gb_trees:empty(),
        discard_next_merge = true
    },
    {reply, ok, NewState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(checkpoint, State) ->
    %% Write everything in the buffer to a new rawfile, and add it to
    %% the list of existing rawfiles.
    Rootfile = State#state.rootfile,
    Buffer = State#state.buffer,
    NewState = case length(Buffer) > 0 of
        true ->
            TempFileName = write_temp_file(Rootfile, Buffer, State),
            NewRawfiles = [TempFileName|State#state.rawfiles],
            State#state { buffer=[], rawfiles = NewRawfiles };
        false ->
            State
    end,
    %% Check if we should do some merging...
    HasRawFiles = length(NewState#state.rawfiles) > 0,
    MergeInterval = get_config(merge_interval, State, ?DEFAULT_MERGE_INTERVAL),
    IsTimeForMerge = (timer:now_diff(now(), NewState#state.last_merge)/1000) > MergeInterval,
    IsMerging = is_pid(NewState#state.merge_pid),
    case HasRawFiles andalso IsTimeForMerge andalso not IsMerging of
        true -> 
            Self = self(),
            Pid = spawn_link(fun() -> merge(Self, NewState) end),
            {noreply, NewState#state { rawfiles=[], merge_pid=Pid }};
        false -> 
            {noreply, NewState}
    end;

handle_cast({merge_complete, Buckets}, State) when State#state.discard_next_merge == false ->
    RF = State#state.rootfile,
    swap_files(RF ++ ".merged", RF ++ ".data"),
    swap_files(RF ++ ".buckets_merged", RF ++ ".buckets"),
    NewState = State#state { buckets=Buckets, last_merge=now(), merge_pid=undefined },
    {noreply, NewState};

handle_cast({merge_complete, _Buckets}, State) when State#state.discard_next_merge == true ->
    NewState = State#state { last_merge=now(), merge_pid=undefined, discard_next_merge=false },
    {norelpy, NewState};

handle_cast({stream, BucketName, Pid, Ref, FilterFun}, State) ->
    %% Read bytes from the file...
    Rootfile = State#state.rootfile,    
    Bytes = case gb_trees:lookup(BucketName, State#state.buckets) of
        {value, Bucket} -> 
            {ok, FH} = file:open(Rootfile ++ ".data", [raw, read, read_ahead, binary]),
            {ok, B} = file:pread(FH, Bucket#bucket.offset, Bucket#bucket.size),
            file:close(FH),
            B;
        none -> 
            <<>>
    end,
    stream_bytes(Bytes, undefined, Pid, Ref, FilterFun),
    Pid!{result, '$end_of_table', Ref},
    {noreply, State};

handle_cast(Msg, State) ->
    ?PRINT({unhandled_cast, Msg}),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_config(Key, State, Default) ->
    Config = State#state.config,
    proplists:get_value(Key, Config, Default).

merge(Pid, State) ->
    RF = State#state.rootfile,

    %% Sort the partial files...
    Rawfiles = State#state.rawfiles,
    SortBufferSize = get_config(sort_buffer_size, State, ?DEFAULT_SORT_BUFFER_SIZE),
    ok = file_sorter:sort(Rawfiles, RF ++ ".rawmerged", [{size, SortBufferSize}]),
    
    %% Merge all files into a main file, and create the buckets tree...
    FileBufferSize = get_config(file_buffer_size, State, ?DEFAULT_FILE_BUFFER_SIZE),
    FileBufferDuration = get_config(file_buffer_duration, State, ?DEFAULT_FILE_BUFFER_DURATION),
    FileOptions = [raw, write, {delayed_write, FileBufferSize, FileBufferDuration}, binary],
    {ok, FH} = file:open(RF ++ ".merged", FileOptions),
    InitialBucket = #bucket { offset=0, count=0, size=0 },
    F = create_index_fun(0, FH, undefined, undefined, InitialBucket, gb_trees:empty()),
    Buckets = file_sorter:merge([RF ++ ".data", RF ++ ".rawmerged"], F, {size, SortBufferSize}),
    file:close(FH),

    %% Persist the buckets...
    file:write_file(RF ++ ".buckets_merged", term_to_binary(Buckets)),
    
    %% Cleanup...
    [file:delete(X) || X <- State#state.rawfiles],
    file:delete(RF ++ ".rawmerged"),
    
    gen_server:cast(Pid, {merge_complete, Buckets}).


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
                    NewCount = Bucket#bucket.count + 1,
                    NewBucket = Bucket#bucket { count=NewCount, size=NewSize },
                    create_index(Pos + size(H) + 4, FH, Value, LastBucketName, NewBucket, Buckets, T);
                {BucketName, Value, _, _} ->
                    %% Save the old bucket, create new bucket, continue...
                    write_value(FH, H),
                    NewBuckets = gb_trees:enter(LastBucketName, Bucket, Buckets),
                    NewBucket = #bucket { offset=Pos, count=1, size=size(H) + 4 },
                    create_index(Pos + size(H) + 4, FH, Value, BucketName, NewBucket, NewBuckets, T)
            end;
        [] ->
            %% End of list, return a callback function...
            create_index_fun(Pos, FH, LastValue, LastBucketName, Bucket, Buckets);
        close ->
            gb_trees:enter(LastBucketName, Bucket, Buckets)
    end.

swap_files(Filename1, Filename2) ->
    ok = file:rename(Filename1, Filename1 ++ ".tmp"),
    ok = file:rename(Filename2, Filename1),
    ok = file:rename(Filename1 ++ ".tmp", Filename2).
            
write_temp_file(Rootfile, Buffer, State) ->
    TempFileName = Rootfile ++ ".raw." ++ integer_to_list(random:uniform(999999)),
    FileBufferSize = get_config(file_buffer_size, State, ?DEFAULT_FILE_BUFFER_SIZE),
    FileBufferDuration = get_config(file_buffer_duration, State, ?DEFAULT_FILE_BUFFER_DURATION),
    FileOptions = [raw, append, {delayed_write, FileBufferSize, FileBufferDuration}, binary],
    {ok, FH} = file:open(TempFileName, FileOptions),
    [write_value(FH, term_to_binary(X)) || X <- Buffer],
    file:close(FH),
    TempFileName.

write_value(FH, B) ->
    Size = size(B),
    ok = file:write(FH, <<Size:32/integer, B/binary>>).

read_value(FH) ->
    case file:read(FH, 4) of
        {ok, <<Size:32/integer>>} ->
            {ok, Bytes} = file:read(FH, Size),
            {ok, binary_to_term(Bytes)};
        eof ->
            eof
    end.
                

stream_bytes(<<>>, _, _, _, _) -> 
    ok;
stream_bytes(<<Size:32/integer, B:Size/binary, Rest/binary>>, LastValue, Pid, Ref, FilterFun) ->
    case binary_to_term(B) of
        {_, LastValue, _, _} -> 
            % Skip duplicates.
            stream_bytes(Rest, LastValue, Pid, Ref, FilterFun);
        {_, Value, _, Props} ->
            case FilterFun(Value, Props) == true of
                true -> Pid!{result, {Value, Props}, Ref};
                false -> skip
            end,
            stream_bytes(Rest, Value, Pid, Ref, FilterFun)
    end.


do_fold(FH, Fun, Acc) ->
    case read_value(FH) of
        {ok, T} ->
            ?PRINT(T),
            %% Create a new Riak object.  Lots of binary/list
            %% manipulation here. Can we streamline this?
            {BucketName, Value, InverseTimestamp, Props} = T,
            [Index, Field, Term] = string:tokens(binary_to_list(BucketName), "."),
            IndexB = list_to_binary(Index),
            FieldTermB = list_to_binary([Field, ".", Term]),
            Timestamp = invert_timestamp(InverseTimestamp),
            Obj = riak_object:new(IndexB, FieldTermB, {put, Timestamp, Value, Props}),
            ObjB = term_to_binary(Obj),
            NewAcc = Fun({IndexB, FieldTermB}, ObjB, Acc),
            do_fold(FH, Fun, NewAcc);
        eof ->
            ?PRINT({fold_finished, eof}),
            Acc
    end.

invert_timestamp(Timestamp) ->
    {TS1, TS2, TS3} = Timestamp,
    {-1 * TS1, -1 * TS2, -1 * TS3}.


%% Walk through a gb_tree, selecting values between a range. Special
%% case for a wildcard, where we convert a prefix to the values that
%% would come right before and right after it by adding -1 and 1,
%% respectively.
gb_trees_select(Start, Wildcard, _, Tree) 
when Wildcard == wildcard_all orelse Wildcard == wildcard_one ->
    {_, T} = Tree,
    NewStart = <<Start/binary, 0:8/integer>>,
    NewEnd = <<Start/binary, 255:8/integer>>,
    RequiredSize = case Wildcard of
        wildcard_one -> size(Start) + 1;
        wildcard_all -> undefined
    end,
    gb_trees_select(NewStart, NewEnd, false, RequiredSize, T, []);
gb_trees_select(Start, End, Inclusive, Tree) ->
    {_, T} = Tree,
    gb_trees_select(Start, End, Inclusive, undefined, T, []).

gb_trees_select(_, _, _, _, nil, Acc) -> 
    Acc;
gb_trees_select(Start, End, Inclusive, RequiredSize, {Key, Value, Left, Right}, Acc) ->
    LBound = (Start == all) orelse (Start < Key) orelse (Start == Key andalso Inclusive),
    RBound = (End == all) orelse (Key < End) orelse (End == Key andalso Inclusive),
    %% Key is undefined because the list of Buckets has a dummy
    %% entry. Makes other parts of the code cleaner.
    CorrectSize = ((Key /= undefined andalso size(Key) == RequiredSize) orelse RequiredSize == undefined),

    %% If we are within bounds, then add this value...
    NewAcc = if 
        LBound andalso RBound andalso CorrectSize ->
            [{Key, Value}|Acc];
        true ->
            Acc
    end,

    %% If we are in lbound, then go left...
    NewAcc1 = case LBound of
        true ->  gb_trees_select(Start, End, Inclusive, RequiredSize, Left, NewAcc);
        false -> NewAcc
    end,

    %% If we are in rbound, then go right...
    NewAcc2 = case RBound of
        true ->  gb_trees_select(Start, End, Inclusive, RequiredSize, Right, NewAcc1);
        false -> NewAcc1
    end,
    NewAcc2.
