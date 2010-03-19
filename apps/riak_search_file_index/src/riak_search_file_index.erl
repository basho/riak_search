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

-module(riak_search_file_index).
-include("riak_search_file_index.hrl").
-behaviour(gen_server).

-define(MERGE_COUNT, 2000).
-define(MERGE_INTERVAL, 5000).

%% API
-export([
    start_link/0,
    put/3,
    merge/1,
    stream/3,
    unlock/1,
    bucket_path/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, { buckets, locks, blocks }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

put(Bucket, Key, Props) ->
    gen_server:call(?SERVER, {put, Bucket, Key, Props}).

merge(Bucket) ->
    gen_server:cast(?SERVER, {merge, Bucket}).

stream(Bucket, Pid, Ref) ->
    gen_server:cast(?SERVER, {stream, Bucket, Pid, Ref}).

unlock(Bucket) ->
    gen_server:cast(?SERVER, {unlock, Bucket}).

bucket_path(Bucket) ->
    <<MD5:128/integer>> = erlang:md5(Bucket),
    [A, B, C|Rest] = integer_to_list(MD5),
    "./index/" ++ [A, B, C, $/] ++ Rest.

init([]) ->
    timer:apply_interval(1000, gen_server, cast, [?SERVER, interval]),
    State = #state { buckets=gb_trees:empty(), locks=[], blocks=[] },
    {ok, State}.

handle_call({put, Bucket, Key, Props}, _From, State) ->
    %% Get the bucket. If it exists, add this new value.
    %% If not, then create a new bucket.
    Buckets = State#state.buckets,
    case gb_trees:lookup(Bucket, Buckets) of
        {value, BucketRec} ->
            NewBucketRec = BucketRec#bucket { 
                count = BucketRec#bucket.count + 1,
                values = gb_trees:enter(Key, Props, BucketRec#bucket.values)
            },
            NewBuckets = gb_trees:update(Bucket, NewBucketRec, Buckets),
            {reply, ok, State#state { buckets=NewBuckets }};
        none ->
            NewBucketRec = #bucket {
                count = 1,
                last_merge=now(),
                values = gb_trees:from_orddict([{Key, Props}])
            },
            NewBuckets = gb_trees:insert(Bucket, NewBucketRec, Buckets),
            {reply, ok, State#state { buckets=NewBuckets }}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(Msg = {merge, Bucket}, State) ->
    %% Lock the bucket. If lock was successful, then
    %% call the merge function. Otherwise, queue the operation
    %% until the lock is released.
    case lock(Bucket, State) of
        {true, NewState} ->
            Buckets = State#state.buckets,
            case gb_trees:lookup(Bucket, Buckets) of
                {value, BucketRec} ->
                    riak_search_file_index_merge:merge(Bucket, BucketRec),
                    NewBuckets = gb_trees:delete(Bucket, Buckets),
                    {noreply, NewState#state { buckets=NewBuckets }};
                none ->
                    {noreply, NewState}
            end;
        false ->
            NewState = queue_block(Bucket, Msg, State),
            {noreply, NewState}
    end;

handle_cast(interval, State) ->
    List = gb_trees:to_list(State#state.buckets),
    Now = now(),
    F = fun({Bucket, Rec}) ->
        ShouldMerge = (Rec#bucket.count > ?MERGE_COUNT) orelse
        (timer:now_diff(Now, Rec#bucket.last_merge) > ?MERGE_INTERVAL),
        case ShouldMerge of
            true  -> merge(Bucket);
            false -> ignore
        end
    end,
    [F(X) || X <- List],
    {noreply, State};

handle_cast(Msg = {stream, Bucket, Pid, Ref}, State) ->
%%     ?PRINT(Msg),
    %% Lock the bucket. If successful, then stream the results.
    %% Otherwise, wait until we can get the lock.
    case lock(Bucket, State) of
        {true, NewState} ->
            riak_search_file_index_stream:stream(Bucket, Pid, Ref),
            {noreply, NewState};
        false ->
            NewState = queue_block(Bucket, Msg, State),
            {noreply, NewState}
    end;

handle_cast({unlock, Bucket}, State) ->
    %% Remove the lock...
    NewLocks = State#state.locks -- [Bucket],
    Blocks = State#state.blocks,

    %% See if anything is blocking on the lock.  
    %% If so, retry it, otherwise, just continue...
    case lists:keytake(Bucket, 1, Blocks) of
        {value, {Bucket, BlockedMsg}, NewBlocks} ->
            gen_server:cast(?SERVER, BlockedMsg),
            {noreply, State#state { locks=NewLocks, blocks=NewBlocks }};
        false ->
            {noreply, State#state { locks=NewLocks }}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

lock(Bucket, State) ->
    Locks = State#state.locks,
    case lists:member(Bucket, Locks) of
        false ->
            NewState = State#state { locks = [Bucket|Locks] },
            {true, NewState};
        true ->
            false
    end.

queue_block(Bucket, Msg, State) ->
    Blocks = State#state.blocks,
    State#state { blocks=[{Bucket, Msg}|Blocks] }.
