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

-module(riak_search_file_index_stream).
-include("riak_search_file_index.hrl").
-behaviour(gen_server).

-define(MERGE_COUNT, 2000).
-define(MERGE_INTERVAL, 5000).

%% API
-export([
    start_link/0,
    stream/3
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, { }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stream(Bucket, Pid, Ref) ->
    gen_server:cast(?SERVER, {stream, Bucket, Pid, Ref}).

init([]) ->
    State = #state {},
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({stream, Bucket, Pid, Ref}, State) ->
    %% Open the bucket file.
    BucketPath = riak_search_file_index:bucket_path(Bucket),
    case file:open(BucketPath, [raw, read, read_ahead, binary, compressed]) of
        {ok, In} -> 
            %% Discard the first 8 bytes, for size.
            {ok, _} = file:read(In, 8),
            stream_results(In, Pid, Ref),
            file:close(In);
        {error, enoent} -> 
            ignore
    end,
    Pid!{result, '$end_of_table', Ref},
    riak_search_file_index:unlock(Bucket),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

stream_results(In, Pid, Ref) ->
    case read_value(In) of
        Value when Value /= eof -> 
            Pid!{result, Value, Ref},
            stream_results(In, Pid, Ref);
        _ -> ok
    end.

read_value(In) ->
    %% How many bytes should we read?
    case file:read(In, 4) of
        eof -> 
            eof;

        {ok, <<ValueSize:32/integer>>} ->
            %% Read the bytes and decode.
            case file:read(In, ValueSize) of
                eof -> 
                    eof;
                {ok, Bytes} ->
                    binary_to_term(Bytes)
            end
    end.
