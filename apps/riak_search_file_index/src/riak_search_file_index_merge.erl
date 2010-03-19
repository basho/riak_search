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

-module(riak_search_file_index_merge).
-include("riak_search_file_index.hrl").
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    merge/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

merge(Bucket, BucketRec) ->
    gen_server:cast(?SERVER, {merge, Bucket, BucketRec}).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({merge, Bucket, BucketRec}, State) ->
    Count = BucketRec#bucket.count,
    SortedValues = gb_trees:to_list(BucketRec#bucket.values),
    BucketPath = riak_search_file_index:bucket_path(Bucket),
    file_merge_sort(BucketPath, Count, SortedValues),
    riak_search_file_index:unlock(Bucket),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

file_merge_sort(BucketPath, Count, Values) ->
    filelib:ensure_dir(BucketPath),
    MergedBucketPath = BucketPath ++ ".merge",
    TempBucketPath = BucketPath ++ ".tmp",

    %% Open the file...
    {ok, Out} = file:open(MergedBucketPath, [raw, write, delayed_write, binary, compressed]),
    {In, OldCount} = case file:open(BucketPath, [raw, read, read_ahead, binary, compressed]) of
        {ok, FH} -> 
            %% Read the first 8 bytes, for size.
            {ok, <<Int:64/integer>>} = file:read(FH, 8),
            {FH, Int};
        {error, enoent} -> 
            {enoent, 0}
    end,

    %% Write the new size...
    NewCount = OldCount + Count,
    file:write(Out, <<NewCount:64/integer>>),

    %% Do the merge...
    Value = read_value(In),
    file_merge_sort_inner(In, Value, Values, Out),
    
    %% Files are reused, so write a trailing eof marker on the
    %% outfile, otherwise we won't know when to stop when we read it
    %% in the next time.
    write_value(Out, eof),
    
    %% Close the files...
    case In of 
        enoent -> ignore;
        _ -> file:close(In)
    end,
    file:close(Out),

    %% Move the files around...
    file:rename(BucketPath, TempBucketPath),
    file:rename(MergedBucketPath, BucketPath),
    file:rename(TempBucketPath, MergedBucketPath),
    ok.


file_merge_sort_inner(_In, eof, [], _Out) ->
    ok;

file_merge_sort_inner(_In, eof, Values, Out) ->
    [write_value(Out, X) || X <- Values],
    ok;

file_merge_sort_inner(In, Value, [], Out) ->
    write_value(Out, Value),
    NextValue = read_value(In),
    file_merge_sort_inner(In, NextValue, [], Out);

file_merge_sort_inner(In, {Key1, Props1}, [{Key2, Props2}|Rest] = Values, Out) ->
    if 
        Key1 < Key2 -> 
            %% Merge Key1...
            write_value(Out, {Key1, Props1}),
            NextValue = read_value(In),
            file_merge_sort_inner(In, NextValue, Values, Out);

        Key1 > Key2 ->
            %% Merge Key2...
            write_value(Out, {Key2, Props2}),            
            file_merge_sort_inner(In, {Key1, Props1}, Rest, Out);

        Key1 == Key2 -> 
            %% Equal, so Merge Key2 and advance both...
            write_value(Out, {Key2, Props2}),
            NextValue = read_value(In),
            file_merge_sort_inner(In, NextValue, Rest, Out)
    end.


read_value(enoent) ->
    eof;
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

write_value(Out, Value) ->
    %% How many bytes should we write?
    BinValue = term_to_binary(Value),
    ValueSize = size(BinValue),

    %% Write the bytes.
    ok = file:write(Out, <<ValueSize:32/integer, BinValue/binary>>).
    
    
