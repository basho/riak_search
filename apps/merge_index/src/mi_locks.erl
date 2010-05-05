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
-module(mi_locks).
-include("merge_index.hrl").
-author("Rusty Klophaus <rusty@basho.com>").
-export([
    new/0,
    claim/3, claim/2,
    release/2
]).

-record(lock, {
    key,
    count,
    funs=[]
}).

new() -> [].

claim(Key, Fun, Locks) ->
    case lists:keyfind(Key, #lock.key, Locks) of
        #lock { count=Count, funs=Funs} ->
            NewLock = #lock { key=Key, count=Count + 1, funs=[Fun|Funs] };
        false ->
            NewLock = #lock { key=Key, count=1, funs=[Fun] }
    end,
    lists:keystore(Key, #lock.key, Locks, NewLock).

claim(Key, Locks) ->
    case lists:keyfind(Key, #lock.key, Locks) of
        #lock { count=Count, funs=Funs} ->
            NewLock = #lock { key=Key, count=Count + 1, funs=Funs };
        false ->
            NewLock = #lock { key=Key, count=1, funs=[] }
    end,
    lists:keystore(Key, #lock.key, Locks, NewLock).

release(Key, Locks) ->
    case lists:keyfind(Key, #lock.key, Locks) of
        Lock = #lock { count=1, funs=Funs } ->
            [X() || X <- Funs],
            Locks -- [Lock];
        Lock = #lock { count=Count } ->
            NewLock = Lock#lock { count = Count - 1 },
            lists:keystore(Key, #lock.key, Locks, NewLock);
        false ->
            throw({lock_does_not_exist, Key})
    end.
