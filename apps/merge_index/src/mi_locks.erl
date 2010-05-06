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
    claim/2,
    release/2,
    when_free/3
]).

-record(lock, {
    key,
    count,
    funs=[]
}).

new() -> [].

claim(Key, Locks) ->
    case lists:keyfind(Key, #lock.key, Locks) of
        Lock = #lock { count=Count } ->
            NewLock = Lock#lock { count=Count + 1 },
            lists:keystore(Key, #lock.key, Locks, NewLock);

        false ->
            NewLock = #lock { key=Key, count=1, funs=[] },
            lists:keystore(Key, #lock.key, Locks, NewLock)
    end.

release(Key, Locks) ->
    case lists:keyfind(Key, #lock.key, Locks) of
        #lock { count=1, funs=Funs } ->
            [X() || X <- Funs],
            lists:keydelete(Key, #lock.key, Locks);

        Lock = #lock { count=Count } ->
            NewLock = Lock#lock { count = Count - 1 },
            lists:keystore(Key, #lock.key, Locks, NewLock);

        false ->
            throw({lock_does_not_exist, Key})
    end.

%% Run the provided function when the key is free. If the key is
%% currently free, then this is run immeditaely.
when_free(Key, Fun, Locks) ->
    case lists:keyfind(Key, #lock.key, Locks) of
        false ->
            Fun(),
            Locks;

        #lock { count=0, funs=Funs } ->
            [X() || X <- [Fun|Funs]],
            lists:keydelete(Key, #lock.key, Locks);
            
        Lock = #lock { funs=Funs} ->
            NewLock = Lock#lock { funs=[Fun|Funs] },
            lists:keystore(Key, #lock.key, Locks, NewLock)
    end.
