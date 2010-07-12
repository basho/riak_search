%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

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
