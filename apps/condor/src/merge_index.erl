%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(merge_index).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-include_lib("kernel/include/file.hrl").

-export([
    %% API
    start_link/1,
    stop/1,
    index/7, index/2, 
    stream/7,
    range/9,
    info/4,
    is_empty/1,
    fold/3,
    drop/1,
    compact/1
]).

start_link(Root) ->
    gen_server:start_link(mi_server, [Root], [{timeout, infinity}]).

stop(_ServerPid) ->
    ok.

index(ServerPid, Index, Field, Term, Value, Props, Timestamp) ->
    index(ServerPid, [{Index, Field, Term, Value, Props, Timestamp}]).

index(ServerPid, Postings) ->
    gen_server:call(ServerPid, {index, Postings}, infinity).

info(ServerPid, Index, Field, Term) ->
    gen_server:call(ServerPid, {info, Index, Field, Term}, infinity).

stream(ServerPid, Index, Field, Term, Pid, Ref, FilterFun) ->
    gen_server:call(ServerPid, 
        {stream, Index, Field, Term, Pid, Ref, FilterFun}, infinity).

range(ServerPid, Index, Field, StartTerm, EndTerm, Size, Pid, Ref, FilterFun) ->
    gen_server:call(ServerPid, 
        {range, Index, Field, StartTerm, EndTerm, Size, Pid, Ref, FilterFun}, infinity).

is_empty(ServerPid) ->
    gen_server:call(ServerPid, is_empty, infinity).

fold(ServerPid, Fun, Acc) ->
    gen_server:call(ServerPid, {fold, Fun, Acc}, infinity).

drop(ServerPid) ->
    gen_server:call(ServerPid, drop, infinity).

compact(ServerPid) ->
    {ok, Ref} = gen_server:call(ServerPid, {start_compaction, self()}, infinity),

    %% TODO - This is shaky, but good enough for version one. If the
    %% compaction process crashes, then we sit here waiting forever.
    receive 
        {compaction_complete, Ref, OldSegments, OldBytes} -> 
            {ok, OldSegments, OldBytes}
    end.
