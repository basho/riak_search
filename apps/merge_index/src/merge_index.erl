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
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-include_lib("kernel/include/file.hrl").

-export([
    %% API
    start_link/2,
    index/7,
    stream/7,
    info/4,
    info_range/6,
    is_empty/1,
    fold/3,
    drop/1
]).

start_link(Root, Config) ->
    gen_server:start_link(mi_server, [Root, Config], [{timeout, infinity}]).

index(ServerPid, Index, Field, Term, Value, Props, Timestamp) ->
    gen_server:call(ServerPid, 
        {index, Index, Field, Term, Value, Props, Timestamp}, infinity).

info(ServerPid, Index, Field, Term) ->
    gen_server:call(ServerPid, {info, Index, Field, Term}, infinity).

info_range(ServerPid, Index, Field, StartTerm, EndTerm, Size) ->
    gen_server:call(ServerPid,
        {info_range, Index, Field, StartTerm, EndTerm, Size}, infinity).

stream(ServerPid, Index, Field, Term, Pid, Ref, FilterFun) ->
    gen_server:call(ServerPid, 
        {stream, Index, Field, Term, Pid, Ref, FilterFun}, infinity).

is_empty(ServerPid) ->
    gen_server:call(ServerPid, is_empty, infinity).

fold(ServerPid, Fun, Acc) ->
    gen_server:call(ServerPid, {fold, Fun, Acc}, infinity).

drop(ServerPid) ->
    gen_server:call(ServerPid, drop, infinity).
