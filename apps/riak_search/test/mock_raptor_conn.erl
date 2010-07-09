%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% Mock module for testing raptor conn timeout handling in
%% riak_search_raptor_backend
%% -------------------------------------------------------------------
-module(mock_raptor_conn).

-behaviour(gen_server).

%% API
-export([new_conn/0,close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {mode}).

%%%===================================================================
%%% API
%%%===================================================================

new_conn() ->
    gen_server:start_link(?MODULE, [], []).
close(Pid) ->
    gen_server:call(Pid, close_conn).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.
handle_call(close_conn, _From, State) ->
    {stop, normal, ok, State};
handle_call({multistream, Pid, ClientRef, Rec}, From, State) ->
    %% have to rewrite multistream as stream
    handle_call({stream, Pid, ClientRef, Rec}, From, State);
handle_call({info_range, Pid, ClientRef, Rec}, From, State) ->
    %% have to rewrite info_range as info
    handle_call({info, Pid, ClientRef, Rec}, From, State);
handle_call({ReqType, Pid, _ClientRef, _Rec}, _From, State) when 
      is_atom(ReqType), is_pid(Pid) ->
    Ref = make_ref(),
    Pid ! {ReqType, Ref, timeout},
    {reply, {ok, Ref}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
