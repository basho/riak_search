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
-module(riak_search_index_fsm).
-behaviour(gen_fsm).

%% API
-export([start_link/0]).
-export([index_terms/2, done/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([idle/2, idle/3,
         receiving/2, receiving/3]).

-define(BATCH_SIZE, 20).
-define(RECV_TIMEOUT_USECS, 5000).
-define(INDEX_TIMEOUT_USECS, 5000).

-record(state, {terms=[],          % Terms left to index
                ref,               % Reference for current batch of terms sent out
                replies_left,      % Count of replies pending
                timer,             % Timer reference for batch
                done_waiters=[]}). % List to reply to once terms is empty

-type index() :: term().
-type field() :: term().
-type idxterm() :: term().
-type idxvalue() :: term().
-type props() :: list().


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error,term()}.
start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

%% Index the list of index/field/term/value/props
-spec index_terms(pid(), [{index(),field(),idxterm(),idxvalue(),props()}]) -> ok.
index_terms(Pid, Terms) ->
    gen_fsm:sync_send_event(Pid, {index, Terms}, ?INDEX_TIMEOUT_USECS).

%% Signal we are done and wait for the FSM to complete and exit
-spec done(pid()) -> ok.
done(Pid) ->
    gen_fsm:sync_send_event(Pid, done, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, idle, #state{}}.

%% @private
%% Handle gen_fsm:send_event
idle({_Ref, {w, _Node}}, State) ->
    %% Ignore stale reply from previous batch
    {next_state, idle, State};

%% send_batch/1 ran out of terms to send - decide if caller is waiting
%% for us to exit.
idle(empty, State) ->
    case State#state.done_waiters of
        [] ->
            {next_state, idle, State};
        Waiters ->
            lists:foreach(fun(From) -> gen_fsm:reply(From, ok) end, Waiters),
            {stop, normal, State}
    end.

%% @private
%% Handle sync_send_event
idle({index, Terms}, _From, #state{terms = []} = State) ->
    {StateName, NewState} = send_batch(State#state{terms = Terms}),
    {reply, ok, StateName, NewState};
idle(done, _From, #state{terms = []} = State) ->
    {stop, normal, ok, State}.

%% @private
%% handle gen_fsm:send_event
receiving({Ref, {indexed, _Node}}, #state{ref = Ref}=State) ->
    %% Reply for the current batch
    case State#state.replies_left - 1 of
        0 ->
            cancel_recv_timeout(State#state.timer),
            {StateName, NewState} = send_batch(State),
            {next_state, StateName, NewState};
        RepliesLeft ->
            {next_state, receiving, State#state{replies_left = RepliesLeft}}
    end;
receiving({Ref, recv_timeout}, #state{ref=Ref} = State) ->    
    %% Receive timeout for current batch - for now just continue
    %% on to the next batch
    {StateName, NewState} = send_batch(State#state{timer=undefined}),
    {next_state, StateName, NewState};

receiving({_Ref, {indexed, _Node}}, State) ->
    %% Ignore stale reply from previous batch
    {next_state, waiting, State};
receiving(empty, State) ->
    %% Got given some more work after the terms list was empty - ignore
    {next_state, receiving, State}.

%% @private
%% Handle sync_send_event
receiving({index, Terms}, _From, State) ->
    {reply, ok, receiving, State#state{terms = State#state.terms ++ Terms}};

receiving(done, From, State) ->
    {next_state, receiving, 
     State#state{done_waiters = [From | State#state.done_waiters]}}.

                
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, not_implemented, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

send_batch(#state{terms=[]}=State) ->
    gen_fsm:send_event(self(), empty),
    {idle, State};
send_batch(State) ->
    {Rest, PlTerms} = lookup_preflist(?BATCH_SIZE, State#state.terms, 
                                      orddict:new(), riak_search_utils:n_val()),
    Ref = make_ref(),
    Sent = orddict:fold(fun(Pl, NodeTerms, SentAcc) ->
                                riak_search_vnode:multi_index(Pl, NodeTerms, {fsm, Ref, self()}),
                                SentAcc + 1
                        end, 0, PlTerms),
    Timer = gen_fsm:send_event_after(?RECV_TIMEOUT_USECS, {Ref, recv_timeout}),
    {receiving, State#state{terms = Rest,
                            ref = Ref,
                            replies_left = Sent,
                            timer = Timer}}.    

cancel_recv_timeout(undefined) ->
    ok;
cancel_recv_timeout(Timer) ->
    gen_fsm:cancel_timer(Timer).

%% Lookup the preflist for each of the terms and build an orddict by 
%% preflist entry {7,n1} -> [{I1,F1,T1,V1,P1},{I2,F2,T2,V2,P2}]
lookup_preflist(0, Terms, PlTerms, _N) ->
    {Terms, PlTerms};
lookup_preflist(_BatchLeft, [], PlTerms, _N) ->
    {[], PlTerms};
lookup_preflist(BatchLeft, [IFTVP|Terms], PlTerms, N) ->
    {Index,Field,Term,_Value,_Props} = IFTVP,
    Partition = riak_search_utils:calc_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    NewPlTerms = lists:foldl(fun(Pl,PlTermsAcc) ->
                                     orddict:append_list(Pl,[IFTVP],PlTermsAcc)
                             end, PlTerms, Preflist),
    lookup_preflist(BatchLeft-1, Terms, NewPlTerms, N).





