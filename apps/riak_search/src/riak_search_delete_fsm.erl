%%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_delete_fsm).
-behaviour(gen_fsm).

%% API
-export([start_link/0]).
-export([delete_terms/2, delete_terms/3, done/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([idle/2, idle/3,
         receiving/2, receiving/3]).

-include("riak_search.hrl").

-define(RECV_TIMEOUT_USECS, 60000).
-define(DELETE_TIMEOUT_USECS, 60000).

-record(state, {total=0,           % Total number of terms sent to all vnodes
                acks=0,            % Number of deleted messages received
                timeouts=0,        % Number of timeout messages received
                num_terms=0,       % Number of terms in the terms list below
                ref,               % Reference for current batch of terms sent out
                replies_left,      % Count of replies pending
                timer,             % Timer reference for batch
                overload_waiters=[], % List to reply to once out of overload
                done_waiters=[],   % List to reply to once terms is empty
                batch_size,        % Size of delete batches to send
                overload_thresh,   % Threshold for num_terms to trigger overload
                terms=[]}).        % Terms left to delete,


-type delete() :: term().
-type field() :: term().
-type idxterm() :: term().
-type idxvalue() :: term().
-type props() :: list().
-type delete_term() :: {delete(),field(),idxterm(),idxvalue(),props()}.
-type delete_terms() :: [delete_term()].
-type overload_state() :: ok | overload.
-type statename() :: idle | receiving.
-type event() :: {reference(), {deleted, node()}} |
                 {reference(), recv_timeout} |
                 empty.
-type sync_event() :: {delete, delete_terms()} |
                      done.


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error,term()}.
start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

%% Delete the list of delete/field/term/value/props
-spec delete_terms(pid(), delete_terms()) -> ok.
delete_terms(Pid, Terms) ->
    delete_terms(Pid, Terms, ?DELETE_TIMEOUT_USECS).

-spec delete_terms(pid(), delete_terms() ,timeout()) -> ok.
delete_terms(Pid, Terms, Timeout) ->
    %% Add the keyclock
    K = riak_search_utils:current_key_clock(),
    %% Terms1 = [{I,F,T,V,P,K} || {I,F,T,V,P} <- Terms],
    %% Rc = gen_fsm:sync_send_event(Pid, {delete, Terms1}, Timeout),
    F = fun({I,F,T,V}, Status) ->
                NewStatus = gen_fsm:sync_send_event(Pid, {delete, [{I,F,T,V,K}]}, Timeout),
                case Status of
                    ok ->
                        NewStatus;
                    _ ->
                        Status
                end
        end,
    lists:foldl(F, ok, Terms).

%% Signal we are done and wait for the FSM to complete and exit
-spec done(pid()) -> ok.
done(Pid) ->
    gen_fsm:sync_send_event(Pid, done, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @private
init([]) ->
    BatchSize = app_helper:get_env(riak_search, delete_batch_size, 20),
    OverloadThresh = app_helper:get_env(riak_search, delete_overload_thresh, 100),
    {ok, idle, #state{batch_size=BatchSize,overload_thresh=OverloadThresh}}.

%% @private
%% Handle gen_fsm:send_event
-spec idle(event(),#state{}) -> {next_state, statename(), #state{}} | {stop, term(), #state{}}.
idle({_Ref, {deleted, _Partition}}, State) ->
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
-spec idle(sync_event(),term(),#state{}) -> {reply, term(), statename(), #state{}} |
                                            {next_state, statename(), #state{}} |
                                            {stop, term(), term(), #state{}}.
idle({delete, Terms}, From, #state{terms = []} = State) ->
    {Status, State1} = add_terms(Terms, From, State),
    {StateName, NewState} = send_batch(State1),
    case Status of
        ok ->
            {reply, ok, StateName, NewState};
        overload ->
            {next_state, StateName, NewState}
    end;
idle(done, _From, #state{terms = []} = State) ->
    {stop, normal, ok, State}.

%% @private
%% handle gen_fsm:send_event
-spec receiving(event(),#state{}) -> {next_state, statename(), #state{}}.
receiving({Ref, {deleted, _Node}}, #state{acks = Acks, ref = Ref}=State) ->
    %% Reply for the current batch
    case State#state.replies_left - 1 of
        0 ->
            cancel_recv_timeout(State#state.timer),
            {StateName, NewState} = send_batch(State),
            {next_state, StateName, NewState};
        RepliesLeft ->
            {next_state, receiving, State#state{acks = Acks + 1,
                                                replies_left = RepliesLeft}}
    end;
receiving({Ref, recv_timeout}, #state{timeouts=Timeouts,ref=Ref} = State) ->    
    %% Receive timeout for current batch - for now just continue
    %% on to the next batch
    {StateName, NewState} = send_batch(State#state{timeouts=Timeouts+1,
                                                   timer=undefined}),
    {next_state, StateName, NewState};

receiving({_Ref, {deleted, _Node}}, State) ->
    %% Ignore stale reply from previous batch
    {next_state, receiving, State};
receiving(empty, State) ->
    %% Got given some more work after the terms list was empty - ignore
    {next_state, receiving, State}.

%% @private
%% Handle sync_send_event
-spec receiving(sync_event(),term(),#state{}) -> {reply, term(), statename(), #state{}} |
                                                 {next_state, statename(), #state{}}.
receiving({delete, Terms}, From, State) ->
    case add_terms(Terms, From, State) of
        {ok, NewState} ->
            {reply, ok, receiving, NewState};
        {overload, NewState} ->
            {next_state, receiving, NewState}
    end;

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

%% @private
%% Add new terms to by deleted
-spec add_terms(delete_terms(), term(), #state{}) -> {overload_state(), #state{}}.
add_terms(NewTerms, From, State) ->
    UpdTerms = State#state.terms ++ NewTerms,
    NumNewTerms = length(NewTerms),
    UpdNumTerms = State#state.num_terms + NumNewTerms,
    case UpdNumTerms < State#state.overload_thresh of
        true ->
            {ok, State#state{terms = UpdTerms,
                             num_terms = UpdNumTerms}};
        _ ->
            {overload, State#state{terms = UpdTerms,
                                   num_terms = UpdNumTerms,
                                   overload_waiters =
                                       [From | State#state.overload_waiters]}}
    end.
                                   
    
%% @private
%% Send the next batch of terms
-spec send_batch(#state{}) -> {statename(), #state{}}.
send_batch(#state{terms=[]}=State) ->
    [gen_fsm:reply(From, ok) || From <- State#state.overload_waiters],
    gen_fsm:send_event(self(), empty),
    {idle, State#state{overload_waiters=[]}};
send_batch(State) ->
    {Rest, PlTerms, TermCount} = lookup_preflist(State#state.batch_size,
                                                 0,
                                                 State#state.terms, 
                                                 orddict:new(),
                                                 riak_search_utils:n_val()),
    Ref = make_ref(),
    Sent = orddict:fold(fun(Pl, NodeTerms, SentAcc) ->
                                riak_search_vnode:multi_delete(Pl, NodeTerms, {fsm, Ref, self()}),
                                SentAcc + 1
                        end, 0, PlTerms),
    Timer = gen_fsm:send_event_after(?RECV_TIMEOUT_USECS, {Ref, recv_timeout}),
    UpdNumTerms = State#state.num_terms - TermCount,
    State1 = State#state{total = State#state.total + TermCount,
                         terms = Rest,
                         num_terms = UpdNumTerms,
                         ref = Ref,
                         replies_left = Sent,
                         timer = Timer},
    %% Check if sending this batch clears the overload
    case UpdNumTerms < State#state.overload_thresh of
        true ->
            [gen_fsm:reply(From, ok) || From <- State#state.overload_waiters],
            {receiving, State1#state{overload_waiters=[]}};
        false ->
            {receiving, State1}
    end.

%% @private
%% Cancel a timer if it was set
-spec cancel_recv_timeout(undefined|reference()) -> ok.
cancel_recv_timeout(undefined) ->
    ok;
cancel_recv_timeout(Timer) ->
    gen_fsm:cancel_timer(Timer).

%% Lookup the preflist for each of the terms and build an orddict by 
%% preflist entry {7,n1} -> [{I1,F1,T1,V1},{I2,F2,T2,V2}]
lookup_preflist(0, TermCount, Terms, PlTerms, _N) ->
    {Terms, PlTerms, TermCount};
lookup_preflist(_BatchLeft, TermCount, [], PlTerms, _N) ->
    {[], PlTerms, TermCount};
lookup_preflist(BatchLeft, TermCount, [IFTV|Terms], PlTerms, N) ->
    {Index,Field,Term,_Value,_KeyClock} = IFTV,
    Partition = riak_search_utils:calc_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N, riak_search),
    NewPlTerms = lists:foldl(fun(Pl,PlTermsAcc) ->
                                     orddict:append_list(Pl,[IFTV],PlTermsAcc)
                             end, PlTerms, Preflist),
    lookup_preflist(BatchLeft-1, TermCount+1, Terms, NewPlTerms, N).
