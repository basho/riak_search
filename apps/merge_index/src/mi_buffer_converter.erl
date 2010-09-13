%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%
%% This module is a supervisor and gen_sever rolled into one.  The
%% supervisor's sole purpose is to monitor the one gen_server it owns,
%% and attempt a couple of restarts, or blow up if restarts happen too
%% fast.
%%
%% The supervisor is started by mi_server, and the gen_server alerts
%% mi_server to its presence.  mi_server talks directly to the worker
%% using the convert/3 function.  mi_server is linked to the supervisor,
%% so it gets an EXIT message only after the gen_server has reached its
%% max restart limit.
-module(mi_buffer_converter).

%-behaviour(supervisor). % not actually conflicting...
-behaviour(gen_server).

%% API
-export([start_link/2, convert/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% private callbacks
-export([start_worker/2]).

-record(state, {mi_server, mi_root}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(MIServerPid, MIServerRoot) ->
    supervisor:start_link(
      ?MODULE, [supervisor, MIServerPid, MIServerRoot]).

%% @private
start_worker(MIServerPid, MIServerRoot) ->
    gen_server:start_link(
      ?MODULE, [gen_server, MIServerPid, MIServerRoot], []).

convert(undefined, _Root, _Buffer) ->
    %% mi_server tried to convert a buffer before
    %% it had a registered converter: ignore
    ok;
convert(Converter, Root, Buffer) ->
    gen_server:cast(Converter, {convert, Root, Buffer}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using 
%% supervisor:start_link/[2,3], this function is called by the new process 
%% to find out about restart strategy, maximum restart frequency and child 
%% specifications.
%%--------------------------------------------------------------------
init([supervisor, MIServerPid, MIServerRoot]) ->
    AChild = {buffer_converter_worker,
              {mi_buffer_converter,start_worker,
               [MIServerPid, MIServerRoot]},
              permanent,2000,worker,[mi_buffer_converter]},
    {ok,{{one_for_all,2,1}, [AChild]}};
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([gen_server, MIServerPid, MIServerRoot]) ->
    mi_server:register_buffer_converter(MIServerPid, self()),
    {ok, #state{mi_server=MIServerPid, mi_root=MIServerRoot}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({convert, Root, Buffer}, #state{mi_root=Root}=State) ->
    %% Calculate the segment filename, open the segment, and convert.
    SNum  = mi_server:get_id_number(mi_buffer:filename(Buffer)),
    SName = filename:join(Root, "segment." ++ integer_to_list(SNum)),

    case mi_server:has_deleteme_flag(SName) of
        true ->
            %% remove files from a previously-failed conversion
            file:delete(mi_segment:data_file(SName)),
            file:delete(mi_segment:offsets_file(SName));
        false ->
            mi_server:set_deleteme_flag(SName)
    end,
    SegmentWO = mi_segment:open_write(SName),
    mi_segment:from_buffer(Buffer, SegmentWO),
    mi_server:buffer_to_segment(State#state.mi_server, Buffer, SegmentWO),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
