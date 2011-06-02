%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_sock_pool).

-behaviour(gen_server).

%% API
-export([start_link/3, checkout/1, checkin/2, current_count/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([new_conn/0, close/1, close_nonblocking/1]). % for unit testing
-endif.

%% wait up to 250ms before trying to checkout a connection again
-define(MAX_RETRY_WAIT, 250).

-record(state,
        {
          pool=[],          %% available connections
          inuse=[],         %% list of checked-out connections
          openmod,          %% module to use to start connections
          closemod,         %% module to use to close connections
          countfun,         %% how many connections to start
          initialized=false %% have any conns been inited?
         }).

-record(use,
        {
          ref,  %% monitor reverence
          conn  %% PID of the connection
         }).

%% thanks to Tony Rogvall for the idea
-record('DOWN',
	{
	  ref,   %% monitor reference
	  type,  %% type of object 'process'
	  id,    %% object id (pid)
	  reason %% reason for termination
	 }).

%% @spec start_link(atom(), {Open::atom(), Close::atom()}, function())
%%          -> {ok,Pid} | ignore | {error,Error}
%% @doc Start a new connection pool.
%%
%%      Name is the atom under which the pool will be registered.
%%
%%      Open should be the name of a module that exports a zero-arity
%%      function named 'new_conn', that returns {ok, pid()} after
%%      successfully starting a new connection.  Open:new_conn/0 will
%%      be called at least CountFun() times, plus as many times as
%%      necessary to replace connections that have failed.
%%
%%      Close should be the name of a module that exports an arity-one
%%      function named 'close', that takes a pid() (as returned from
%%      Open:new_conn/0) as its parameter, and closes the connection.
%%      This module should also export a function named
%%      'close_nonblocking', with the same contract as 'close', that
%%      is guaranteed not to block the caller while executing.
%%
%%      CountFun should be a zero-arity function that returns an
%%      integer, specifying the number of connections to keep alive.
start_link(Name, ConnMod={_,_}, CountFun) ->
    gen_server:start_link({local, Name}, ?MODULE,
                          [ConnMod, CountFun], []).

%% @spec checkout(atom()) -> {ok, pid()}
%% @doc Checkout a connection from the pool.  If no connections are
%%      available when this function is called, it will wait and try
%%      again until one is available.  This function does not return
%%      until either a connection is successfully checked out, or the
%%      gen_server for the pool dies.
checkout(Name) ->
    case gen_server:call(Name, checkout, infinity) of
        empty_pool ->
            timer:sleep(random:uniform(?MAX_RETRY_WAIT)),
            checkout(Name);
        R ->
            R
    end.

%% @spec checkin(atom(), pid()) -> ok
%% @doc Check a connection back into the pool.
checkin(Name, Conn) ->
    gen_server:cast(Name, {checkin, Conn}).

%% @spec current_count(atom()) -> integer()
%% @doc Get the number of connections currently available to be
%%      checked out
current_count(Name) ->
    gen_server:call(Name, current_count, infinity).

init([{OpenMod, CloseMod}, CountFun]) ->
    {ok, #state{countfun=CountFun, openmod=OpenMod, closemod=CloseMod}}.

handle_call(checkout, From,
            #state{initialized=false,
                   countfun=CountFun,
                   openmod=ConnMod
                  }=State) ->
    %% we've never started any connections - fire them all up
    Conns = CountFun(),
    Pool = add_to_pool(ConnMod, Conns),
    handle_call(checkout, From, State#state{initialized=true, pool=Pool});
handle_call(checkout, _From, #state{pool=[]}=State) ->
    %% nothing available for checkout right now
    {reply, empty_pool, State};
handle_call(checkout, {Pid,_}, #state{pool=[H|T], inuse=InUse0}=State) ->
    %% simple: hand back the available connection
    %% watch the pid, so we can cleanup the connect it's using if
    %%  it exits before checking the conn back in
    Ref = erlang:monitor(process, Pid),
    InUse = [#use{ref=Ref, conn=H}|InUse0],
    {reply, {ok, H}, State#state{pool=T, inuse=InUse}};
handle_call(current_count, _From, #state{pool=Pool}=State) ->
    %% grab the number of connections available for checkout
    {reply, length(Pool), State};
handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({checkin, Conn}, #state{pool=Pool, inuse=InUse0}=State) ->
    case is_process_alive(Conn) of
        true ->
            case lists:keytake(Conn, #use.conn, InUse0) of
                {value, #use{ref=Ref}, InUse} ->
                    %% don't watch the pid for exits any more
                    erlang:demonitor(Ref, [flush]),
                    {noreply, State#state{pool=[Conn|Pool],inuse=InUse}};
                false ->
                    %% bogus return - we don't know this connection
                    {noreply, State}
            end;
        false ->
            %% process death will have triggered handle_info(#'DOWN'{})
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'DOWN'{ref=Ref, id=DownPid},
            #state{openmod=OpenMod, closemod=CloseMod,
                   pool=Pool0, inuse=InUse0}=State) ->
    case lists:keytake(Ref, #use.ref, InUse0) of
        {value, #use{conn=Conn}, InUse} ->
            %% this is a 'DOWN' for a user of one of our connections
            %% can't be sure what state they left the conn in, so
            %%  terminate it, then fire up a new one when we get the
            %%  'DOWN' for the connection
            %%  Warning: this means that a connection that takes a
            %%  long time to close will leave the pool under-full
            %%  for a time, but it means we will never exceed the
            %%  maximum open connections specified by CountFun()
            CloseMod:close_nonblocking(Conn),
            Pool = Pool0;
        false ->
            %% this is a 'DOWN' for one of our connections
            case lists:keytake(DownPid, #use.conn, InUse0) of
                {value, #use{ref=UseRef}, InUse} ->
                    %% the connection was in use at the time
                    erlang:demonitor(UseRef, [flush]),
                    Pool1 = Pool0;
                false ->
                    %% the connection was not in use at the time
                    InUse = InUse0,
                    Pool1 = lists:delete(DownPid, Pool0)
            end,
            %% Create a new pid to replace the downed one
            Pool = Pool1 ++ [init_conn(OpenMod)]
    end,
    {noreply, State#state{pool=Pool, inuse=InUse}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{closemod=ConnMod, pool=Pool, inuse=InUse}) ->
    [ConnMod:close(Conn) || Conn <- Pool],
    [ConnMod:close(Conn) || #use{conn=Conn} <- InUse ],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @spec add_to_pool(atom(), integer()) -> [pid()]
%% @doc Spin up Count connections in a new pool.
add_to_pool(ConnMod, Count) ->
    add_to_pool(ConnMod, Count, []).

%% @spec add_to_pool(atom(), integer(), [pid()]) -> [pid()]
%% @doc Spin up Count new connections and add them to Pool.
add_to_pool(_ConnMod, 0, Pool) ->
    Pool;
add_to_pool(ConnMod, Count, Pool) ->
    Pid = init_conn(ConnMod),
    add_to_pool(ConnMod, Count - 1, [Pid|Pool]).

%% @spec init_conn(atom()) -> pid()
%% @doc Spin up a new connection.
init_conn(ConnMod) ->
    {ok, Pid} = ConnMod:new_conn(),
    erlang:monitor(process, Pid),
    Pid.
    
   
-ifdef(TEST).

new_conn() ->
    {ok, spawn(fun() -> receive done -> ok end end)}.

close(Pid) ->
    Pid ! done.

close_nonblocking(Pid) ->
    Pid ! done.

exhausted_test() ->
    ConnCount = 3,
    {ok, Pool} = start_link(sock_pool_ut, {?MODULE, ?MODULE},
                            fun() -> ConnCount end),
    try
        %% Lazy evaluation - does not open pool until first use
        ?assertEqual(0, ?MODULE:current_count(sock_pool_ut)),

        %% Checkout connection - make sure pool size is smaller
        {ok, Pid1} = checkout(sock_pool_ut),
        ?assertEqual(2, ?MODULE:current_count(sock_pool_ut)),
        {ok, _Pid2} = checkout(sock_pool_ut),
        {ok, _Pid3} = checkout(sock_pool_ut),
        ?assertEqual(0, ?MODULE:current_count(sock_pool_ut)),
        
        %% Pool is exhausted, delay checking in Pid1 and 
        %% request a new connection - should get Pid1.
        spawn(fun() -> timer:sleep(10), checkin(sock_pool_ut, Pid1) end),
        {ok, Pid1} = checkout(sock_pool_ut)
   after
        unlink(Pool),
        exit(Pool, kill)
    end.

exit_test() ->
    ConnCount = 5,
    {ok, Pool} = start_link(sock_pool_ut, {?MODULE, ?MODULE},
                            fun() -> ConnCount end),
    try
        %% Lazy evaluation - does not open pool until first use
        ?assertEqual(0, ?MODULE:current_count(sock_pool_ut)),

        %% Checkout connection - make sure pool size is smaller
        {ok, Pid} = checkout(sock_pool_ut),
        ?assertEqual(ConnCount-1, ?MODULE:current_count(sock_pool_ut)),

        %% Close the connection and make sure the process has exited
        erlang:monitor(process, Pid),
        close(Pid),
        receive
            #'DOWN'{id = Pid} ->
                ok
        after
            1000 ->
                ?assert(false)
        end,

        %% Make sure the pool is back up to size again
        ?assertEqual(ConnCount, ?MODULE:current_count(sock_pool_ut)),

        %% Checkin the dead process and make sure current
        %% count stays the same
        checkin(sock_pool_ut, Pid),
        ?assertEqual(ConnCount, ?MODULE:current_count(sock_pool_ut))
    after
        unlink(Pool),
        exit(Pool, kill)
    end.

leak_test() ->
    ConnCount = 5,
    {ok, Pool} = start_link(sock_pool_ut, {?MODULE, ?MODULE},
                            fun() -> ConnCount end),
    try
        %% Lazy evaluation - does not open pool until first use
        ?assertEqual(0, ?MODULE:current_count(sock_pool_ut)),

        %% Proc will wait for a close message, then die without
        %% returning its connection
        EunitPid = self(),
        Pid = spawn(fun() ->
                            {ok, PoolPid} = checkout(sock_pool_ut),
                            EunitPid ! {checked_out, PoolPid},
                            receive please_die -> ok
                            after 5000 -> ok
                            end
                    end),
        erlang:monitor(process, Pid),
        PoolPid = receive {checked_out, COPid} ->
                          erlang:monitor(process, COPid),
                          COPid
                  after 1000 ->
                          ?assert(false)
                  end,
        
        %% make sure pool size is smaller
        ?assertEqual(ConnCount-1, ?MODULE:current_count(sock_pool_ut)),

        Pid ! please_die,
        %% Close the connection and make sure the process has exited
        receive #'DOWN'{id = Pid} -> ok
        after 1000 -> ?assert(false)
        end,
        receive #'DOWN'{id = PoolPid} -> ok
        after 1000 -> ?assert(false)
        end,

        %% Make sure the pool is back up to size again
        ?assertEqual(ConnCount, ?MODULE:current_count(sock_pool_ut))
    after
        unlink(Pool),
        exit(Pool, kill)
    end.

-endif.
