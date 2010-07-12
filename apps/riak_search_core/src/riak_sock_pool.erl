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
-export([new_conn/0]). % for unit testing
-endif.

-define(MAX_RETRY_WAIT, 250).

-record(state, {pool=[],
                openmod,
                closemod,
                countfun,
                initialized=false}).

%% thanks to Tony Rogvall for the idea
-record('DOWN',
	{
	  ref,   %% monitor reference
	  type,  %% type of object 'process'
	  id,    %% object id (pid)
	  reason %% reason for termination
	 }).

start_link(Name, ConnMod, CountFun) ->
    gen_server:start_link({local, Name}, ?MODULE, [ConnMod, CountFun], []).

checkout(Name) ->
    case gen_server:call(Name, checkout) of
        empty_pool ->
            timer:sleep(random:uniform(?MAX_RETRY_WAIT)),
            checkout(Name);
        R ->
            R
    end.

checkin(Name, Conn) ->
    gen_server:cast(Name, {checkin, Conn}).

current_count(Name) ->
    gen_server:call(Name, current_count).

init([{OpenMod, CloseMod}, CountFun]) ->
    process_flag(trap_exit, true),
    {ok, #state{countfun=CountFun, openmod=OpenMod, closemod=CloseMod}}.

handle_call(checkout, _From, #state{countfun=CountFun, openmod=ConnMod, initialized=false}=State) ->
    Conns = CountFun(),
    [H|T] = add_to_pool(ConnMod, Conns),
    {reply, {ok, H}, State#state{pool=T, initialized=true}};
handle_call(checkout, _From, #state{pool=[]}=State) ->
    {reply, empty_pool, State};
handle_call(checkout, _From, #state{pool=[H|T]}=State) ->
    {reply, {ok, H}, State#state{pool=T}};
handle_call(current_count, _From, #state{pool=Pool}=State) ->
    {reply, length(Pool), State};
handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({checkin, Conn}, #state{pool=Pool}=State) ->
    case is_process_alive(Conn) of
        true ->
            {noreply, State#state{pool=Pool ++ [Conn]}};
        false ->
            %% Death of the process will have triggered handle_info(#'DOWN'{})
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'DOWN'{id=DownPid}, #state{openmod=ConnMod, pool=Pool}=State) ->
    %% Create a new pid to replace the downed one
    Pool1 = lists:delete(DownPid, Pool) ++ [init_conn(ConnMod)],
    {noreply, State#state{pool=Pool1}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{closemod=ConnMod, pool=Pool}) ->
    [ConnMod:close(Conn) || Conn <- Pool],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

add_to_pool(ConnMod, Count) ->
    add_to_pool(ConnMod, Count, []).

add_to_pool(_ConnMod, 0, Pool) ->
    Pool;
add_to_pool(ConnMod, Count, Pool) ->
    Pid = init_conn(ConnMod),
    add_to_pool(ConnMod, Count - 1, [Pid|Pool]).

init_conn(ConnMod) ->
    {ok, Pid} = ConnMod:new_conn(),
    erlang:monitor(process, Pid),
    Pid.
    
   
-ifdef(TEST).

new_conn() ->
    {ok, spawn(fun() -> receive done -> ok end end)}.

close(Pid) ->
    Pid ! done.


exhausted_test() ->
    ConnCount = 3,
    {ok, Pool} = start_link(sock_pool_ut, {?MODULE, ?MODULE}, fun() -> ConnCount end),
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
    {ok, Pool} = start_link(sock_pool_ut, {?MODULE, ?MODULE}, fun() -> ConnCount end),
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

        %% Checkin the dead process and make sure current count stays the same
        checkin(sock_pool_ut, Pid),
        ?assertEqual(ConnCount, ?MODULE:current_count(sock_pool_ut))
    after
        unlink(Pool),
        exit(Pool, kill)
    end.

-endif.
