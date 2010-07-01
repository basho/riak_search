-module(riak_sock_pool).

-behaviour(gen_server).

%% API
-export([start_link/3, checkout/1, checkin/2, current_count/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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
    {noreply, State#state{pool=Pool ++ [Conn]}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'DOWN'{id=DownPid}, #state{openmod=ConnMod, countfun=CountFun, pool=Pool0}=State) ->
    Pool1 = lists:delete(DownPid, Pool0),
    DesiredCount = CountFun(),
    Pool2 = Pool1 ++ add_to_pool(ConnMod, DesiredCount - length(Pool1)),
    {noreply, State#state{pool=Pool2}};

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
    {ok, Pid} = ConnMod:new_conn(),
    erlang:monitor(process, Pid),
    add_to_pool(ConnMod, Count - 1, [Pid|Pool]).
