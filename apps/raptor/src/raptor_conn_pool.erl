-module(raptor_conn_pool).

-behaviour(gen_server).

%% API
-export([start_link/0, checkout/0, checkin/1, size/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(MAX_RETRY_WAIT, 250).

-record(state, {pool}).

%% thanks to Tony Rogvall for the idea
-record('DOWN',
	{
	  ref,   %% monitor reference
	  type,  %% type of object 'process'
	  id,    %% object id (pid)
	  reason %% reason for termination
	 }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

checkout() ->
    case gen_server:call(?SERVER, checkout) of
        empty_pool ->
            %error_logger:info_msg("(~p) raptor_conn_pool empty. Retrying...~n", [self()]),
            timer:sleep(random:uniform(?MAX_RETRY_WAIT)),
            checkout();
        R ->
            R
    end.

checkin(Conn) ->
    gen_server:cast(?SERVER, {checkin, Conn}).

size() ->
    gen_server:call(?SERVER, size).

init([]) ->
    process_flag(trap_exit, true),
    Conns = raptor_util:get_env(raptor, backend_conn_count, 10),
    Pool = init_pool(Conns),
    {ok, #state{pool=Pool}}.

handle_call(checkout, _From, #state{pool=[]}=State) ->
    {reply, empty_pool, State};
handle_call(checkout, _From, #state{pool=[H|T]}=State) ->
    {reply, {ok, H}, State#state{pool=T}};
handle_call(size, _From, #state{pool=Pool}=State) ->
    {reply, length(Pool), State};
handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({checkin, Conn}, #state{pool=Pool}=State) ->
    {noreply, State#state{pool=Pool ++ [Conn]}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'DOWN'{id=DownPid}, #state{pool=Pool0}=State) ->
    Pool1 = lists:delete(DownPid, Pool0),
    DesiredCount = raptor_util:get_env(raptor, backend_cons, 10),
    Pool2 = Pool1 ++ init_pool(DesiredCount - length(Pool1)),
    {noreply, State#state{pool=Pool2}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{pool=Pool}) ->
    [raptor_conn:close(Conn) || Conn <- Pool],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

init_pool(Count) ->
    init_pool(Count, []).

init_pool(0, Pool) ->
    Pool;
init_pool(Count, Pool) ->
    {ok, Pid} = raptor_conn_sup:new_conn(),
    erlang:monitor(process, Pid),
    init_pool(Count - 1, [Pid|Pool]).
