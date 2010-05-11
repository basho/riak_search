-module(raptor_conn_pool).

-behaviour(gen_server).

%% API
-export([start_link/0, add_conn/0, get_conn/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {tid}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

add_conn() ->
    gen_server:cast(?SERVER, {add_conn, self()}).

get_conn() ->
    gen_server:call(?SERVER, get_conn).

init([]) ->
    Tid = ets:new(?MODULE, [named_table]),
    Count = raptor_util:get_env(raptor, persistent_connections, 4),
    start_conns(Count),
    {ok, #state{tid=Tid}}.

handle_call(get_conn, _From, #state{tid=Tid}=State) ->
    Conn = case ets:first(Tid) of
               '$end_of_table' ->
                   {ok, Pid} = raptor_conn_sup:new_conn(false),
                   Pid;
               {CPid} ->
                   ets:delete(Tid, CPid),
                   CPid
           end,
    {reply, Conn, State};

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({add_conn, ConnPid}, #state{tid=Tid}=State) ->
    erlang:monitor(process, ConnPid),
    ets:insert_new(Tid, {ConnPid}),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, _Type, Pid, _Info}, #state{tid=Tid}=State) ->
    case ets:lookup(Tid, Pid) of
        [] ->
            {noreply, State};
        [{Pid}] ->
            ets:delete(?MODULE, Pid),
            raptor_conn_sup:new_conn(true),
            {noreply, State}
    end;

handle_info({start_conns, Count}, State) ->
    start_conns(Count),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
start_conns(0) ->
    ok;
start_conns(Count) ->
    raptor_conn_sup:new_conn(true),
    start_conns(Count - 1).
