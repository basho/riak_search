-module(raptor_monitor).

-behaviour(gen_server).

%% API
-export([start_link/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {port,
                sock,
                portnum}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?SERVER, stop_analyzer).

init([]) ->
    error_logger:info_msg("Raptor monitor starting (~p)~n", [self()]),
    {MinRam, MaxRam, DataDir, PortNum} = read_config(),
    CmdDir = filename:join([priv_dir(), "raptor_server"]),
    Cmd = filename:join([CmdDir, "raptor_server.sh"]),
    case catch erlang:open_port({spawn_executable, Cmd}, [stderr_to_stdout,
                                                          {args, [MinRam,
                                                                  MaxRam,
                                                                  integer_to_list(PortNum),
                                                                  DataDir]},
                                                          {line, 2048},
                                                          {cd, CmdDir}]) of
        {'EXIT', Error} ->
            {stop, Error};
        Port when is_port(Port) ->
            case connect({127,0,0,1}, PortNum + 1, [], 10) of
                {ok, Sock} ->
                    erlang:link(Port),
                    {ok, #state{port=Port, portnum=PortNum, sock=Sock}};
                _ ->
                    {stop, connect_error}
            end
    end.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, _}, State) ->
    restart(State);
handle_info({tcp_error, _, _}, State) ->
    restart(State);
handle_info({_Port, {eol, Msg}}, State) ->
    error_logger:info_msg("~p~n", [Msg]),
    {noreply, State};
handle_info({_Port, {noeol, Msg}}, State) ->
    error_logger:info_msg("~p", [Msg]),
    {noreply, State};
handle_info({_Port, {data, {eol, Msg}}}, State) ->
    error_logger:info_msg("~p", [Msg]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions
read_config() ->
    MinRam = app_helper:get_env(raptor, min_backend_mem, 512),
    MaxRam = app_helper:get_env(raptor, max_backend_mem, 1024),
    BackendPort = app_helper:get_env(raptor, backend_port, 5099),
    DataDir = app_helper:get_env(raptor, raptor_backend_root, "data/raptor"),
    {integer_to_list(MinRam), integer_to_list(MaxRam), filename:absname(DataDir), BackendPort}.

connect(_Addr, _PortNum, _Options, 0) ->
    {error, no_connection};
connect(Addr, PortNum, Options, Tries) ->
    case gen_tcp:connect(Addr, PortNum, Options) of
        {ok, Sock} ->
            {ok, Sock};
        _ ->
            timer:sleep(250),
            connect(Addr, PortNum, Options, Tries - 1)
    end.

priv_dir() ->
    case code:priv_dir(raptor) of
        {error, bad_name} ->
            Path0 = filename:dirname(code:which(?MODULE)),
            Path1 = filename:absname_join(Path0, ".."),
            filename:join([Path1, "priv"]);
        Path ->
            filename:absname(Path)
    end.

restart(State) ->
    error_logger:warning_msg("Restarting Raptor monitor (~p)~n", [self()]),
    {stop, normal, State}.
