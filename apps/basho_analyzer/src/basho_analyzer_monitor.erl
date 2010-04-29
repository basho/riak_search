-module(basho_analyzer_monitor).

-behaviour(gen_server).

-include("analysis_pb.hrl").

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
    process_flag(trap_exit, true),
    {ok, PortNum} = application:get_env(analysis_port),
    case application:get_env(analysis_port) of
        {ok, PortNum} when is_integer(PortNum) ->
            CmdDir = filename:join([code:priv_dir(basho_analyzer), "analysis_server"]),
            Cmd = filename:join([CmdDir, "analysis_server.sh"]),
            case catch erlang:open_port({spawn, Cmd}, [stderr_to_stdout,
                                                       {cd, CmdDir}]) of
                {'EXIT', Error} ->
                    {stop, Error};
                Port when is_port(Port) ->
                    timer:sleep(1000),
                    {ok, Sock} = gen_tcp:connect({127,0,0,1}, PortNum, [{active, false},
                                                                        binary, {packet, 4}]),
                    erlang:link(Port),
                    erlang:link(Sock),
                    {ok, #state{port=Port, portnum=PortNum, sock=Sock}}
            end;
        _ ->
            {stop, {error, missing_port}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Sock, _}, #state{portnum=PortNum, sock=Sock}=State) ->
    error_logger:info_msg("Re-establishing connection"),
    gen_tcp:close(Sock),
    {ok, NewSock} = gen_tcp:connect({127,0,0,1}, PortNum, [{active, false}, binary, {packet, 4}]),
    {noreply, State#state{sock=NewSock}};

handle_info({Port, Message}, #state{port=Port}=State) ->
    error_logger:error_msg("Java process error: ~p~n", [Message]),
    {stop, java_error, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{sock=Sock}) ->
    StopCmd = #analysisrequest{text= <<"__basho_analyzer_monitor_stop__">>},
    gen_tcp:send(Sock, analysis_pb:encode_analysisrequest(StopCmd)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
