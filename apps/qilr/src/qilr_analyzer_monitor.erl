%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(qilr_analyzer_monitor).

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
    error_logger:info_msg("analysis server monitor starting (~p)~n", [self()]),
    {ok, PortNum} = application:get_env(analysis_port),
    case application:get_env(analysis_port) of
        {ok, PortNum} when is_integer(PortNum) ->
            CmdDir = filename:join([priv_dir(), "analysis_server"]),
            Cmd = filename:join([CmdDir, "analysis_server.sh"]),
            case catch erlang:open_port({spawn_executable, Cmd}, [stderr_to_stdout,
                                                                  {env, build_env()},
                                                                  {args, [integer_to_list(PortNum)]},
                                                                  {cd, CmdDir}]) of
                {'EXIT', Error} ->
                    error_logger:error_msg("Could not start analysis server: ~p~n", [Error]),
                    {stop, Error};
                Port when is_port(Port) ->
                    case connect({127,0,0,1}, PortNum + 1, [], 10) of
                        {ok, Sock} ->
                            erlang:link(Port),
                            erlang:link(Sock),
                            {ok, #state{port=Port, portnum=PortNum, sock=Sock}};
                        Error ->
                            error_logger:error_msg("Could not connect to analysis_server: ~p~n", [Error]),
                            {stop, connect_error}
                    end
            end;
        _ ->
            {stop, {error, missing_port}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, _}, State) ->
    restart(State);
handle_info({tcp_error, _, _}, State) ->
    restart(State);
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions
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
    case code:priv_dir(qilr) of
        {error, bad_name} ->
            Path0 = filename:dirname(code:which(?MODULE)),
            Path1 = filename:absname_join(Path0, ".."),
            filename:join([Path1, "priv"]);
        Path ->
            filename:absname(Path)
    end.

restart(State) ->
    error_logger:warning_msg("Restarting analysis server monitor (~p)~n", [self()]),
    {stop, normal, State}.

build_env() ->
    case application:get_env(riak_search, java_home) of
        undefined ->
            throw({error, missing_java_home});
        {ok, JH} ->
            JavaPath = filename:join([JH,"bin","java"]),
            case filelib:is_file(JavaPath) of
                false ->
                    error_logger:error_msg(
                      "Unable to find java executable.~n"
                      "Please check the java_home setting in the"
                      " riak_search section of your app.config.~n"
                      "(~s does not exist)~n",
                      [JavaPath]),
                    throw({error, invalid_java_home});
                true ->
                    [{"JAVA_HOME", JH}]
            end
    end.
