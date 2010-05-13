-module(qilr_analyzer).

-behaviour(gen_server).

-include("analysis_pb.hrl").

-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, analyze/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {socket,
                caller}).

analyze(Text) when is_list(Text) ->
    case analyze(list_to_binary(Text)) of
        {ok, Tokens} ->
            {ok, [binary_to_list(Token) || Token <- Tokens]};
        Error ->
            Error
    end;
analyze(Text) when is_binary(Text) ->
    {ok, Pid} = qilr_analyzer_sup:new_analyzer(),
    gen_server:call(Pid, {analyze, Text}).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    case application:get_env(analysis_port) of
        {ok, Port} when is_integer(Port) ->
            {ok, #state{}};
        _ ->
            {stop, {error, bad_analysis_port}}
    end.

handle_call({analyze, Text}, From, State) ->
    Req = #analysisrequest{text=Text},
    {ok, Port} = application:get_env(analysis_port),
    case service_connect(Port) of
        {ok, Sock} ->
            gen_tcp:send(Sock, analysis_pb:encode_analysisrequest(Req)),
            inet:setopts(Sock, [{active, once}]),
            {noreply, State#state{caller=From}};
        Error ->
            error_logger:error_msg("Error connecting to analysis server: ~p", [Error]),
            {reply, error, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Sock, Data}, #state{caller=Caller}=State) ->
    Res = analysis_pb:decode_analysisresult(Data),
    gen_tcp:close(Sock),
    gen_server:reply(Caller, {ok, parse_results(Res#analysisresult.token)}),
    {stop, normal, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
service_connect(Port) ->
    gen_tcp:connect("127.0.0.1", Port, [binary, {active, once},
                                        {packet, 4},
                                        {nodelay, true}], 250).

parse_results([0]) ->
    [];
parse_results(Results) ->
    F = fun(C, {Curr, Acc}) ->
                if
                    C == 0 ->
                        case Curr of
                            [] ->
                                {Curr, Acc};
                            _ ->
                                {[], [Curr|Acc]}
                        end;
                    true ->
                        {[C|Curr], Acc}
                end end,
    {First, Rest} = lists:foldr(F, {[], []}, Results),
    case Rest of
        [] ->
            case First of
                [] ->
                    [];
                _ ->
                    [First]
            end;
        _ ->
            [First|Rest]
    end.
