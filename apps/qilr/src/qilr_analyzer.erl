-module(qilr_analyzer).

-behaviour(gen_server).

-include("analysis_pb.hrl").

%% API
-export([start_link/0, analyze/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {port,
                results=dict:new(),
                connections=dict:new()}).

analyze(Text) when is_list(Text) ->
    case analyze(list_to_binary(Text)) of
        Token when is_binary(Token) ->
            binary_to_list(Token);
        Tokens ->
            Tokens
    end;
analyze(Text) when is_binary(Text) ->
    case whereis(?SERVER) of
        undefined ->
            Text;
        _ ->
            case gen_server:call(?SERVER, {analyze, Text}) of
                [Token] ->
                    Token;
                Tokens ->
                    Tokens
            end
    end.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    case application:get_env(analysis_port) of
        {ok, Port} when is_integer(Port) ->
            {ok, #state{port=Port}};
        _ ->
            {error, bad_analysis_port}
    end.

handle_call({analyze, Text}, From, #state{connections=Cn, port=Port}=State) ->
    Req = #analysisrequest{text=Text},
    case service_connect(Port) of
        {ok, Sock} ->
            gen_tcp:send(Sock, analysis_pb:encode_analysisrequest(Req)),
            inet:setopts(Sock, [{active, once}]),
            {noreply, State#state{connections=dict:store(Sock, From, Cn)}};
        Error ->
            error_logger:error_msg("Error connecting to analysis server: ~p", [Error]),
            {reply, error, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Sock, Data}, #state{connections=Cn,
                                      results=R}=State) ->
    Res = analysis_pb:decode_analysisresult(Data),
    case Res#analysisresult.done of
        0 ->
            inet:setopts(Sock, [{active, once}]),
            {noreply, State#state{results=store_results(Sock, Res, R)}};
        1 ->
            From = dict:fetch(Sock, Cn),
            Cn1 = dict:erase(Sock, Cn),
            {Acc, R1} = final_results(Sock, R),
            gen_tcp:close(Sock),
            Acc1 = lists:reverse([list_to_binary(Res#analysisresult.token)|Acc]),

            gen_server:reply(From, {ok, Acc1}),
            {noreply, State#state{connections=Cn1,
                                  results=R1}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
store_results(Sock, #analysisresult{token=Token}, Results) ->
    case dict:find(Sock, Results) of
        {ok, R} ->
            dict:store(Sock, [list_to_binary(Token)|R], Results);
        error ->
            dict:store(Sock, [list_to_binary(Token)], Results)
    end.

final_results(Sock, Results) ->
    case dict:find(Sock, Results) of
        {ok, R} ->
            {R, dict:erase(Sock, Results)};
        error ->
            {[], Results}
    end.

service_connect(Port) ->
    gen_tcp:connect("127.0.0.1", Port, [binary, {active, once},
                                        {packet, 4}], 250).
