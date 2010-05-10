-module(raptor_conn).

-behaviour(gen_server).

%% API
-export([start_link/0,
         index/8,
         stream/8,
         info/5,
         info_range/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {sock, caller}).

index(ConnPid, IndexName, FieldName, Term, SubType,
      SubTerm, Value, Partition) ->
    gen_server:call(ConnPid, {index, IndexName, FieldName, Term,
                              SubType, SubTerm, Value, Partition}).

stream(ConnPid, IndexName, FieldName, Term, SubType, StartSubTerm,
       EndSubTerm, Partition) ->
    gen_server:call(ConnPid, {stream, IndexName, FieldName, Term,
                              SubType, StartSubTerm, EndSubTerm,
                              Partition}).

info(ConnPid, IndexName, FieldName, Term, Partition) ->
    gen_server:call(ConnPid, {info, IndexName, FieldName, Term, Partition}).

info_range(ConnPid, IndexName, FieldName, StartTerm,
           EndTerm, Partition) ->
    gen_server:call(ConnPid, {info_range, IndexName, FieldName, StartTerm,
                              EndTerm, Partition}).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    case app_helper:get_env(raptor, raptor_port, undefined) of
        P when not(is_integer(P)) ->
            {error, bad_raptor_port};
        Port ->
            case raptor_connect(Port) of
                {ok, Sock} ->
                    erlang:link(Sock),
                    {ok, #state{sock=Sock}};
                Error ->
                    error_logger:error_msg("Error connecting to Raptor: ~p~n", [Error]),
                    {stop, raptor_connect_error}
            end
    end.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
raptor_connect(Port) ->
    gen_tcp:connect("127.0.0.1", Port, [binary, {active, once},
                                        {packet, 4}], 250).
