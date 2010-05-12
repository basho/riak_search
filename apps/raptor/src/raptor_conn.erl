-module(raptor_conn).

-behaviour(gen_server).

-include("raptor_pb.hrl").

%% API
-export([start_link/0,
         close/1,
         index/8,
         stream/8,
         info/5,
         info_range/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 30000).

-record(state, {sock, caller, req_type, reqid, dest}).

close(ConnPid) ->
    gen_server:call(ConnPid, close_conn).

index(ConnPid, IndexName, FieldName, Term, SubType,
      SubTerm, Value, Partition) ->
    MessageType = <<"Index">>,
    IndexRec = #index{index=IndexName, field=FieldName,
                      term=Term, subtype=SubType,
                      subterm=SubTerm, value=Value,
                      partition=Partition,
                      message_type=MessageType},
    gen_server:call(ConnPid, {index, IndexRec}, ?TIMEOUT).

stream(ConnPid, IndexName, FieldName, Term, SubType, StartSubTerm,
       EndSubTerm, Partition) ->
    MessageType = <<"Stream">>,
    StreamRec = #stream{index=IndexName, field=FieldName,
                        term=Term, subtype=SubType,
                        start_subterm=StartSubTerm,
                        end_subterm=EndSubTerm,
                        partition=Partition,
                        message_type=MessageType},
    Ref = erlang:make_ref(),
    gen_server:call(ConnPid, {stream, self(), Ref, StreamRec}, ?TIMEOUT).

info(ConnPid, IndexName, FieldName, Term, Partition) ->
    MessageType = <<"Info">>,
    InfoRec = #info{index=IndexName, field=FieldName, term=Term,
                    partition=Partition,
                    message_type=MessageType},
    Ref = erlang:make_ref(),
    gen_server:call(ConnPid, {info, self(), Ref, InfoRec}, ?TIMEOUT).

info_range(ConnPid, IndexName, FieldName, StartTerm,
           EndTerm, Partition) ->
    MessageType = <<"InfoRange">>,
    InfoRangeRec = #inforange{index=IndexName, field=FieldName,
                              start_term=StartTerm, end_term=EndTerm,
                              partition=Partition,
                              message_type=MessageType},
    Ref = erlang:make_ref(),
    gen_server:call(ConnPid, {info_range, self(), Ref, InfoRangeRec}, ?TIMEOUT).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    case raptor_util:get_env(raptor, raptor_port, undefined) of
        P when not(is_integer(P)) ->
            {stop, {error, bad_raptor_port, P}};
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

handle_call(_Msg, _From, #state{req_type=ReqType}=State) when ReqType /= undefined ->
    {reply, {error, busy}, State};

handle_call(close_conn, _From, State) ->
    {stop, normal, ok, State};

handle_call({index, IndexRec}, _From, #state{sock=Sock}=State) ->
    Data = raptor_pb:encode_index(IndexRec),
    gen_tcp:send(Sock, Data),
    {reply, ok, State};

handle_call({stream, Caller, ReqId, StreamRec}, _From, #state{sock=Sock}=State) ->
    Data = raptor_pb:encode_stream(StreamRec),
    gen_tcp:send(Sock, Data),
    {reply, {ok, ReqId}, State#state{req_type=stream, reqid=ReqId, dest=Caller}};

handle_call({info, Caller, ReqId, InfoRec}, _From, #state{sock=Sock}=State) ->
    Data = raptor_pb:encode_info(InfoRec),
    gen_tcp:send(Sock, Data),
    {reply, {ok, ReqId}, State#state{req_type=info, reqid=ReqId, dest=Caller}};

handle_call({info_range, Caller, ReqId, InfoRec}, _From, #state{sock=Sock}=State) ->
    Data = raptor_pb:encode_inforange(InfoRec),
    gen_tcp:send(Sock, Data),
    {reply, {ok, ReqId}, State#state{req_type=info, reqid=ReqId, dest=Caller}};


handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Sock, Data}, #state{req_type=stream, reqid=ReqId, dest=Dest}=State) ->
    StreamResponse = raptor_pb:decode_streamresponse(Data),
    Dest ! {stream, ReqId, StreamResponse#streamresponse.value, StreamResponse#streamresponse.props},
    NewState = if
                   StreamResponse#streamresponse.value =:= "$end_of_table" ->
                       State#state{req_type=undefined,
                                   reqid=undefined,
                                   dest=undefined};
                   true ->
                       inet:setopts(Sock, [{active, once}]),
                       State
               end,
    {noreply, NewState};

handle_info({tcp, Sock, Data}, #state{req_type=info, reqid=ReqId, dest=Dest}=State) ->
    InfoResponse = raptor_pb:decode_inforesponse(Data),
    Dest ! {info, ReqId, InfoResponse#inforesponse.term, InfoResponse#inforesponse.count},
    NewState = if
                   InfoResponse#inforesponse.term =:= "$end_of_info" ->
                       State#state{req_type=undefined,
                                   reqid=undefined,
                                   dest=undefined};
                   true ->
                       inet:setopts(Sock, [{active, once}]),
                       State
               end,
    {noreply, NewState};

handle_info({tcp_error, _Sock, Reason}, State) ->
    {stop, Reason, State};
handle_info({tcp_closed, _Sock}, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
raptor_connect(Port) ->
    gen_tcp:connect("127.0.0.1", Port, [binary, {active, once},
                                        {packet, 4},
                                        {keepalive, true},
                                        {nodelay, true}], 250).
