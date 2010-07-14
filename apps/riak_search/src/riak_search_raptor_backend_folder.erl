%%%-------------------------------------------------------------------
%%% @author Jon Meredith <jmeredith@jons-macpro.local>
%%% @copyright (C) 2010, Jon Meredith
%%% @doc
%%%
%%% @end
%%% Created : 14 Jul 2010 by Jon Meredith <jmeredith@jons-macpro.local>
%%%-------------------------------------------------------------------
-module(riak_search_raptor_backend_folder).

-behaviour(gen_server).

%% API
-export([start_link/0, fold/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("raptor/include/raptor.hrl").

-record(state, {cat_conn,
                cat_ref,
                str_conn,
                str_ref,
                part_bin,
                reply_to,
                folder,
                acc,
                vpk_list=[],
                fold_key,
                pending=[]}).

-define(TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

fold(Pid, Partition, Folder, Acc) ->
    gen_server:call(Pid, {fold, Partition, Folder, Acc}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true), % make sure terminate fires
    {ok, CatConn} = riak_sock_pool:checkout(?CONN_POOL),
    {ok, StreamConn} = riak_sock_pool:checkout(?CONN_POOL),
    {ok, #state{cat_conn = CatConn, str_conn = StreamConn}}.

handle_call({fold, Partition, Folder, Acc}, From, State) ->
    PartStr = integer_to_list(Partition),
    {ok, CatRef} = raptor_conn:catalog_query(
                     State#state.cat_conn,
                     ["partition_id:\"", PartStr , "\""]),
    {noreply, State#state{reply_to = From,
                          part_bin = riak_search_utils:to_binary(PartStr),
                          folder = Folder,
                          acc = Acc,
                          cat_ref = CatRef}, ?TIMEOUT}.
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({catalog_query, _ReqId, "$end_of_results", _, _, _, _}, State) ->
    maybe_checkin(State#state.cat_conn), % return to pool, no longer needed
    NewState = State#state{cat_conn = undefined, cat_ref = undefined},
    case fold_complete(NewState) of
        true ->
            {stop, normal, NewState};
        false ->
            {noreply, NewState, ?TIMEOUT}
    end;
handle_info({catalog_query, CatRef, _Partition, Index, Field, Term, _JSONProps},
            #state{cat_ref = CatRef} = State) ->
    I = riak_search_utils:to_binary(Index),
    F = riak_search_utils:to_binary(Field),
    T = riak_search_utils:to_binary(Term),
    case State#state.str_ref of 
        undefined -> 
            NewState = start_stream({I,F,T}, State);
        _ ->
            NewState = State#state{pending = [{I, F, T} | 
                                              State#state.pending]}
    end,
    {noreply, NewState, ?TIMEOUT};

handle_info({stream, StreamRef, timeout},
            #state{str_ref = StreamRef} = State) ->
    gen_server:reply(State#state.reply_to, {error, stream_socket_timeout}),
    {stop, stream_socket_timeout, State};

handle_info({stream, StreamRef, "$end_of_table", _, _}, 
            #state{str_ref = StreamRef, folder = Folder, acc = Acc,
                   fold_key = FoldKey, vpk_list = VPKList} = State) ->
    %% End of stream, call the folder function
    NewAcc = Folder(FoldKey, VPKList, Acc),

    case State#state.pending of
        [] ->
            NewState = State#state{str_ref = undefined,
                                   fold_key = undefined,
                                   vpk_list = [],
                                   acc = NewAcc},
            case fold_complete(NewState) of
                true ->
                    {stop, normal, NewState};
                _ ->
                    {noreply, NewState, ?TIMEOUT}
            end;
        [IFT | Rest] ->
            {noreply, start_stream(IFT, State#state{pending = Rest, 
                                                    str_ref = undefined,
                                                    fold_key = undefined,
                                                    vpk_list = [],
                                                    acc = NewAcc}), 
             ?TIMEOUT}
    end;

handle_info({stream, StreamRef, Value, PropsBin, KeyClock}, 
            #state{str_ref = StreamRef, vpk_list = VPKList} = State) ->
    case PropsBin of
        <<"">> ->
            Props = [];
        _ ->
            Props = binary_to_term(PropsBin)
    end,
    {noreply, State#state{vpk_list = [{Value, Props, KeyClock} | VPKList]}, ?TIMEOUT}.

terminate(_Reason, State) ->
    maybe_checkin(State#state.cat_conn),
    maybe_checkin(State#state.str_conn),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_stream({I,F,T}, State) ->
    {ok, StreamRef} = raptor_conn:stream(
                        State#state.str_conn,
                        I,F,T,
                        State#state.part_bin),
    FoldKey = {I,{F,T}},
    State#state{str_ref = StreamRef, fold_key = FoldKey}.

fold_complete(State) ->
    case (State#state.cat_ref =:= undefined andalso
          State#state.str_ref =:= undefined andalso
          State#state.pending =:= []) of
        true ->
            gen_server:reply(State#state.reply_to, {ok, State#state.acc}),
            true;
        false ->
            false
    end.
 
maybe_checkin(undefined) ->
    ok;
maybe_checkin(Conn) ->
    riak_sock_pool:checkin(?CONN_POOL, Conn).
