%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_phase).

-behaviour(luke_phase).

-include("riak_search.hrl").

-export([init/1, handle_input/3, handle_input_done/1, handle_event/2,
         handle_info/2, handle_timeout/1, terminate/2]).

-record(state, {done=false, qterm, searchref, index, client}).

init([QTerm]) ->
    io:format("QTerm: ~p~n", [QTerm]),
    {ok, #state{qterm=QTerm}}.

handle_input([{{Index0, DefaultField0}, Query0}], State, _Timeout) ->
    Index = binary_to_list(Index0),
    DefaultField = binary_to_list(DefaultField0),
    Query = binary_to_list(Query0),
    {ok, Client} = riak_search:local_client(),
    SRef = Client:stream_search(Index, DefaultField, Query),
    {no_output, State#state{index=Index, searchref=SRef, client=Client}}.

handle_input_done(#state{done=true}=State) ->
    {no_output, State};

handle_input_done(State) ->
    {no_output, State}.

handle_event(_Event, State) ->
    {no_output, State}.

handle_info({results, Results, Id}, #state{searchref=#riak_search_ref{id=Id},
                                           index=Index, client=Client,
                                           qterm=QTerm}=State) ->
    Docs = [Client:get_document(Index, DocId) || {DocId, _} <- Results],
    Output = execute_functions(QTerm, Docs, []),
    {output, Output, State};
handle_info({disconnect, Id}, #state{searchref= #riak_search_ref{id=Id,
                                                                inputcount=InputCount}=SRef}=State) ->
    NewInputCount = InputCount - 1,
    case NewInputCount of
        0 ->
            luke_phase:complete(),
            {no_output, State};
        _ ->
            NewState = State#state{searchref=SRef#riak_search_ref{inputcount=NewInputCount - 1}},
            {no_output, NewState}
    end;
handle_info(_Info, State) ->
    {no_output, State}.

handle_timeout(State) ->
    {no_output, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions
execute_functions(_QTerm, [], Acc) ->
    Acc;
execute_functions(QTerm, [Doc|T], Acc) ->
    execute_functions(QTerm, T, Acc ++ execute_function(QTerm, Doc)).
execute_function({javascript, {search, FunRef, Arg, _Acc}}, Doc) ->
    {ok, R} = riak_kv_js_manager:blocking_dispatch({FunRef, iolist_to_binary(riak_indexed_doc:to_json(Doc)),
                                                    Arg}),
    R.
