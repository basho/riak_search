%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(merge_index_backend).
-behavior(riak_search_backend).

-export([
    start/2,
    stop/1,
    index/2,
    delete/2,
    stream/6,
    range/8,
    info/5,
    fold/3,
    is_empty/1,
    drop/1
]).

-include_lib("riak_search/include/riak_search.hrl").

% @type state() = term().
-record(state, {partition, pid}).

%% @spec start(Partition :: integer(), Config :: proplist()) ->
%%          {ok, state()} | {{error, Reason :: term()}, state()}
%% @doc Start this backend.
start(Partition, _Config) ->
    PartitionStr = lists:flatten(io_lib:format("~p", [Partition])),
    {ok, Root} = application:get_env(merge_index, data_root),
    PartitionRoot = filename:join([Root, PartitionStr]),
    {ok, Pid} = merge_index:start_link(PartitionRoot),
    {ok, #state { partition=Partition, pid=Pid }}.

%% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(State) ->
    Pid = State#state.pid,
    ok = merge_index:stop(Pid).

index(IFTVPKList, State) ->
    Pid = State#state.pid,
    merge_index:index(Pid, IFTVPKList),
    {reply, {indexed, node()}, State}.

delete(IFTVPKList, State) ->
    Pid = State#state.pid,
    %% Merge_index deletes a posting when you send it into the system
    %% with properties set to 'undefined'.
    F = fun ({I,F,T,V,_,K}) -> {I,F,T,V,undefined,K};
            ({I,F,T,V,K}) -> {I,F,T,V,undefined,K}
        end,
    IFTVPKList1 = [F(X) || X <- IFTVPKList],
    merge_index:index(Pid, IFTVPKList1),
    {reply, {deleted, node()}, State}.

info(Index, Field, Term, Sender, State) ->
    Pid = State#state.pid,
    {ok, Info} = merge_index:info(Pid, Index, Field, Term),
    Info1 = [{Term, node(), Count} || {_, Count} <- Info],
    riak_search_backend:info_response(Sender, Info1),
    noreply.

stream(Index, Field, Term, FilterFun, Sender, State) ->
    Pid = State#state.pid,
    OutputRef = make_ref(),
    OutputPid = spawn_link(fun() -> result_loop(OutputRef, Sender) end),
    merge_index:stream(Pid, Index, Field, Term, OutputPid, OutputRef, FilterFun),
    noreply.

range(Index, Field, StartTerm, EndTerm, Size, FilterFun, Sender, State) ->
    Pid = State#state.pid,
    OutputRef = make_ref(),
    OutputPid = spawn_link(fun() -> result_loop(OutputRef, Sender) end),
    merge_index:range(Pid, Index, Field, StartTerm, EndTerm, Size, OutputPid, OutputRef, FilterFun),
    noreply.

result_loop(Ref, Sender) ->
    receive
        {result, {DocID, Props}, Ref} ->
            riak_search_backend:response_results(Sender, [{DocID, Props}]),
            result_loop(Ref, Sender);
        {result_vec, ResultVec, Ref} ->
            riak_search_backend:response_results(Sender, ResultVec),
            result_loop(Ref, Sender);
        {result, '$end_of_table', Ref} ->
            riak_search_backend:response_done(Sender);
        Other ->
            ?PRINT({unexpected_result, Other}),
            result_loop(Ref, Sender)
    end.

is_empty(State) ->
    Pid = State#state.pid,
    merge_index:is_empty(Pid).

fold(FoldFun, Acc, State) ->
    %% Copied almost verbatim from riak_search_ets_backend.
    {ok, FoldBatchSize} = application:get_env(merge_index, fold_batch_size),
    Fun = fun
        (I,F,T,V,P,K, {OuterAcc, {FoldKey = {I,{F,T}}, VPKList}, Count}) ->
            %% same IFT. If we have reached the fold_batch_size, then
            %% call FoldFun/3 on the batch and start the next
            %% batch. Otherwise, accumulate.
            case Count >= FoldBatchSize of
                true ->
                    NewOuterAcc = FoldFun(FoldKey, VPKList, OuterAcc),
                    {NewOuterAcc, {FoldKey, [{V,P,K}]}, 1};
                false ->
                    {OuterAcc, {FoldKey, [{V,P,K}|VPKList]}, Count + 1}
            end;
        (I,F,T,V,P,K, {OuterAcc, {FoldKey, VPKList}, _Count}) ->
            %% finished a string of IFT, send it off
            %% (sorted order is assumed)
            NewOuterAcc = FoldFun(FoldKey, VPKList, OuterAcc),
            {NewOuterAcc, {{I,{F,T}},[{V,P,K}]}, 1};
        (I,F,T,V,P,K, {OuterAcc, undefined, _Count}) ->
            %% first round through the fold - just start building
            {OuterAcc, {{I,{F,T}},[{V,P,K}]}, 1}
        end,
    Pid = State#state.pid,
    {ok, {OuterAcc0, Final, _Count}} = merge_index:fold(Pid, Fun, {Acc, undefined, 0}),
    OuterAcc = case Final of
        {FoldKey, VPKList} ->
            %% one last IFT to send off
            FoldFun(FoldKey, VPKList, OuterAcc0);
        undefined ->
            %% this partition was empty
            OuterAcc0
    end,
    {reply, OuterAcc, State}.

drop(State) ->
    Pid = State#state.pid,
    merge_index:drop(Pid).
