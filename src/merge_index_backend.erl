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

-export([stream_worker/6,
         range_worker/8]).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("riak_search.hrl").

-type mi_size() :: all | pos_integer().
-type(iterator() :: fun(() -> {any(), iterator()}
                                  | eof
                                  | {error, Reason::any()})).
-type itr_result() :: {any(), iterator()}
                    | eof
                    | {error, Reason::any()}.

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
    {ok, Count} = merge_index:info(Pid, Index, Field, Term),
    riak_search_backend:info_response(Sender, [{Term, node(), Count}]),
    noreply.

-spec stream(index(), field(), s_term(), fun(), sender(), #state{}) -> noreply.
stream(Index, Field, Term, Filter, Sender, State) ->
    Pid = State#state.pid,
    spawn_link(?MODULE, stream_worker, [Pid, Index, Field,
                                        Term, Filter, Sender]),
    noreply.

range(Index, Field, StartTerm, EndTerm, Size, Filter, Sender, State) ->
    Pid = State#state.pid,
    spawn_link(?MODULE, range_worker, [Pid, Index, Field, StartTerm,
                                       EndTerm, Size, Filter, Sender]),
    noreply.

is_empty(State) ->
    Pid = State#state.pid,
    merge_index:is_empty(Pid).

%% Note: Always async folding
fold(FoldFun, Acc, State) ->
    AsyncFold = async_fold_fun(FoldFun, Acc, State),
    {async, AsyncFold}.

drop(State) ->
    Pid = State#state.pid,
    merge_index:drop(Pid).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

async_fold_fun(FoldFun, Acc, State) ->
    fun() ->
            {ok, BatchSize} = application:get_env(riak_search, fold_batch_size),
            Pid = State#state.pid,
            Fun = batch_fold(FoldFun, BatchSize),
            Filter = fun riak_search_op_term:default_filter/2,

            case iterate_worker(Pid, Filter, Fun, {Acc, undefined, 0}) of
                {error, Reason, PartialAcc} ->
                    lager:error("failed to iterate the index with reason ~p "
                                "and partial acc ~p", [Reason, PartialAcc]),
                    {error, Reason, PartialAcc};
                {OuterAcc0, Final, _Count} ->
                    case Final of
                        {FoldKey, VPKList} ->
                            %% one last IFT to send off
                            FoldFun(FoldKey, VPKList, OuterAcc0);
                        undefined ->
                            %% this partition was empty
                            OuterAcc0
                    end
            end
    end.

batch_fold(FoldFun, FoldBatchSize) ->
    fun
        ({I,F,T,V,K,P}, {OuterAcc, {FoldKey = {I,{F,T}}, VPKList}, Count}) ->
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
        ({I,F,T,V,K,P}, {OuterAcc, {FoldKey, VPKList}, _Count}) ->
            %% finished a string of IFT, send it off
            %% (sorted order is assumed)
            NewOuterAcc = FoldFun(FoldKey, VPKList, OuterAcc),
            {NewOuterAcc, {{I,{F,T}},[{V,P,K}]}, 1};
        ({I,F,T,V,K,P}, {OuterAcc, undefined, _Count}) ->
            %% first round through the fold - just start building
            {OuterAcc, {{I,{F,T}},[{V,P,K}]}, 1}
    end.

-spec stream_worker(pid(), index(), field(), s_term(), fun(), sender()) ->
                           any().
stream_worker(Pid, Index, Field, Term, Filter, Sender) ->
    Iter = merge_index:lookup(Pid, Index, Field, Term, Filter),
    stream_to(Iter(), Sender).

-spec range_worker(pid(), index(), field(), s_term(), s_term(), mi_size(),
                   fun(), sender()) -> any().
range_worker(Pid, Index, Field, StartTerm, EndTerm, Size, Filter, Sender) ->
    Iter = merge_index:range(Pid, Index, Field, StartTerm, EndTerm,
                             Size, Filter),
    stream_to(Iter(), Sender).

-spec stream_to(itr_result(), sender()) -> any().
stream_to(eof, To) ->
    riak_search_backend:response_done(To);
stream_to({error, Reason}, To) ->
    riak_search_backend:response_error(To, Reason);
stream_to({Results, Iter}, To) ->
    riak_search_backend:response_results(To, Results),
    stream_to(Iter(), To).

-spec iterate_worker(pid(), fun(), fun(), term()) -> term().
iterate_worker(Pid, Filter, Fun, Acc) ->
    Iter = merge_index:iterator(Pid, Filter),
    iterate_worker(Iter(), Fun, Acc).

iterate_worker(eof, _, Acc) ->
    Acc;
iterate_worker({error, Reason}, _Fun, Acc) ->
    {error, Reason, Acc};
iterate_worker({Results, Iter}, Fun, Acc) ->
    Acc2 = lists:foldl(Fun, Acc, Results),
    iterate_worker(Iter(), Fun, Acc2).
