%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_merge_index).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { pid }).
-define(INDEX, <<"index">>).
-define(FIELD, <<"field">>).
-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Get reference to local merge_index.
    Root = "../data_" ++ integer_to_list(Id),
    SyncInterval = basho_bench_config:get(merge_index_sync_interval, 30 * 1000),
    RolloverSize = basho_bench_config:get(merge_index_rollover_size, 50 * 1024 * 1024),
    Options = [{merge_index_sync_interval, SyncInterval}, {merge_index_rollover_size, RolloverSize}],
    {ok, Pid} = merge_index:start_link(Root, Options),
    {ok, #state { pid=Pid }}.

now_to_timestamp({Mega, Sec, Micro}) ->
    <<TS:64/integer>> = <<Mega:16/integer, Sec:24/integer, Micro:24/integer>>,
    TS.

run(index, KeyGen, ValueGen, State) ->
    #state { pid=Pid } = State,
    TS = now_to_timestamp(now()),
    merge_index:index(Pid, ?INDEX, ?FIELD, KeyGen(), ValueGen(), [], TS),
    {ok, State};

run(info, KeyGen, _ValueGen, State) ->
    #state { pid=Pid } = State,
    merge_index:info(Pid, ?INDEX, ?FIELD, KeyGen()),
    {ok, State};

run(stream, KeyGen, _ValueGen, State) ->
    #state { pid=Pid } = State,
    Ref = make_ref(),
    F = fun(_X, _Y) -> true end,
    merge_index:stream(Pid, ?INDEX, ?FIELD, KeyGen(), self(), Ref, F),
    collect_stream(Ref, 0, undefined),
    {ok, State}.

collect_stream(Ref, Count, LastKey) ->
    receive 
        {result, '$end_of_table', Ref} ->
            ok;
        {result, {Key, _Props}, Ref} when (LastKey == undefined orelse LastKey =< Key) ->
            collect_stream(Ref, Count + 1, Key);
        {result, {Key, _Props}, Ref} ->
            throw({key_out_of_order, Key})
    end.
