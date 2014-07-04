%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_range_worker).
-export([
         chain_op/4
        ]).

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

chain_op(Op, OutputPid, OutputRef, State) ->
    F = fun() ->
                erlang:link(State#search_state.parent),
                start_loop(Op, OutputPid, OutputRef, State)
        end,
    erlang:spawn_link(F),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, State) ->
    %% Start streaming the results...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,

    %% Create the start term and end term...
    case Op#range_worker.from of
        {inclusive, OldStartTerm} ->
            StartTerm = OldStartTerm;
        {exclusive, OldStartTerm} ->
            StartTerm = riak_search_utils:binary_inc(OldStartTerm, +1)
    end,

    case Op#range_worker.to of
        {inclusive, OldEndTerm} ->
            EndTerm = OldEndTerm;
        {exclusive, OldEndTerm} ->
            EndTerm = riak_search_utils:binary_inc(OldEndTerm, -1)
    end,

    Size = Op#range_worker.size,
    VNode = Op#range_worker.vnode,
    FilterFun = State#search_state.filter,
    TransformFun = fun({DocID, Props}) ->
                           {IndexName, DocID, Props}
                   end,
    {ok, Ref} = range(VNode, IndexName, FieldName, StartTerm, EndTerm, Size, FilterFun),
    Timeout = app_helper:get_env(riak_search, stream_timeout, 15000),
    riak_search_op_utils:gather_stream_results(Ref, OutputPid, OutputRef, TransformFun, Timeout).

range(VNode, Index, Field, StartTerm, EndTerm, Size, FilterFun) ->
    riak_search_vnode:range(VNode, Index, Field, riak_search_utils:to_binary(StartTerm), riak_search_utils:to_binary(EndTerm), Size, FilterFun, self()).
