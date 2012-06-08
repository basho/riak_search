%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_range_sized).
-export([
         extract_scoring_props/1,
         preplan/2,
         chain_op/4,
         correct_term_order/2
        ]).

-import(riak_search_utils, [to_binary/1]).
-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

extract_scoring_props(_Op) ->
    %% Don't alter the scoring props, return addition/multiplication
    %% identities.
    {0,1}.

preplan(Op, _State) ->
    Op.

chain_op(Op, OutputPid, OutputRef, State) ->
    F = fun() ->
                erlang:link(State#search_state.parent),
                start_loop(Op, OutputPid, OutputRef, State)
        end,
    erlang:spawn_link(F),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, State) ->
    %% Figure out how many extra nodes to add to make the groups even.
    IndexName = State#search_state.index,
    {ok, Schema} = riak_search_config:get_schema(IndexName),
    NVal = Schema:n_val(),
    {ok, Preflist} = riak_search_ring_utils:get_covering_preflist(NVal),

    %% Create a #range_worker for each entry in the preflist...
    {From, To} = correct_term_order(Op#range_sized.from, Op#range_sized.to),
    Size = Op#range_sized.size,
    RangeWorkerOp = #range_worker { from=From, to=To, size=Size },
    OpList = [RangeWorkerOp#range_worker { vnode=VNode } || VNode <- Preflist],

    %% Create the iterator...
    SelectFun = fun(I1, I2) -> select_fun(I1, I2) end,
    Iterator1 = riak_search_op_utils:iterator_tree(SelectFun, OpList, State),
    Iterator2 = make_dedup_iterator(Iterator1),

    %% Spawn up pid to gather and send results...
    F = fun() ->
                erlang:link(State#search_state.parent),
                riak_search_op_utils:gather_iterator_results(OutputPid, OutputRef, Iterator2())
        end,
    erlang:spawn_link(F),

    %% Return.
    {ok, 1}.


%% Given two range boundaries, make sure to return the smaller one
%% first.
correct_term_order(From = {_, FromTerm}, To = {_, ToTerm}) ->
    case FromTerm =< ToTerm of
        true  -> {From, To};
        false -> {To, From}
    end.

%% Given an iterator, return a new iterator that removes any
%% duplicates.
make_dedup_iterator(Iterator) ->
    fun() -> dedup_iterator(Iterator(), undefined) end.
dedup_iterator({Term, _, Iterator}, LastTerm) when ?INDEX_DOCID(Term) /= ?INDEX_DOCID(LastTerm) ->
    %% Term is different from last term, so return the iterator.
    {Term, ignore, fun() -> dedup_iterator(Iterator(), Term) end};
dedup_iterator({Term, _, Iterator}, undefined) ->
    %% We don't yet have a last term, so return the iterator.
    {Term, ignore, fun() -> dedup_iterator(Iterator(), Term) end};
dedup_iterator({Term, _, Iterator}, LastTerm) when ?INDEX_DOCID(Term) == ?INDEX_DOCID(LastTerm) ->
    %% Term is same as last term, so skip it.
    dedup_iterator(Iterator(), LastTerm);
dedup_iterator({eof, _}, _) ->
    %% No more results.
    {eof, ignore}.

%% This is very similar to logic in riak_search_op_intersection.erl, but
%% simplified for speed. Returns the smaller of the two iterators,
%% plus a new iterator function.
select_fun({Term1, _, Iterator1}, {Term2, _, Iterator2}) when ?INDEX_DOCID(Term1) < ?INDEX_DOCID(Term2) ->
    {Term1, ignore, fun() -> select_fun(Iterator1(), {Term2, ignore, Iterator2}) end};
select_fun({Term1, _, Iterator1}, {Term2, _, Iterator2}) when ?INDEX_DOCID(Term1) > ?INDEX_DOCID(Term2) ->
    {Term2, ignore, fun() -> select_fun({Term1, ignore, Iterator1}, Iterator2()) end};
select_fun({Term1, _, Iterator1}, {Term2, _, Iterator2}) when ?INDEX_DOCID(Term1) == ?INDEX_DOCID(Term2) ->
    {Term1, ignore, fun() -> select_fun(Iterator1(), Iterator2()) end};
select_fun({Term, _, Iterator}, {eof, _}) ->
    {Term, ignore, Iterator};
select_fun({eof, _}, {Term, _, Iterator}) ->
    {Term, ignore, Iterator};
select_fun({eof, _}, {eof, _}) ->
    {eof, ignore}.
