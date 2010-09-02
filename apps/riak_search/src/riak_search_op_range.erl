%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_range).
-export([
         preplan_op/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
-define(STREAM_TIMEOUT, 15000).
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef, QueryProps) ->
    spawn_link(fun() -> start_loop(Op, OutputPid, OutputRef, QueryProps) end),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, QueryProps) ->
    %% Get the full preflist...
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    VNodes = riak_core_ring:all_owners(Ring),

    %% Pick out the preflist of covering nodes. There are two
    %% approaches in the face of down nodes. One is to minimize the
    %% amount of duplicate data that we read. The other is maximize
    %% load distribution. We take the latter approach, because
    %% otherwise one or two down nodes could cause all range queries
    %% to take the same set of covering nodes. Works like this: rotate
    %% the ring a random amount, then clump the preflist into groups
    %% of size NVal and take the first up node in the list. If
    %% everything goes perfectly, this will be the first node in each
    %% list, and we'll have very little duplication.  If one of the
    %% nodes is down, then we'll take the next node in the list down,
    %% then just take the next vnode in the list.

    %% Figure out how many extra nodes to add to make the groups even.
    NVal = riak_search_utils:n_val(),
    NumExtraNodes = length(VNodes) rem NVal,
    {ExtraNodes, _} = lists:split(NumExtraNodes, VNodes),
    UpNodes = riak_core_node_watcher:nodes(riak_search),
    Preflist = get_range_preflist(NVal, VNodes ++ ExtraNodes, UpNodes),

    %% Create a #range_worker for each entry in the preflist...
    RangeWorkerOp = #range_worker { q=Op#range.q, size=Op#range.size, options=Op#range.options },
    OpList = [RangeWorkerOp#range_worker { vnode=VNode } || VNode <- Preflist],

    %% Create the iterator...
    SelectFun = fun(I1, I2) -> select_fun(I1, I2) end,
    Iterator1 = riak_search_utils:iterator_tree(SelectFun, OpList, QueryProps),
    Iterator2 = make_dedup_iterator(Iterator1),

    %% Spawn up pid to gather and send results...
    F = fun() -> gather_results(OutputPid, OutputRef, Iterator2(), []) end,
    spawn_link(F),

    %% Return.
    {ok, 1}.

get_range_preflist(NVal, VNodes, UpNodes) ->
    %% Create an ordered set for fast repeated checking.
    UpNodesSet = ordsets:from_list(UpNodes),

    %% Randomly rotate the vnodes...
    random:seed(now()),
    RotationFactor = random:uniform(NVal),
    {Pre, Post} = lists:split(RotationFactor, VNodes),
    
    %% Calculate the preflist.
    Preflist1 = get_range_preflist_1(NVal, Post ++ Pre, UpNodesSet),
    Preflist2 = [VNode || VNode <- Preflist1, VNode /= undefined],
    lists:usort(Preflist2).
get_range_preflist_1(_, [], _) ->
    [];
get_range_preflist_1(NVal, VNodes, UpNodesSet) ->
    %% Get the first set of nodes...
    {Group, Rest} = lists:split(NVal, VNodes),

    %% Filter out any down nodes...
    VNode = get_first_up_node(Group, UpNodesSet),
    [VNode|get_range_preflist_1(NVal, Rest, UpNodesSet)].

%% Return the first active vnode in a set of vnodes.
get_first_up_node([], _UpNodesSet) ->
    undefined;
get_first_up_node([{Index, Node}|Rest], UpNodesSet) ->
    IsUp = ordsets:is_element(Node, UpNodesSet),
    case IsUp of
        true -> {Index, Node};
        false -> get_first_up_node(Rest, UpNodesSet)
    end.

gather_results(OutputPid, OutputRef, {Term, Op, Iterator}, Acc)
  when length(Acc) > ?RESULTVEC_SIZE ->
    OutputPid ! {results, lists:reverse(Acc), OutputRef},
    gather_results(OutputPid, OutputRef, {Term, Op, Iterator}, []);

gather_results(OutputPid, OutputRef, {Term, _Op, Iterator}, Acc) ->
    gather_results(OutputPid, OutputRef, Iterator(), [Term|Acc]);

gather_results(OutputPid, OutputRef, {eof, _}, Acc) ->
    OutputPid ! {results, lists:reverse(Acc), OutputRef},
    OutputPid ! {disconnect, OutputRef}.


%% Given an iterator, return a new iterator that removes any
%% duplicates.
make_dedup_iterator(Iterator) ->
    fun() -> dedup_iterator(Iterator(), undefined) end.
dedup_iterator({Term, _, Iterator}, LastTerm) when ?INDEX_DOCID(Term) /= ?INDEX_DOCID(LastTerm) ->
    %% Term is different from last term, so return the iterator.
    {Term, ignore, fun() -> dedup_iterator(Iterator(), Term) end};
dedup_iterator({Term, _, Iterator}, LastTerm) when ?INDEX_DOCID(Term) == ?INDEX_DOCID(LastTerm) ->
    %% Term is same as last term, so skip it.
    dedup_iterator(Iterator(), LastTerm);
dedup_iterator({eof, _}, _) ->
    %% No more results.
    {eof, ignore}.

%% This is very similar to logic in riak_search_op_land.erl, but
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
