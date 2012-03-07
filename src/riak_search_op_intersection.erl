%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_intersection).
-export([
         chain_op/4,
         chain_op/5,
         extract_scoring_props/1,
         frequency/1,
         make_filter_iterator/1,
         preplan/2
        ]).
-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

extract_scoring_props(Op) ->
    riak_search_op:extract_scoring_props(Op#intersection.ops).

frequency(Op) ->
    Freqs = [riak_search_op:frequency(Child) || Child <- Op#intersection.ops],
    Sum = lists:sum([Freq || {Freq, _} <- Freqs, Freq /= unknown]),
    {Sum, Op}.

preplan(Op, State) ->
    case riak_search_op:preplan(Op#intersection.ops, State) of
        [ChildOp] ->
            ChildOp;
        ChildOps ->
            NewOp = Op#intersection { ops=ChildOps },
            NodeOp = #node { ops=NewOp },
            riak_search_op:preplan(NodeOp, State)
    end.

chain_op(Op, OutputPid, OutputRef, State) ->
    %% 1. order the operations by ascending frequency
    Ops = order_ops(fun order_by_freq/2, Op#intersection.ops),

    %% 2. realize the candidate set
    [Op1|Ops2] = Ops,
    CandidateSet = get_candidate_set(Op1, State),

    %% 3. sequentially check candidate set against rest of term
    %% indexes
    FinalSet = lists:foldl(intersection(State), CandidateSet, Ops2),

    %% 4. output results
    send_results(FinalSet, {OutputPid, OutputRef, State#search_state.index}),

    {ok, 1}.

%% Nested intersection query, use incoming candidate set.
chain_op(Op, OutputPid, OutputRef, CandidateSet, State) ->
    %% 1. order the operations by ascending frequency
    Ops = order_ops(fun order_by_freq/2, Op#intersection.ops),

    %% 2. realize the candidate set
    %% [Op1|Ops2] = Ops,
    %% CandidateSet = get_candidate_set(Op1, State),

    %% 3. sequentially check candidate set against rest of term
    %% indexes
    FinalSet = lists:foldl(intersection(State), CandidateSet, Ops),

    %% 4. output results
    send_results(FinalSet, {OutputPid, OutputRef, State#search_state.index}),

    {ok, 1}.


%% Given an iterator, return a new iterator that filters out any
%% negated results.
make_filter_iterator(Iterator) ->
    fun() -> filter_iterator(Iterator()) end.

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @doc Return a `Fun' used to perform the intersection of `Op'
%% results against `CandidateSet'.
-spec intersection(term()) -> function().
intersection(State) ->
    fun(Op, CandidateSet) ->
            case gb_trees:is_empty(CandidateSet) of
                true ->
                    CandidateSet;
                false ->
                    Itr = riak_search_op_utils:iterator_tree(none,
                                                             [Op],
                                                             CandidateSet,
                                                             State),
                    ResultSet = id_set(Itr),
                    case riak_search_op_negation:is_negation(Op) of
                        true ->
                            subtract(CandidateSet,
                                     gb_trees:iterator(ResultSet));
                        false ->
                            merge_props(CandidateSet, ResultSet)
                    end
            end
    end.

-spec merge_props(gb_tree(), gb_tree()) -> gb_tree().
merge_props(CandidateSet, ResultSet) ->
    Empty = gb_trees:is_empty(ResultSet),
    if Empty ->
            ResultSet;
       true ->
            merge_props_2(CandidateSet, gb_trees:iterator(ResultSet))
    end.

-spec merge_props_2(gb_tree(), any()) -> gb_tree().
merge_props_2(CandidateSet, Itr) ->
    case gb_trees:next(Itr) of
        {DocId, PropsRS, Itr2} ->
            PropsCS = gb_trees:get(DocId, CandidateSet),
            {ignore, DocId, NewProps} =
                riak_search_utils:combine_terms({ignore, DocId, PropsCS},
                                                {ignore, DocId, PropsRS}),
            CandidateSet2 = gb_trees:update(DocId, NewProps, CandidateSet),
            merge_props_2(CandidateSet2, Itr2);
        none ->
            CandidateSet
    end.

subtract(CandidateSet, Itr) ->
    case gb_trees:next(Itr) of
        {DocId, _Props, Itr2} ->
            subtract(gb_trees:delete_any(DocId, CandidateSet), Itr2);
        none ->
            CandidateSet
    end.

filter_iterator({_, Op, Iterator})
  when (is_tuple(Op) andalso is_record(Op, negation)) orelse Op == true ->
    %% Term is negated, so skip it.
    filter_iterator(Iterator());
filter_iterator({Term, _, Iterator}) ->
    %% Term is not negated, so keep it.
    {Term, ignore, fun() -> filter_iterator(Iterator()) end};
filter_iterator({eof, _}) ->
    %% No more results.
    {eof, ignore}.

%% @private
%%
%% @doc Realize the results for `Op'.
%%
%% TODO: The usual iterator fuss can be bypassed since these results
%% are to be realized immediately.  Instead call chain_op and directly
%% collect.
%%
-spec get_candidate_set(term(), term()) -> gb_tree().
get_candidate_set(Op, State) ->
    Itr = riak_search_op_utils:iterator_tree(none, [Op], State),
    id_set(Itr).

id_set(Itr) ->
    id_set(Itr(), gb_trees:empty()).

id_set({{_Idx, DocId, Props}, _, Itr}, Set) ->
    id_set(Itr(), gb_trees:insert(DocId, Props, Set));
id_set({eof, _}, Set) ->
    Set.

%% @private
%%
%% @doc Order operations by frequency.
-spec order_by_freq({non_neg_integer(), term()}, {non_neg_integer(), term()}) ->
                           boolean().
order_by_freq({_FreqA, _}, {unknown, _}) ->
    true;
order_by_freq({unknown, _}, {_FreqB, _}) ->
    false;
order_by_freq({FreqA, _}, {FreqB, _}) ->
    FreqA =< FreqB.

%% @private
%%
%% @doc Order the operations in ascending order based on frequency of
%% matched objects.
-spec order_ops(function(), [{non_neg_integer(), term()}]) -> [term()].
order_ops(Fun, Ops) ->
    Asc = lists:sort(Fun, lists:map(fun riak_search_op:frequency/1, Ops)),
    swap_front([Op || {_, Op} <- Asc]).

%% @private
%%
%% @doc Make sure that `Ops' does not start with negation op.
-spec swap_front([term()]) -> [term()].
swap_front(Ops) ->
    {Negations, [H|T]} =
        lists:splitwith(fun riak_search_op_negation:is_negation/1, Ops),
    [H] ++ Negations ++ T.

send_results(FinalSet, O) ->
    send_results(gb_trees:iterator(FinalSet), O, []).

%% TODO: MICROOPT: Explicitly keep track of count to avoid calling
%% `length' each time.
send_results(Itr, {OutputPid, OutputRef, _Idx}=O, Acc)
  when length(Acc) > ?RESULTVEC_SIZE ->
    OutputPid ! {results, lists:reverse(Acc), OutputRef},
    send_results(Itr, O, []);
send_results(Itr, {OutputPid, OutputRef, Idx}=O, Acc) ->
    case gb_trees:next(Itr) of
        {DocId, Props, Itr2} ->
            %% need to add the Idx back for upstream
            send_results(Itr2, O, [{Idx, DocId, Props}|Acc]);
        none ->
            OutputPid ! {results, lists:reverse(Acc), OutputRef},
            OutputPid ! {disconnect, OutputRef}
    end.

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

order_by_freq_test() ->
    Unordered = [{10,op1}, {unknown,op2}, {2,op3}, {unknown,op4},
                 {unknown,op5}, {7,op6}],
    Expect = [op3, op6, op1, op2, op4, op5],
    Asc = [Op || {_, Op} <- lists:sort(fun order_by_freq/2, Unordered)],
    ?assertEqual(Expect, Asc).

-endif.
