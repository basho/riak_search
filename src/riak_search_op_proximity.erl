%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_proximity).
-export([
         extract_scoring_props/1,
         preplan/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

%%% Conduct a proximity match over the terms. The #proximity operator
%%% expects to be handed a list of #term operators. The #term operator
%%% reads a sorted list of results from the index along with
%%% positional data, plus a "maximum distance" between the terms.
%%% 
%%% The select_fun, when combining results, creates a list of the
%%% positions in order, in other words, if we search for ("see spot
%%% run"~10), then we'd have a list containing three sublists, the
%%% first is the positions for "see", the second is the positions for
%%% "spot", and the third is the positions for "run".
%%%
%%% A final iterator compares the positions to ensure that we find the
%%% terms all within the right distance from eachother. This acts
%%% differently depending on whether we are doing an exact phrase search vs. a proximity search. 
%%%
%%% - For exact phrase search, the terms should be within N words from
%%% eachother, where N is the number of words in the phrase
%%% (accounting for stopwords). The sublists should line up so that
%%% some combination of word positions is M, M+1, M+2, etc.
%%%
%%% - For proximity search, the terms should be within N words from
%%% eachother, where N is specified by the user, ie: ("spot see
%%% run"~5). This works by continually peeling off the smallest value
%%% that we find in a sublist, and then check the min and maximum
%%% value across *all* sublists. If max-min < N then we have a match.

extract_scoring_props(Op) ->
    riak_search_op:extract_scoring_props(Op#proximity.ops).

preplan(Op, State) ->
    ChildOps = riak_search_op:preplan(Op#proximity.ops, State),
    Op#proximity { ops=ChildOps }.

chain_op(Op, OutputPid, OutputRef, State) ->
    %% Create an iterator chain...
    OpList = Op#proximity.ops,
    Iterator1 = riak_search_op_utils:iterator_tree(fun select_fun/2, OpList, State),
    
    %% Wrap the iterator depending on whether this is an exact match
    %% or proximity search.
    case Op#proximity.proximity of
        exact ->
            Iterator2 = make_exact_match_iterator(Iterator1);
        Proximity ->
            Iterator2 = make_proximity_iterator(Iterator1, Proximity)
    end,
            
    %% Spawn up pid to gather and send results...
    F = fun() -> 
                erlang:link(State#search_state.parent),
                riak_search_op_utils:gather_iterator_results(OutputPid, OutputRef, Iterator2()) 
        end,
    erlang:spawn_link(F),

    %% Return.
    {ok, 1}.


%% Given a result iterator, only return results that are an exact
%% match to the provided phrase.
make_exact_match_iterator(Iterator) ->
    fun() -> exact_match_iterator(Iterator()) end.
exact_match_iterator({Term, PositionLists, Iterator}) ->
    case is_exact_match(PositionLists) of
        true ->
            %% It's a match! Return the result...
            NewIterator = fun() -> exact_match_iterator(Iterator()) end,
            {Term, PositionLists, NewIterator};
        false ->
            %% No match, so skip.
            exact_match_iterator(Iterator())
    end;
exact_match_iterator({eof, _}) ->
    {eof, ignore}.

%% Return true if all of the terms exist within Proximity words from
%% eachother.
is_exact_match(Positions) ->
    case get_position_heads(Positions) of
        undefined ->
            %% We've come to the end of a position list. Not an exact match.
            false;
        Heads ->
            %% This position list represents an exact match if it is
            %% sequential, ie: it's in ascending order with no gaps.
            case is_sequential(Heads) of
                true ->
                    true;
                false ->
                    NewPositions = remove_next_term_position(Positions),
                    is_exact_match(NewPositions)
            end
    end.

%% Return true if the provided list of integers is sequential.
is_sequential([X|Rest]) ->
    case Rest of
        [Y|_] when Y =:= X + 1 ->
            is_sequential(Rest);
        [] ->
            true;
        _ ->
            false
    end;
is_sequential([]) ->
   true.

%% Given a result iterator, only return results that are within a
%% certain proximity of eachother.
make_proximity_iterator(Iterator, Proximity) ->
    fun() -> proximity_iterator(Iterator(), Proximity) end.
proximity_iterator({Term, PositionLists, Iterator}, Proximity) ->
    case within_proximity(Proximity, PositionLists) of
        true ->
            %% It's a match! Return the result...
            NewIterator = fun() -> proximity_iterator(Iterator(), Proximity) end,
            {Term, PositionLists, NewIterator};
        false ->
            %% No match, so skip.
            proximity_iterator(Iterator(), Proximity)
    end;
proximity_iterator({eof, _}, _) ->
    {eof, ignore}.

%% Return true if all of the terms exist within Proximity words from
%% eachother.
within_proximity(Proximity, Positions) ->
    case get_position_heads(Positions) of
        undefined ->
            %% We've come to the end of a position list. Not a proximity match.
            false;
        Heads ->
            %% This position list represents a phrase match if the Max
            %% minus the Min value is less than our Proximity target.
            IsInProximity  = abs(lists:max(Heads) - lists:min(Heads)) < Proximity,
            case IsInProximity of
                true ->
                    true;
                false ->
                    NewPositions = remove_next_term_position(Positions),
                    within_proximity(Proximity, NewPositions)
            end
    end.

%% Given a list of term positions, remove a single term position
%% according to a set of rules, and return the new list.
%%
%% The term position to remove is either the smallest duplicated
%% position, or if there are no duplicates, the smallest position
%% overall.
%%
%% This essentially mimics walking through the document from start to
%% finish, continually removing the earliest occurring term from
%% consideration. The functions that call this one then check to see
%% if the new set of positions represent a successfull proximity match
%% or exact phrase match, depending upon the search query.
remove_next_term_position([]) ->
    undefined;
remove_next_term_position(Positions) ->
    case get_position_heads(Positions) of
        undefined ->
            %% Can't calculate position heads. That means one of the
            %% lists is empty, so return undefined.
            undefined;
        List ->
            %% Figure out which value to remove. This is the smallest
            %% duplicated value, or if no values are duplicated, then the
            %% smallest value.
            ToRemove = get_smallest_duplicate_or_not(List),
            remove_next_term_position(ToRemove, lists:reverse(Positions), [])
    end.
remove_next_term_position(ToRemove, [[ToRemove|Ps]|Rest], NextPass) ->
    %% We've found the value to remove, so toss it and don't remove
    %% anything else.
    remove_next_term_position(undefined, [Ps|Rest], NextPass);
remove_next_term_position(_,  [[]|_], _) ->
    %% Reached the end of a position list, no way to continue.
    undefined;
remove_next_term_position(ToRemove, [Ps|Rest], NextPass) ->
    %% Just loop...
    remove_next_term_position(ToRemove, Rest, [Ps|NextPass]);
remove_next_term_position(_ToRemove, [], NextPass) ->
    %% We've processed all the position lists for this pass, so continue.
    NextPass.

get_position_heads(undefined) ->
    undefined;
get_position_heads(List) ->
    get_position_heads(List, []).
get_position_heads([[]|_Rest], _Acc) ->
    undefined;
get_position_heads([[H|_]|Rest], Acc) ->
    get_position_heads(Rest, [H|Acc]);
get_position_heads([], Acc) ->
    lists:reverse(Acc).

%% Given a list of integers, return the smallest integer that is a duplicate.
get_smallest_duplicate_or_not(List) ->
    get_smallest_duplicate_or_not(lists:sort(List), undefined, undefined).
get_smallest_duplicate_or_not([H,H|Rest], SmallestDup, SmallestVal)
  when SmallestDup == undefined orelse H < SmallestDup ->
    %% We found a duplicate, and it's the first duplicate we've seen,
    %% or it's smaller than the previous one we found, so use this as
    %% the new duplicate value.
    get_smallest_duplicate_or_not(Rest, H, SmallestVal);
get_smallest_duplicate_or_not([H|Rest], SmallestDup, SmallestVal) 
  when SmallestVal == undefined orelse H < SmallestVal ->
    %% We found a new smallest value.
    get_smallest_duplicate_or_not(Rest, SmallestDup, H);
get_smallest_duplicate_or_not([_|Rest], SmallestDup, SmallestVal) ->
    %% Next result is not special, ignore it.
    get_smallest_duplicate_or_not(Rest, SmallestDup, SmallestVal);
get_smallest_duplicate_or_not([], SmallestDup, SmallestVal) ->
    %% Nothing left to process. Return either the smallest duplicate,
    %% the smallest value, or undefined.
    if
        SmallestDup /= undefined -> SmallestDup;
        SmallestVal /= undefined -> SmallestVal;
        true -> undefined
    end.

%% Given a pair of iterators, combine into a single iterator returning
%% results of the form {Term, PositionLists, NewIterator}. Apart from
%% the PositionLists, this is very similar to the #intersection
%% operator logic, except it doesn't need to worry about any #negation
%% operators.
select_fun({{Index, DocID, Props}, Op, Iterator}, I2) when is_record(Op, term) ->
    %% Normalize the first iterator result, replacing Op with a list of positions.
    Positions = proplists:get_value(p, Props, []),
    Positions1 = lists:sort(Positions),
    select_fun({{Index, DocID, Props}, [Positions1], Iterator}, I2);
select_fun(I1, {{Index, DocID, Props}, Op, Iterator}) when is_record(Op, term) ->
    %% Normalize the second iterator result, replacing Op with a list of positions.
    Positions = proplists:get_value(p, Props, []),
    Positions1 = lists:sort(Positions),
    select_fun(I1, {{Index, DocID, Props}, [Positions1], Iterator});
select_fun({Term1, Positions1, Iterator1}, {Term2, Positions2, Iterator2}) when ?INDEX_DOCID(Term1) == ?INDEX_DOCID(Term2) ->
    %% If terms are equal, then combine the terms, concatenate the
    %% position list, and return the result.
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, Positions1 ++ Positions2, fun() -> select_fun(Iterator1(), Iterator2()) end};
select_fun({Term1, _, Iterator1}, {Term2, Positions2, Iterator2}) when ?INDEX_DOCID(Term1) < ?INDEX_DOCID(Term2) ->
    %% Terms not equal, so iterate one of them...
    select_fun(Iterator1(), {Term2, Positions2, Iterator2});
select_fun({Term1, Positions1, Iterator1}, {Term2, _, Iterator2}) when ?INDEX_DOCID(Term1) > ?INDEX_DOCID(Term2) ->
    %% Terms not equal, so iterate one of them...
    select_fun({Term1, Positions1, Iterator1}, Iterator2());
select_fun({eof, _}, _) -> 
    %% Hit an eof, no more results...
    {eof, []};
select_fun(_, {eof, _}) -> 
    %% Hit an eof, no more results...
    {eof, []}.


-ifdef(TEST).

remove_next_term_position_1_test() ->
    %% Test when ToRemove == undefined...
    Input =
        [[1,2,3],
         [4,5,6],
         [7,8,9]],
    Expected =
        [[2,3],
         [4,5,6],
         [7,8,9]],
    ?assertEqual(remove_next_term_position(Input), Expected).

remove_next_term_position_2_test() ->
    %% Test when there is a single matching position in middle list...
    Input =
        [[4,5,6],
         [1,2,3],
         [7,8,9]],
    Expected =
        [[4,5,6],
         [2,3],
         [7,8,9]],
    ?assertEqual(remove_next_term_position(Input), Expected).

remove_next_term_position_3_test() ->
    %% Test when there is a single matching position in end list...
    Input =
        [[4,5,6],
         [7,8,9],
         [1,2,3]],
    Expected =
        [[4,5,6],
         [7,8,9],
         [2,3]],
    ?assertEqual(remove_next_term_position(Input), Expected).

remove_next_term_position_4_test() ->
    %% Test when there are multiple matching positions. Should only remove the last one.
    Input =
        [[1,2,3],
         [4,5,6],
         [7,8,9],
         [1,2,3]],
    Expected =
        [[1,2,3],
         [4,5,6],
         [7,8,9],
         [2,3]],
    ?assertEqual(remove_next_term_position(Input), Expected).

remove_next_term_position_5_test() ->
    %% Test when there are multiple matching positions. Should only remove the last one.
    Input =
        [[0],
         [1,2,3],
         [4,5,6],
         [7,8,9],
         [1,2,3]],
    Expected =
        [[0],
         [1,2,3],
         [4,5,6],
         [7,8,9],
         [2,3]],
    ?assertEqual(remove_next_term_position(Input), Expected).

remove_next_term_position_6_test() ->
    %% Test when a list is empty...
    Input =
        [[1,2,3],
         [4,5,6],
         [],
         [1,2,3]],
    Expected =
        undefined,
    ?assertEqual(remove_next_term_position(Input), Expected).

-ifdef(EQC).

is_sequential_prop() ->
    ?FORALL(L, non_empty(list(int())),
            begin
                A = lists:min(L),
                Z = lists:max(L),
                ?assertEqual(L =:= lists:seq(A, Z),
                             is_sequential(L)),
                true
            end).

is_sequential_test() ->
    true = eqc:quickcheck(eqc:numtests(5000, is_sequential_prop())).

-endif. % EQC

-endif.
