%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_proximity).
-export([
         preplan/2,
         chain_op/4
        ]).
-include("riak_search.hrl").
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
    is_exact_match_1(undefined, Positions).
is_exact_match_1(LastMin, Positions) ->
    case remove_smallest_position(LastMin, Positions) of
        undefined -> 
            false;
        {Min, Max, NextPass} ->
            case is_exact_match_2(lists:seq(Min, Max), [hd(X) || X <- NextPass]) of
                true -> 
                    true;
                false ->
                    is_exact_match_1(Min, NextPass)
            end
    end.
%% Return true if the two lists match. Broken out because we'll
%% eventually need to handle skipped terms in here.
%% TODO - Handle skipped terms.
is_exact_match_2(L1, L2) ->
    L1 == L2.


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
    within_proximity_1(Proximity, undefined, Positions).
within_proximity_1(Proximity, LastMin, Positions) ->
    case remove_smallest_position(LastMin, Positions) of
        undefined -> 
            false;
        {Min, Max, _NextPass} when abs(Min - Max) < Proximity ->
            true;
        {Min, _, NextPass} ->
            within_proximity_1(Proximity, Min, NextPass)
    end.

%% Given a list of Positions, remove the smallest position in the
%% list, and return the new minimum and maximum plus the new list of
%% Positions.
remove_smallest_position(LastMin, Positions) ->
    remove_smallest_position(LastMin, undefined, undefined, Positions, []).
remove_smallest_position(LastMin, Min, Max, [[LastMin|Ps]|Rest], NextPass) ->
    %% This is the same Min we saw last time, so toss it.
    remove_smallest_position(LastMin, Min, Max, [Ps|Rest], NextPass);
remove_smallest_position(LastMin, undefined, undefined, [[P|Ps]|Rest], NextPass) ->
    %% First time through the loop. Set Min and Max.
    remove_smallest_position(LastMin, P, P, Rest, [[P|Ps]|NextPass]);
remove_smallest_position(LastMin, Min, Max, [[P|Ps]|Rest], NextPass) when Min > P ->
    %% Found a new minimum...
    remove_smallest_position(LastMin, P, Max, Rest, [[P|Ps]|NextPass]);
remove_smallest_position(LastMin, Min, Max, [[P|Ps]|Rest], NextPass) when Max < P ->
    %% Found a new maximum...
    remove_smallest_position(LastMin, Min, P, Rest, [[P|Ps]|NextPass]);
remove_smallest_position(LastMin, Min, Max, [[P|Ps]|Rest], NextPass) ->
    %% Just loop...
    remove_smallest_position(LastMin, Min, Max, Rest, [[P|Ps]|NextPass]);
remove_smallest_position(_, _, _, [[]|_], _) ->
    %% Reached the end of a position list, no way to continue.
    undefined;
remove_smallest_position(_LastMin, Min, Max, [], NextPass) ->
    %% We've processed all the position lists for this pass, so continue.
    {Min, Max, lists:reverse(NextPass)}.

    
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

