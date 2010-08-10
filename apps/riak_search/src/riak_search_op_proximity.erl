%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_proximity).
-export([
         preplan_op/2,
         chain_op/4
        ]).
-include("riak_search.hrl").
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

preplan_op(Op, F) ->
    Op#proximity { ops=F(Op#proximity.ops) }.


chain_op(Op, OutputPid, OutputRef, QueryProps) ->
    %% Create an iterator chain...
    OpList = Op#proximity.ops,
    Iterator = riak_search_utils:iterator_chain(fun select_fun/2, OpList, QueryProps),

    %% Spawn up pid to gather and send results...
    Proximity = Op#proximity.proximity,
    F = fun() -> gather_results(Proximity, OutputPid, OutputRef, Iterator()) end,
    spawn_link(F),

    %% Return.
    {ok, 1}.


%% If we are here, there was only one proximity term, so just send it
%% through.
gather_results(Proximity, OutputPid, OutputRef, {Term, Op, Iterator}) when is_record(Op, term) ->
    OutputPid!{results, [Term], OutputRef},
    gather_results(Proximity, OutputPid, OutputRef, Iterator());

%% Positions holds a list of term positions for this value. Check to
%% see if there is any combination where all the terms are within
%% Proximity words from eachother.
gather_results(Proximity, OutputPid, OutputRef, {Term, Positions, Iterator}) when is_list(Positions) ->
    case within_proximity(Proximity, Positions) of
        true ->
            OutputPid!{results, [Term], OutputRef};
        false ->
            skip
    end,
    gather_results(Proximity, OutputPid, OutputRef, Iterator());

%% Nothing more to send...
gather_results(_, OutputPid, OutputRef, {eof, _}) ->
    OutputPid!{disconnect, OutputRef}.

%% Return true if all of the terms exist within Proximity words from
%% eachother.
within_proximity(Proximity, Positions) ->
    within_proximity_1(Proximity, undefined, Positions).
within_proximity_1(Proximity, LastMin, Positions) ->
    case get_min_max(LastMin, undefined, undefined, Positions, []) of
        undefined -> 
            false;
        {Min, Max, _NextPass} when abs(Min - Max) < Proximity ->
            true;
        {Min, _, NextPass} ->
            within_proximity_1(Proximity, Min, NextPass)
    end.

%% If this is the same Min we saw last time, then toss it.
get_min_max(LastMin, Min, Max, [[LastMin|Ps]|Rest], NextPass) ->
    get_min_max(LastMin, Min, Max, [Ps|Rest], NextPass);

%% First time through the loop. Set Min and Max.
get_min_max(LastMin, undefined, undefined, [[P|Ps]|Rest], NextPass) ->
    get_min_max(LastMin, P, P, Rest, [[P|Ps]|NextPass]);

%% Update Min...
get_min_max(LastMin, Min, Max, [[P|Ps]|Rest], NextPass) when Min > P ->
    get_min_max(LastMin, P, Max, Rest, [[P|Ps]|NextPass]);

%% Update Max...
get_min_max(LastMin, Min, Max, [[P|Ps]|Rest], NextPass) when Max < P ->
    get_min_max(LastMin, Min, P, Rest, [[P|Ps]|NextPass]);

%% Just loop...
get_min_max(LastMin, Min, Max, [[P|Ps]|Rest], NextPass) ->
    get_min_max(LastMin, Min, Max, Rest, [[P|Ps]|NextPass]);

%% Reached the end of a position list, no way to continue.
get_min_max(_, _, _, [[]|_], _) ->
    undefined;

%% End of list, return.
get_min_max(_LastMin, Min, Max, [], NextPass) ->
    {Min, Max, NextPass}.

    
%% Normalize, throw away the operators, replace with a list of lists of positions.
select_fun({{Index, DocID, Props}, Op, Iterator}, I2) when is_record(Op, term) ->
    Positions = proplists:get_value(word_pos, Props, []),
    Positions1 = lists:sort(Positions),
    select_fun({{Index, DocID, Props}, [Positions1], Iterator}, I2);

select_fun(I1, {{Index, DocID, Props}, Op, Iterator}) when is_record(Op, term) ->
    Positions = proplists:get_value(word_pos, Props, []),
    Positions1 = lists:sort(Positions),
    select_fun(I1, {{Index, DocID, Props}, [Positions1], Iterator});


%% If terms are equal, then bubble up the result...
select_fun({Term1, Positions1, Iterator1}, {Term2, Positions2, Iterator2}) when ?INDEX_DOCID(Term1) == ?INDEX_DOCID(Term2) ->
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, Positions1 ++ Positions2, fun() -> select_fun(Iterator1(), Iterator2()) end};

%% Terms not equal, so iterate one of them...
select_fun({Term1, _, Iterator1}, {Term2, Positions2, Iterator2}) when ?INDEX_DOCID(Term1) < ?INDEX_DOCID(Term2) ->
    select_fun(Iterator1(), {Term2, Positions2, Iterator2});

select_fun({Term1, Positions1, Iterator1}, {Term2, _, Iterator2}) when ?INDEX_DOCID(Term1) > ?INDEX_DOCID(Term2) ->
    select_fun({Term1, Positions1, Iterator1}, Iterator2());

%% Hit an eof, no more results...
select_fun({eof, _}, _) -> 
    {eof, []};

select_fun(_, {eof, _}) -> 
    {eof, []}.

