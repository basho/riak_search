%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_union).

-export([
         extract_scoring_props/1,
         preplan/2,
         chain_op/4
        ]).
-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

extract_scoring_props(Op) ->
    riak_search_op:extract_scoring_props(Op#union.ops).

preplan(Op, State) ->
    case riak_search_op:preplan(Op#union.ops, State) of
        [ChildOp] ->
            ChildOp;
        ChildOps ->
            NewOp = Op#union { ops=ChildOps },
            NodeOp = #node { ops=NewOp },
            riak_search_op:preplan(NodeOp, State)
    end.

chain_op(Op, OutputPid, OutputRef, State) ->
    %% Create an iterator chain...
    OpList = Op#union.ops,
    Iterator1 = riak_search_op_utils:iterator_tree(fun select_fun/2, OpList, State),
    Iterator2 = riak_search_op_intersection:make_filter_iterator(Iterator1),

    %% Spawn up pid to gather and send results...
    F = fun() ->
                erlang:link(State#search_state.parent),
                riak_search_op_utils:gather_iterator_results(OutputPid, OutputRef, Iterator2())
        end,
    erlang:spawn_link(F),

    %% Return.
    {ok, 1}.

%% Now, treat the operation as a comparison between two terms. Return
%% a term if it successfully meets our conditions, or else toss it and
%% iterate to the next term.

%% Normalize Op1 and Op2 into boolean NotFlags...
select_fun({Term1, Op1, Iterator1}, I2) when is_tuple(Op1) ->
    select_fun({Term1, is_record(Op1, negation), Iterator1}, I2);

select_fun({eof, Op1}, I2) when is_tuple(Op1) ->
    select_fun({eof, is_record(Op1, negation)}, I2);

select_fun(I1, {Term2, Op2, Iterator2}) when is_tuple(Op2) ->
    select_fun(I1, {Term2, is_record(Op2, negation), Iterator2});

select_fun(I1, {eof, Op2}) when is_tuple(Op2) ->
    select_fun(I1, {eof, is_record(Op2, negation)});

%% Handle 'OR' cases, notflags = [false, false]
select_fun({Term1, false, Iterator1}, {Term2, false, Iterator2}) when ?INDEX_DOCID(Term1) == ?INDEX_DOCID(Term2) ->
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, false, fun() -> select_fun(Iterator1(), Iterator2()) end};

select_fun({Term1, false, Iterator1}, {Term2, false, Iterator2}) when ?INDEX_DOCID(Term1) < ?INDEX_DOCID(Term2) ->
    {Term1, false, fun() -> select_fun(Iterator1(), {Term2, false, Iterator2}) end};

select_fun({Term1, false, Iterator1}, {Term2, false, Iterator2}) when ?INDEX_DOCID(Term1) > ?INDEX_DOCID(Term2) ->
    {Term2, false, fun() -> select_fun({Term1, false, Iterator1}, Iterator2()) end};

select_fun({eof, false}, {Term2, false, Iterator2}) ->
    {Term2, false, fun() -> select_fun({eof, false}, Iterator2()) end};

select_fun({Term1, false, Iterator1}, {eof, false}) ->
    {Term1, false, fun() -> select_fun(Iterator1(), {eof, false}) end};


%% Handle 'OR' cases, notflags = [false, true].
%% Basically, not flags are ignored in an OR.

select_fun({Term1, false, Iterator1}, {_, true, _}) ->
    select_fun({Term1, false, Iterator1}, {eof, false});

select_fun({Term1, false, Iterator1}, {eof, _}) ->
    select_fun({Term1, false, Iterator1}, {eof, false});

select_fun({_, true, _}, {Term2, false, Iterator2}) ->
    select_fun({Term2, false, Iterator2}, {eof, false});

select_fun({eof, _}, {Term2, false, Iterator2}) ->
    select_fun({Term2, false, Iterator2}, {eof, false});


%% Handle 'OR' cases, notflags = [true, true]
select_fun({eof, _}, {_, true, _}) ->
    {eof, false};
select_fun({_, true, _}, {eof, _}) ->
    {eof, false};
select_fun({_, true, _}, {_, true, _}) ->
    {eof, false};

%% HANDLE 'OR' case, end of results.
select_fun({eof, _}, {eof, _}) ->
    {eof, false};

%% Error on any unhandled cases...
select_fun(Iterator1, Iterator2) ->
    ?PRINT({select_fun, unhandled_case, union, Iterator1, Iterator2}),
    throw({select_fun, unhandled_case, union, Iterator1, Iterator2}).
