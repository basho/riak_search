%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_intersection).
-export([
         extract_scoring_props/1,
         preplan/2,
         chain_op/4,
         make_filter_iterator/1
        ]).
-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

extract_scoring_props(Op) ->
    riak_search_op:extract_scoring_props(Op#intersection.ops).

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
    %% Create an iterator chain...
    OpList = Op#intersection.ops,
    Iterator1 = riak_search_op_utils:iterator_tree(fun select_fun/2, OpList, State),
    Iterator2 = make_filter_iterator(Iterator1),

    %% Spawn up pid to gather and send results...
    F = fun() ->
                erlang:link(State#search_state.parent),
                riak_search_op_utils:gather_iterator_results(OutputPid, OutputRef, Iterator2())
        end,
    erlang:spawn_link(F),

    %% Return.
    {ok, 1}.

%% Given an iterator, return a new iterator that filters out any
%% negated results.
make_filter_iterator(Iterator) ->
    fun() -> filter_iterator(Iterator()) end.
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



%% Handle 'AND' cases, notflags = [false, false]
select_fun({Term1, false, Iterator1}, {Term2, false, Iterator2}) when ?INDEX_DOCID(Term1) == ?INDEX_DOCID(Term2) ->
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, false, fun() -> select_fun(Iterator1(), Iterator2()) end};

select_fun({Term1, false, Iterator1}, {Term2, false, Iterator2}) when ?INDEX_DOCID(Term1) < ?INDEX_DOCID(Term2) ->
    select_fun(Iterator1(), {Term2, false, Iterator2});

select_fun({Term1, false, Iterator1}, {Term2, false, Iterator2}) when ?INDEX_DOCID(Term1) > ?INDEX_DOCID(Term2) ->
    select_fun({Term1, false, Iterator1}, Iterator2());

select_fun({eof, false}, {_Term2, false, Iterator2}) ->
    exhaust_it(Iterator2()),
    {eof, false};

select_fun({_Term1, false, Iterator1}, {eof, false}) ->
    exhaust_it(Iterator1()),
    {eof, false};

%% Handle 'AND' cases, notflags = [false, true]
select_fun({Term1, false, Iterator1}, {Term2, true, Iterator2}) when ?INDEX_DOCID(Term1) == ?INDEX_DOCID(Term2) ->
    select_fun(Iterator1(), {Term2, true, Iterator2});

select_fun({Term1, false, Iterator1}, {Term2, true, Iterator2}) when ?INDEX_DOCID(Term1) < ?INDEX_DOCID(Term2) ->
    {Term1, false, fun() -> select_fun(Iterator1(), {Term2, true, Iterator2}) end};

select_fun({Term1, false, Iterator1}, {Term2, true, Iterator2}) when ?INDEX_DOCID(Term1) > ?INDEX_DOCID(Term2) ->
    select_fun({Term1, false, Iterator1}, Iterator2());

select_fun({eof, false}, {_Term2, true, Iterator2}) ->
    exhaust_it(Iterator2()),
    {eof, false};

select_fun({Term1, false, Iterator1}, {eof, true}) ->
    {Term1, false, fun() -> select_fun(Iterator1(), {eof, true}) end};


%% Handle 'AND' cases, notflags = [true, false], use previous clauses...
select_fun({Term1, true, Iterator1}, {Term2, false, Iterator2}) ->
    select_fun({Term2, false, Iterator2}, {Term1, true, Iterator1});

select_fun({eof, true}, {Term2, false, Iterator2}) ->
    select_fun({Term2, false, Iterator2}, {eof, true});

select_fun({Term1, true, Iterator1}, {eof, false}) ->
    select_fun({eof, false}, {Term1, true, Iterator1});


%% Handle 'AND' cases, notflags = [true, true]
select_fun({Term1, true, Iterator1}, {Term2, true, Iterator2}) when ?INDEX_DOCID(Term1) == ?INDEX_DOCID(Term2) ->
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, true, fun() -> select_fun(Iterator1(), Iterator2()) end};

select_fun({Term1, true, Iterator1}, {Term2, true, Iterator2}) when ?INDEX_DOCID(Term1) < ?INDEX_DOCID(Term2) ->
    {Term1, true, fun() -> select_fun(Iterator1(), {Term2, true, Iterator2}) end};

select_fun({Term1, true, Iterator1}, {Term2, true, Iterator2}) when ?INDEX_DOCID(Term1) > ?INDEX_DOCID(Term2) ->
    {Term2, true, fun() -> select_fun({Term1, true, Iterator1}, Iterator2()) end};

select_fun({eof, true}, {Term2, true, Iterator2}) ->
    {Term2, true, fun() -> select_fun({eof, true}, Iterator2()) end};

select_fun({Term1, true, Iterator1}, {eof, true}) ->
    {Term1, true, fun() -> select_fun(Iterator1(), {eof, true}) end};

%% HANDLE 'AND' case, end of results.
select_fun({eof, true}, {eof, true}) ->
    {eof, true};

select_fun({eof, _}, {eof, _}) ->
    {eof, false};

%% Error on any unhandled cases...
select_fun(Iterator1, Iterator2) ->
    ?PRINT({select_fun, unhandled_case, 'intersection', Iterator1, Iterator2}),
    throw({select_fun, unhandled_case, 'intersection', Iterator1, Iterator2}).

%% @private
%%
%% @doc Exhaust the iterator of all results.
exhaust_it({eof, _}) ->
    ok;
exhaust_it({_,_,It}) ->
    exhaust_it(It()).
