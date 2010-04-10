-module(riak_search_op_land).
-export([
         preplan_op/2,
         chain_op/3,
         chain_op/4
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#land { ops=F(Op#land.ops) }.

chain_op(Op, OutputPid, OutputRef) ->
    chain_op(Op, OutputPid, OutputRef, 'land').

chain_op(Op, OutputPid, OutputRef, Type) ->
    %% Create an iterator chain...
    OpList = Op#land.ops,
    SelectFun = fun(I1, I2) -> select_fun(Type, I1, I2) end,
    Iterator = riak_search_utils:iterator_chain(SelectFun, OpList),

    %% Spawn up pid to gather and send results...
    F = fun() -> gather_results(OutputPid, OutputRef, Iterator()) end,
    spawn_link(F),

    %% Return.
    {ok, 1}.

gather_results(OutputPid, OutputRef, {Term, NotFlag, Iterator}) ->
    case NotFlag of
        true  -> skip;
        false -> OutputPid!{results, [Term], OutputRef}
    end,
    gather_results(OutputPid, OutputRef, Iterator());
gather_results(OutputPid, OutputRef, {eof, _}) ->
    OutputPid!{disconnect, OutputRef}.

%% Now, treat the operation as a comparison between two terms. Return
%% a term if it successfully meets our conditions, or else toss it and
%% iterate to the next term.

%% Normalize Op1 and Op2 into boolean NotFlags...
select_fun(Type, {Term1, Op1, Iterator1}, I2) when is_tuple(Op1) ->
    select_fun(Type, {Term1, is_record(Op1, lnot), Iterator1}, I2);

select_fun(Type, {eof, Op1}, I2) when is_tuple(Op1) ->
    select_fun(Type, {eof, is_record(Op1, lnot)}, I2);

select_fun(Type, I1, {Term2, Op2, Iterator2}) when is_tuple(Op2) ->
    select_fun(Type, I1, {Term2, is_record(Op2, lnot), Iterator2});

select_fun(Type, I1, {eof, Op2}) when is_tuple(Op2) ->
    select_fun(Type, I1, {eof, is_record(Op2, lnot)});


%% Handle 'AND' cases, notflags = [false, false]
select_fun(land, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when element(1, Term1) == element(1, Term2) ->
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, false, fun() -> select_fun(land, Iterator1(), Iterator2()) end};

select_fun(land, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 < Term2 ->
    select_fun(land, Iterator1(), {Term2, false, Iterator2});

select_fun(land, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 > Term2 ->
    select_fun(land, {Term1, false, Iterator1}, Iterator2());

select_fun(land, {eof, false}, {_Term2, false, _Iterator2}) ->
    {eof, false};

select_fun(land, {_Term1, false, _Iterator1}, {eof, false}) ->
    {eof, false};

%% Handle 'AND' cases, notflags = [false, true]
select_fun(land, {Term1, false, Iterator1}, {Term2, true, Iterator2}) when element(1, Term1) == element(1, Term2) ->
    select_fun(land, Iterator1(), {Term2, true, Iterator2});

select_fun(land, {Term1, false, Iterator1}, {Term2, true, Iterator2}) when Term1 < Term2 ->
    {Term1, false, fun() -> select_fun(land, Iterator1(), {Term2, true, Iterator2}) end};

select_fun(land, {Term1, false, Iterator1}, {Term2, true, Iterator2}) when Term1 > Term2 ->
    {Term2, true, fun() -> select_fun(land, {Term1, false, Iterator1}, Iterator2()) end};

select_fun(land, {eof, false}, {Term2, true, Iterator2}) ->
    {Term2, true, fun() -> select_fun(land, {eof, false}, Iterator2()) end};

select_fun(land, {Term1, false, Iterator1}, {eof, true}) ->
    {Term1, false, fun() -> select_fun(land, Iterator1(), {eof, true}) end};


%% Handle 'AND' cases, notflags = [true, false], use previous clauses...
select_fun(land, {Term1, true, Iterator1}, {Term2, false, Iterator2}) ->
    select_fun(land, {Term2, false, Iterator2}, {Term1, true, Iterator1});

select_fun(land, {eof, true}, {Term2, false, Iterator2}) ->
    select_fun(land, {Term2, false, Iterator2}, {eof, true});

select_fun(land, {Term1, true, Iterator1}, {eof, false}) ->
    select_fun(land, {eof, false}, {Term1, true, Iterator1});


%% Handle 'AND' cases, notflags = [true, true]
select_fun(land, {Term1, true, Iterator1}, {Term2, true, Iterator2}) when element(1, Term1) == element(1, Term2) ->
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, true, fun() -> select_fun(land, Iterator1(), Iterator2()) end};

select_fun(land, {Term1, true, Iterator1}, {Term2, true, Iterator2}) when Term1 < Term2 ->
    {Term1, true, fun() -> select_fun(land, Iterator1(), {Term2, true, Iterator2}) end};

select_fun(land, {Term1, true, Iterator1}, {Term2, true, Iterator2}) when Term1 > Term2 ->
    {Term2, true, fun() -> select_fun(land, {Term1, true, Iterator1}, Iterator2()) end};

select_fun(land, {eof, true}, {Term2, true, Iterator2}) ->
    {Term2, true, fun() -> select_fun(land, {eof, true}, Iterator2()) end};

select_fun(land, {Term1, true, Iterator1}, {eof, true}) ->
    {Term1, true, fun() -> select_fun(land, Iterator1(), {eof, true}) end};

%% HANDLE 'AND' case, end of results.
select_fun(land, {eof, true}, {eof, true}) ->
    {eof, true};

select_fun(land, {eof, _}, {eof, _}) ->
    {eof, false};


%% Handle 'OR' cases, notflags = [false, false]
select_fun(lor, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when element(1, Term1) == element(1, Term2) ->
    NewTerm = riak_search_utils:combine_terms(Term1, Term2),
    {NewTerm, false, fun() -> select_fun(lor, Iterator1(), Iterator2()) end};

select_fun(lor, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 < Term2 ->
    {Term1, false, fun() -> select_fun(lor, Iterator1(), {Term2, false, Iterator2}) end};

select_fun(lor, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 > Term2 ->
    {Term2, false, fun() -> select_fun(lor, {Term1, false, Iterator1}, Iterator2()) end};

select_fun(lor, {eof, false}, {Term2, false, Iterator2}) ->
    {Term2, false, fun() -> select_fun(lor, {eof, false}, Iterator2()) end};

select_fun(lor, {Term1, false, Iterator1}, {eof, false}) ->
    {Term1, false, fun() -> select_fun(lor, Iterator1(), {eof, false}) end};


%% Handle 'OR' cases, notflags = [false, true]. 
%% Basically, not flags are ignored in an OR.

select_fun(lor, {Term1, false, Iterator1}, {_, true, _}) ->
    select_fun(lor, {Term1, false, Iterator1}, {eof, false});

select_fun(lor, {Term1, false, Iterator1}, {eof, _}) ->
    select_fun(lor, {Term1, false, Iterator1}, {eof, false});

select_fun(lor, {_, true, _}, {Term2, false, Iterator2}) ->
    select_fun(lor, {Term2, false, Iterator2}, {eof, false});

select_fun(lor, {eof, _}, {Term2, false, Iterator2}) ->
    select_fun(lor, {Term2, false, Iterator2}, {eof, false});


%% Handle 'OR' cases, notflags = [true, true]
select_fun(lor, {_, true, _}, {_, true, _}) ->
    {eof, false};

%% HANDLE 'OR' case, end of results.
select_fun(lor, {eof, _}, {eof, _}) ->
    {eof, false};

%% Error on any unhandled cases...
select_fun(Type, Iterator1, Iterator2) ->
    ?PRINT({select_fun, unhandled_case, Type, Iterator1, Iterator2}),
    throw({select_fun, unhandled_case, Type, Iterator1, Iterator2}).    
