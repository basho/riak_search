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
    %% Set some vars...

    %% Create a grouped iterator...
    OpList = Op#land.ops,
    Iterator = build_group_iterator(Type, OpList),
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


%% Chain a list of iterators into what looks like one single iterator.
build_group_iterator(_, [Op]) ->
    build_op_iterator(Op);
build_group_iterator(Type, [Op|OpList]) ->
    OpIterator = build_op_iterator(Op),
    GroupIterator = build_group_iterator(Type, OpList),
    fun() -> group_iterator(Type, OpIterator(), GroupIterator()) end.

%% Chain an operator, and build an iterator function around it. The
%% iterator will return {Result, NotFlag, NewIteratorFun} each time it is called, or block
%% until one is available. When there are no more results, it will
%% return {eof, NotFlag, NewIteratorFun}.
build_op_iterator(Op) ->
    %% Spawn a collection process...
    Ref = make_ref(),
    Pid = spawn_link(fun() -> collector_loop(Ref, []) end),
    NotFlag = is_record(Op, lnot),
    
    %% Chain the op...
    riak_search_op:chain_op(Op, Pid, Ref),

    %% Return an iterator function. Returns
    %% a new result.
    fun() -> iterator_fun(Pid, make_ref(), NotFlag) end.


%% Iterator function body.
iterator_fun(Pid, Ref, NotFlag) ->
    Pid!{get_result, self(), Ref},
    receive 
        {result, eof, Ref} ->
            {eof, NotFlag};

        {result, Result, Ref} ->
            {Result, NotFlag, fun() -> iterator_fun(Pid, Ref, NotFlag) end}
    end.


%% Collect messages in the process's mailbox, and wait until someone
%% requests it.
collector_loop(Ref, []) ->
    receive
        {results, Results, Ref} ->
            collector_loop(Ref, Results);
        {disconnect, Ref} ->
            collector_loop(Ref, eof)
    end;
collector_loop(Ref, [Result|Results]) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, Result, OutputRef},
            collector_loop(Ref, Results)
    end;
collector_loop(_Ref, eof) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, eof, OutputRef}
    end.



%% Now, treat the operation as a comparison between two terms.

%% Handle 'AND' cases, notflags = [false, false]
group_iterator(land, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 == Term2->
    {Term1, false, fun() -> group_iterator(land, Iterator1(), Iterator2()) end};

group_iterator(land, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 < Term2 ->
    group_iterator(land, Iterator1(), {Term2, false, Iterator2});

group_iterator(land, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 > Term2 ->
    group_iterator(land, {Term1, false, Iterator1}, Iterator2());

group_iterator(land, {eof, false}, {_Term2, false, _Iterator2}) ->
    {eof, false};

group_iterator(land, {_Term1, false, _Iterator1}, {eof, false}) ->
    {eof, false};

%% Handle 'AND' cases, notflags = [false, true]
group_iterator(land, {Term1, false, Iterator1}, {Term2, true, Iterator2}) when Term1 == Term2->
    group_iterator(land, Iterator1(), {Term2, true, Iterator2});

group_iterator(land, {Term1, false, Iterator1}, {Term2, true, Iterator2}) when Term1 < Term2 ->
    {Term1, false, fun() -> group_iterator(land, Iterator1(), {Term2, true, Iterator2}) end};

group_iterator(land, {Term1, false, Iterator1}, {Term2, true, Iterator2}) when Term1 > Term2 ->
    {Term2, true, fun() -> group_iterator(land, {Term1, false, Iterator1}, Iterator2()) end};

group_iterator(land, {eof, false}, {Term2, true, Iterator2}) ->
    {Term2, true, fun() -> group_iterator(land, {eof, false}, Iterator2()) end};

group_iterator(land, {Term1, false, Iterator1}, {eof, true}) ->
    {Term1, false, fun() -> group_iterator(land, Iterator1(), {eof, true}) end};


%% Handle 'AND' cases, notflags = [true, false], use previous clauses...
group_iterator(land, {Term1, true, Iterator1}, {Term2, false, Iterator2}) ->
    group_iterator(land, {Term2, false, Iterator2}, {Term1, true, Iterator1});

group_iterator(land, {eof, true}, {Term2, false, Iterator2}) ->
    group_iterator(land, {Term2, false, Iterator2}, {eof, true});

group_iterator(land, {Term1, true, Iterator1}, {eof, false}) ->
    group_iterator(land, {eof, false}, {Term1, true, Iterator1});


%% Handle 'AND' cases, notflags = [true, true]
group_iterator(land, {Term1, true, Iterator1}, {Term2, true, Iterator2}) when Term1 == Term2 ->
    {Term1, true, fun() -> group_iterator(land, Iterator1(), Iterator2()) end};

group_iterator(land, {Term1, true, Iterator1}, {Term2, true, Iterator2}) when Term1 < Term2 ->
    {Term1, true, fun() -> group_iterator(land, Iterator1(), {Term2, true, Iterator2}) end};

group_iterator(land, {Term1, true, Iterator1}, {Term2, true, Iterator2}) when Term1 > Term2 ->
    {Term2, true, fun() -> group_iterator(land, {Term1, true, Iterator1}, Iterator2()) end};

group_iterator(land, {eof, true}, {Term2, true, Iterator2}) ->
    {Term2, true, fun() -> group_iterator(land, {eof, true}, Iterator2()) end};

group_iterator(land, {Term1, true, Iterator1}, {eof, true}) ->
    {Term1, true, fun() -> group_iterator(land, Iterator1(), {eof, true}) end};

%% HANDLE 'AND' case, end of results.
group_iterator(land, {eof, true}, {eof, true}) ->
    {eof, true};

group_iterator(land, {eof, _}, {eof, _}) ->
    {eof, false};


%% Handle 'OR' cases, notflags = [false, false]
group_iterator(lor, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 == Term2->
    {Term1, false, fun() -> group_iterator(lor, Iterator1(), Iterator2()) end};

group_iterator(lor, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 < Term2 ->
    {Term1, false, fun() -> group_iterator(lor, Iterator1(), {Term2, false, Iterator2}) end};

group_iterator(lor, {Term1, false, Iterator1}, {Term2, false, Iterator2}) when Term1 > Term2 ->
    {Term2, false, fun() -> group_iterator(lor, {Term1, false, Iterator1}, Iterator2()) end};

group_iterator(lor, {eof, false}, {Term2, false, Iterator2}) ->
    {Term2, false, fun() -> group_iterator(lor, {eof, false}, Iterator2()) end};

group_iterator(lor, {Term1, false, Iterator1}, {eof, false}) ->
    {Term1, false, fun() -> group_iterator(lor, Iterator1(), {eof, false}) end};


%% Handle 'OR' cases, notflags = [false, true]. 
%% Basically, not flags are ignored in an OR.

group_iterator(lor, {Term1, false, Iterator1}, {_, true, _}) ->
    group_iterator(lor, {Term1, false, Iterator1}, {eof, false});

group_iterator(lor, {Term1, false, Iterator1}, {eof, _}) ->
    group_iterator(lor, {Term1, false, Iterator1}, {eof, false});

group_iterator(lor, {_, true, _}, {Term2, false, Iterator2}) ->
    group_iterator(lor, {Term2, false, Iterator2}, {eof, false});

group_iterator(lor, {eof, _}, {Term2, false, Iterator2}) ->
    group_iterator(lor, {Term2, false, Iterator2}, {eof, false});


%% Handle 'OR' cases, notflags = [true, true]
group_iterator(lor, {_, true, _}, {_, true, _}) ->
    {eof, false};

%% HANDLE 'OR' case, end of results.
group_iterator(lor, {eof, _}, {eof, _}) ->
    {eof, false};

%% Error on any unhandled cases...
group_iterator(Type, Iterator1, Iterator2) ->
    ?PRINT({group_iterator, unhandled_case, Type, Iterator1, Iterator2}),
    throw({group_iterator, unhandled_case, Type, Iterator1, Iterator2}).


    
