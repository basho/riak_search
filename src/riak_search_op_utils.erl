%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_utils).

-export([
    iterator_tree/3,
    gather_iterator_results/3,
    gather_stream_results/4,
    gather_stream_results/5
]).

-include("riak_search.hrl").
-define(STREAM_TIMEOUT, 15000).

%% Given a long list of iterators (which are zero arity functions that
%% dole out results one at a time in the form {Term, Op, NewIterator}
%% or {eof, Op}), combine the list in a balanced binary tree format to
%% expose as a single topmost iterator.  All of the iterators return
%% results in sorted order, so a SelectFun is used to maintain the
%% sorted order as well as filter out any results that we don't want.
iterator_tree(SelectFun, OpList, SearchState) ->
    %% Turn all operations into iterators and then combine into a tree.
    Iterators = [it_op(X, SearchState) || X <- OpList],
    it_combine(SelectFun, Iterators).

%% @private Given a list of iterators, combine into a tree. Works by
%% walking through the list pairing two iterators together (which
%% combines a level of iterators) and then calling itself recursively
%% until there is only one iterator left.
it_combine(SelectFun, Iterators) ->
    case it_combine_inner(SelectFun, Iterators) of
        [] ->
            %% No iterators, so return eof.
            fun() -> {eof, undefined} end;
        [OneIterator] ->
            %% We've successfully collapsed to a single iterator.
            OneIterator;
        ManyIterators ->
            %% More collapsing is neccessary.
            it_combine(SelectFun, ManyIterators)
    end.
%% @private it_combine_inner walks through a list of iterators,
%% pairing them together, returning the new paired list which should
%% be at most N/2+1 of the original iterator's size.
it_combine_inner(_SelectFun, []) ->
    [];
it_combine_inner(_SelectFun, [Iterator]) ->
    [Iterator];
it_combine_inner(SelectFun, [IteratorA,IteratorB|Rest]) ->
    Iterator = fun() -> SelectFun(IteratorA(), IteratorB()) end,
    [Iterator|it_combine_inner(SelectFun, Rest)].

%% @private Chain an operator, and build an iterator function around
%% it. The iterator will return {Result, NotFlag, NewIteratorFun} each
%% time it is called, or block until one is available. When there are
%% no more results, it will return {eof, NotFlag}.
it_op(Op, SearchState) ->
    %% Spawn a collection process...
    Ref = make_ref(),
    F = fun() ->
                Parent = SearchState#search_state.parent,
                erlang:link(Parent),
                erlang:process_flag(trap_exit, true),
                it_op_collector_loop(Parent, Ref, [])
        end,
    Pid = erlang:spawn_link(F),

    %% Chain the op...
    riak_search_op:chain_op(Op, Pid, Ref, SearchState),

    %% Return an iterator function. Returns
    %% a new result.
    fun() ->
            it_op_inner(Pid, Ref, Op)
    end.

%% @private Holds the function body of a leaf-iterator. When called,
%% it requests the next result from the mailbox, and serves it up
%% along with the new Iterator function to be called in the form
%% {Term, Op, NewIteratorFun}. If we've hit the end of results, it
%% returns {eof, Op}.
it_op_inner(Pid, Ref, Op) ->
    Pid!{get_result, self(), Ref},
    receive
        {result, eof, Ref} ->
            {eof, Op};
        {result, Result, Ref} ->
            {Result, Op, fun() -> it_op_inner(Pid, Ref, Op) end};
        X ->
            io:format("it_inner(~p, ~p, ~p)~n>> unknown message: ~p~n", [Pid, Ref, Op, X])
    end.

%% @private This runs in a separate process, collecting the incoming
%% messages from an operation, and holding the message until it is
%% requested by it_op_inner/3. We trap_exits and look for the 'EXIT'
%% message because there is a chance that the calling process doesn't
%% iterate through the entire result set (for instance, during a set
%% intersection when one side runs out of results, there's no need to
%% iterate through the remaining results on the other side). In that
%% case, we want to end. The other option would be to change the
%% calling process to make sure we iterate through the entire result
%% set, but that just creates more work for the system (and more
%% code).
it_op_collector_loop(Parent, Ref, []) ->
    receive
        {results, Results, Ref} ->
            it_op_collector_loop(Parent, Ref, Results);
        {disconnect, Ref} ->
            it_op_collector_loop(Parent, Ref, eof);
        {'EXIT', Parent, _} ->
            stop
    end;
it_op_collector_loop(Parent, Ref, [Result|Results]) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, Result, OutputRef},
            it_op_collector_loop(Parent, Ref, Results);
        {'EXIT', Parent, _} ->
            stop
    end;
it_op_collector_loop(Parent, _Ref, eof) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, eof, OutputRef};
        {'EXIT', Parent, _} ->
            stop
    end.

%% Given an iterator, gather results into an accumulator, and send to
%% an OutputPid with OutputRef once we've collected enough results.
gather_iterator_results(OutputPid, OutputRef, Iterator) ->
    gather_iterator_results(OutputPid, OutputRef, Iterator, []).
gather_iterator_results(OutputPid, OutputRef, {Term, Op, Iterator}, Acc)
  when length(Acc) > ?RESULTVEC_SIZE ->
    OutputPid ! {results, lists:reverse(Acc), OutputRef},
    gather_iterator_results(OutputPid, OutputRef, {Term, Op, Iterator}, []);
gather_iterator_results(OutputPid, OutputRef, {Term, _Op, Iterator}, Acc) ->
    gather_iterator_results(OutputPid, OutputRef, Iterator(), [Term|Acc]);
gather_iterator_results(OutputPid, OutputRef, {eof, _}, Acc) ->
    OutputPid ! {results, lists:reverse(Acc), OutputRef},
    OutputPid ! {disconnect, OutputRef}.


%% Gathers result vectors sent to the current Pid by a backend stream
%% or range operation, run a transform function, and shuttles the
%% results to the given OutputPid and OutputRef.
%%
%% TODO maybe instead of throw return timeout or {error, timeout}
-spec gather_stream_results(stream_ref(), pid(), reference(), fun()) ->
                                   any() | no_return().
gather_stream_results(Ref, OutputPid, OutputRef, TransformFun) ->
    gather_stream_results(Ref, OutputPid, OutputRef, TransformFun, ?STREAM_TIMEOUT).

-spec gather_stream_results(stream_ref(), pid(), reference(), fun(), integer()) ->
                                   any() | no_return().
gather_stream_results(Ref, OutputPid, OutputRef, TransformFun, Timeout) ->
    receive
        {Ref, done} ->
            OutputPid!{disconnect, OutputRef};

        {Ref, {result_vec, ResultVec}} ->
            ResultVec2 = lists:map(TransformFun, ResultVec),
            OutputPid!{results, ResultVec2, OutputRef},
            gather_stream_results(Ref, OutputPid, OutputRef, TransformFun, Timeout);

        {Ref, {error, _} = Err} ->
            OutputPid ! Err;

        %% TODO: Check if this is dead code
        {Ref, {result, {DocID, Props}}} ->
            NewResult = TransformFun({DocID, Props}),
            OutputPid!{results, [NewResult], OutputRef},
            gather_stream_results(Ref, OutputPid, OutputRef, TransformFun, Timeout)
    after
        Timeout ->
            throw(stream_timeout)
    end.
