%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_range_worker).
-export([
         preplan_op/2,
         chain_op/4
        ]).

-include("riak_search.hrl").
-define(STREAM_TIMEOUT, 15000).

-record(scoring_vars, {term_boost, doc_frequency, num_docs}).
preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef, QueryProps) ->
    spawn_link(fun() -> start_loop(Op, OutputPid, OutputRef, QueryProps) end),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, _QueryProps) ->
    %% Get the scoring vars...
    %% ScoringVars = #scoring_vars {
    %%     term_boost = proplists:get_value(boost, Op#term.options, 1),
    %%     doc_frequency = hd([X || {node_weight, _, X} <- Op#term.options] ++ [0]),
    %%     num_docs = proplists:get_value(num_docs, QueryProps)
    %% },
    ScoringVars = #scoring_vars {
        term_boost = 1,
        doc_frequency = 1,
        num_docs = 1
    },

    %% Create filter function...
    Facets = proplists:get_all_values(facets, Op#range_worker.options),
    Fun = fun(_DocID, Props) ->
        riak_search_facets:passes_facets(Props, Facets)
    end,

    %% Start streaming the results...
    {Index, Field, StartTerm, EndTerm} = Op#range_worker.q,
    Size = Op#range_worker.size,
    VNode = Op#range_worker.vnode,
    {ok, Ref} = range(VNode, Index, Field, StartTerm, EndTerm, Size, Fun),

    %% Gather the results...
    %% TODO - This type conversion should be removed in bug 484
    %% "Standardize on a string representation".
    IndexB = riak_search_utils:to_binary(Index),
    loop(IndexB, ScoringVars, Ref, OutputPid, OutputRef).

range(VNode, Index, Field, StartTerm, EndTerm, Size, FilterFun) ->
    riak_search_vnode:range(VNode, Index, Field, StartTerm, EndTerm, Size, FilterFun, self()).

loop(Index, ScoringVars, Ref, OutputPid, OutputRef) ->
    receive 
        {Ref, done} ->
            OutputPid!{disconnect, OutputRef};
            
        {Ref, {result_vec, ResultVec}} ->
            % todo: scoring
            F = fun({DocID, Props}) ->
                        NewProps = riak_search_op_term:calculate_score(ScoringVars, Props),
                        {Index, DocID, NewProps} 
                end,
            ResultVec2 = lists:map(F, ResultVec),
            OutputPid!{results, ResultVec2, OutputRef},
            loop(Index, ScoringVars, Ref, OutputPid, OutputRef);

        %% TODO: Check if this is dead code
        {Ref, {result, {DocID, Props}}} ->
            NewProps = riak_search_op_term:calculate_score(ScoringVars, Props),
            OutputPid!{results, [{Index, DocID, NewProps}], OutputRef},
            loop(Index, ScoringVars, Ref, OutputPid, OutputRef)
    after
        ?STREAM_TIMEOUT ->
            throw(stream_timeout)
    end.
