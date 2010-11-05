%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_term).
-export([
         preplan/2,
         chain_op/4,
         default_filter/2,
         default_transform/1
        ]).

-import(riak_search_utils, [to_binary/1]).

%% Look up results from the index without any kind of text analyzis on
%% term. Filter and transform the results, and send them to the
%% OutputPid.

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

%% Need term count for node planning. Used in #intersection and
%% #union. Calculate this during preplan based on where the most
%% results come from.

%% [{info, Index, Field, String}] -> [{Node, Count}]
%% Need term count for scoring, in riak_search_op_search.

%% {info, Index, Field, String} -> [{Node, Count}]
%% Generate term count in term, managed by string.

preplan(Op, State) -> 
    %% Get info about the term, return in props...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    Term = to_binary(Op#term.s),
    Weights = info(IndexName, FieldName, Term),
    [{?OPKEY(Op), {Node, Count}} || {_, Node, Count} <- Weights].

chain_op(Op, OutputPid, OutputRef, State) ->
    spawn_link(fun() -> start_loop(Op, OutputPid, OutputRef, State) end),
    {ok, 1}.

start_loop(Op, OutputPid, OutputRef, State) ->
    %% Get the current index/field...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    Term = to_binary(Op#term.s),

    %% Stream the results for a single term...
    FilterFun = Op#term.filter,
    {ok, Ref} = stream(IndexName, FieldName, Term, FilterFun),

    %% Collect the results...
    TransformFun = Op#term.transform,
    riak_search_op_utils:gather_stream_results(Ref, OutputPid, OutputRef, TransformFun).

stream(Index, Field, Term, FilterFun) ->
    %% Get the primary preflist, minus any down nodes. (We don't use
    %% secondary nodes since we ultimately read results from one node
    %% anyway.)
    DocIdx = riak_search_ring_utils:calc_partition(Index, Field, Term),
    {ok, Schema} = riak_search_config:get_schema(Index),
    NVal = Schema:n_val(),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, NVal, riak_search),

    %% Try to use the local node if possible. Otherwise choose
    %% randomly.
    case lists:keyfind(node(), 2, Preflist) of
        false ->
            PreflistEntry = riak_search_utils:choose(Preflist);
        PreflistEntry ->
            PreflistEntry = PreflistEntry
    end,
    riak_search_vnode:stream([PreflistEntry], Index, Field, Term, FilterFun, self()).

default_filter(_, _) -> true.
default_transform(Result) -> Result.

info(Index, Field, Term) ->
    %% Get the primary preflist, minus any down nodes. (We don't use
    %% secondary nodes since we ultimately read results from one node
    %% anyway.)
    DocIdx = riak_search_ring_utils:calc_partition(Index, Field, Term),
    {ok, Schema} = riak_search_config:get_schema(Index),
    NVal = Schema:n_val(),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, NVal, riak_search),
    
    {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
    {ok, Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, []),
    Results.
