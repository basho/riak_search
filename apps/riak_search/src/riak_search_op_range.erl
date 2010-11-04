%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_range).
-export([
         preplan/2,
         chain_op/4
        ]).

-import(riak_search_utils, [to_binary/1]).
-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").
-define(INDEX_DOCID(Term), ({element(1, Term), element(2, Term)})).

preplan(Op, State) -> 
    NewOp = #range_sized { from=Op#range.from, to=Op#range.to, size=all },
    riak_search_op_range_sized:preplan(NewOp, State).

chain_op(Op, OutputPid, OutputRef, State) ->
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,

    %% Parse the FromTerms, ensure that there is only 1, otherwise throw an error.
    {FromBorder, FromString} = Op#range.from,
    {ok, FromTerms} = riak_search_op_string:analyze_term(IndexName, FieldName, to_binary(FromString)),
    length(FromTerms) == 1 orelse throw({error, too_many_terms, FromTerms}),
    FromTerm = hd(FromTerms),

    %% Parse the ToTerms, ensure that there is only 1, otherwise throw an error.
    {ToBorder, ToString} = Op#range.to,
    {ok, ToTerms} = riak_search_op_string:analyze_term(IndexName, FieldName, to_binary(ToString)),
    length(ToTerms) == 1 orelse throw({error, too_many_terms, ToTerms}),
    ToTerm = hd(ToTerms),

    %% Convert to a #range_sized...
    NewOp = #range_sized { from={FromBorder, FromTerm}, to={ToBorder, ToTerm}, size=all },
    riak_search_op_range_sized:chain_op(NewOp, OutputPid, OutputRef, State).
