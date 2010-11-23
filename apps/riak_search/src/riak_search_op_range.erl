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

    %% Check if the current field is an integer. If so, then we'll
    %% need to OR together two range ops.
    {ok, Schema} = riak_search_config:get_schema(IndexName),
    Field = Schema:find_field(FieldName),
    DifferentSigns = is_negative(FromTerm) xor is_negative(ToTerm),
    case Schema:field_type(Field) of
        integer when DifferentSigns ->
            %% Create two range operations, one on the negative side,
            %% one on the positive side. Don't need to worry about
            %% putting the terms in order here, this is taken care of
            %% by the 'riak_search_op_range_sized' module.
            RangeOp1 = #range_sized { from={FromBorder, FromTerm}, to={inclusive, to_zero(FromTerm)}, size=all },
            RangeOp2 = #range_sized { from={inclusive, to_zero(ToTerm)}, to={ToBorder, ToTerm}, size=all },
            
            %% Run the new operation...
            NewOp = #union { id=Op#range.id, ops=[RangeOp1, RangeOp2] },
            riak_search_op_union:chain_op(NewOp, OutputPid, OutputRef, State);
        _ ->
            %% Convert to a #range_sized...
            NewOp = #range_sized { from={FromBorder, FromTerm}, to={ToBorder, ToTerm}, size=all },
            riak_search_op_range_sized:chain_op(NewOp, OutputPid, OutputRef, State)
    end.


%% Return true if a binary term begins with '-' (ie: it's a negative.)
is_negative(<<C, _/binary>>) ->
    C == $-.


%% Given a binary term, return a binary of the same length with all
%% characters converted to 0.
to_zero(Term) ->
    to_zero(Term, []).
to_zero(<<$-, Rest/binary>>, Acc) ->
    to_zero(Rest, [$-|Acc]);
to_zero(<<_, Rest/binary>>, Acc) ->
    to_zero(Rest, [$0|Acc]);
to_zero(<<>>, Acc) ->
    list_to_binary(lists:reverse(Acc)).

