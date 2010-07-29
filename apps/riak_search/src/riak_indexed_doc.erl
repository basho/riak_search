%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_indexed_doc).

-include("riak_search.hrl").

-export([
    new/2, new/4,
    index/1, id/1, 
    fields/1, add_field/3, set_fields/2, clear_fields/1,
    props/1, add_prop/3, set_props/2, clear_props/1, 
    to_json/1, from_json/1, 
    to_mochijson2/1, to_mochijson2/2,
    analyze/1, analyze/2,
    get_obj/3, get/3, put/2, delete/3
]).

-import(riak_search_utils, [to_binary/1]).

new(Id, Index) ->
    #riak_idx_doc{id=Id, index=Index}.

new(Id, Fields, Props, Index) ->
    #riak_idx_doc{id=Id, fields=Fields, props=Props, index=Index}.

fields(#riak_idx_doc{fields=Fields}) ->
    Fields.

index(#riak_idx_doc{index=Index}) ->
    Index.

id(#riak_idx_doc{id=Id}) ->
    Id.

add_field(Name, Value, #riak_idx_doc{fields=Fields}=Doc) ->
    Doc#riak_idx_doc{fields=[{Name, Value}|Fields]}.

set_fields(Fields, Doc) ->
    Doc#riak_idx_doc{fields=Fields}.

clear_fields(Doc) ->
    Doc#riak_idx_doc{fields=[]}.

props(#riak_idx_doc{props=Props}) ->
    Props.

add_prop(Name, Value, #riak_idx_doc{props=Props}=Doc) ->
    Doc#riak_idx_doc{props=[{Name, Value}|Props]}.

set_props(Props, Doc) ->
    Doc#riak_idx_doc{props=Props}.

clear_props(Doc) ->
    Doc#riak_idx_doc{props=[]}.

to_json(Doc) ->
    mochijson2:encode(to_mochijson2(Doc)).

to_mochijson2(Doc) ->
    F = fun({_Name, Value}) -> Value end,
    to_mochijson2(F, Doc).

to_mochijson2(XForm, #riak_idx_doc{id=Id, index=Index, fields=Fields, props=Props}) ->
    {struct, [{id, riak_search_utils:to_binary(Id)},
              {index, riak_search_utils:to_binary(Index)},
              {fields, {struct, [{riak_search_utils:to_binary(Name),
                                  XForm({Name, Value})} || {Name, Value} <- lists:keysort(1, Fields)]}},
              {props, {struct, [{riak_search_utils:to_binary(Name),
                                 riak_search_utils:to_binary(Value)} || {Name, Value} <- Props]}}]}.

from_json(Json) ->
    case mochijson2:decode(Json) of
        {struct, Data} ->
            Id = proplists:get_value(<<"id">>, Data),
            Index = proplists:get_value(<<"index">>, Data),
            build_doc(Id, Index, Data);
        {error, _} = Error ->
            Error;
        _NonsenseJson ->
            {error, bad_json_format}
    end.

%% @private
build_doc(Id, Index, _Data) when Id =:= undefined orelse
                                 Index =:= undefined ->
    {error, missing_id_or_index};
build_doc(Id, Index, Data) ->
    #riak_idx_doc{id=riak_search_utils:from_binary(Id), index=binary_to_list(Index),
                  fields=read_json_fields(<<"fields">>, Data),
                  props=read_json_fields(<<"props">>, Data)}.

%% @private
read_json_fields(Key, Data) ->
    case proplists:get_value(Key, Data) of
        {struct, Fields} ->
            [{riak_search_utils:from_binary(Name),
              riak_search_utils:from_binary(Value)} || {Name, Value} <- Fields];
        _ ->
            []
    end.

%% Parse a #riak_idx_doc{} record.
%% Return {ok, [{Index, FieldName, Term, DocID, Props}]}.
analyze(IdxDoc) when is_record(IdxDoc, riak_idx_doc) ->
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    try
        analyze(IdxDoc, AnalyzerPid)
    after
        qilr:close_analyzer(AnalyzerPid)
    end.


%% Parse a #riak_idx_doc{} record using the provided analyzer pid.
%% Return {ok, [{Index, FieldName, Term, DocID, Props}]}.
analyze(IdxDoc, AnalyzerPid) when is_record(IdxDoc, riak_idx_doc) ->
    %% Extract fields, get schema...
    #riak_idx_doc{id=DocID, index=Index, fields=DocFields}=IdxDoc,
    {ok, Schema} = riak_search_config:get_schema(Index),

    %% Pull out the facet properties...
    F1 = fun({FieldName, _FieldValue}) ->
                 Field = Schema:find_field(FieldName),
                 Schema:is_field_facet(Field)
    end,
    FacetFields = lists:filter(F1, DocFields),
    RegularFields = DocFields -- FacetFields,
            
    %% For each Field = {FieldName, FieldValue}, split the FieldValue
    %% into terms. Build a list of positions for those terms, then get
    %% a de-duped list of the terms. For each, index the FieldName /
    %% Term / DocID / Props.
    F2 = fun({FieldName, FieldValue}, Acc2) ->
                 {ok, Terms} = analyze_field(FieldName, FieldValue, Schema, AnalyzerPid),
                 PositionTree = get_term_positions(Terms),
                 Terms1 = gb_trees:keys(PositionTree),
                 F3 = fun(Term, Acc3) ->
                              Props = build_props(Term, PositionTree),
                              [{Index, FieldName, Term, DocID, Props ++ FacetFields}|Acc3]
                      end,
                 lists:foldl(F3, Acc2, Terms1)
         end,
    Postings = lists:foldl(F2, [], RegularFields),
    {ok, Postings}.

%% @private
%% Parse a FieldValue into a list of terms.
%% Return {ok, [Terms}}.
analyze_field(FieldName, FieldValue, Schema, AnalyzerPid) ->
    %% Get the field...
    Field = Schema:find_field(FieldName),
    AnalyzerFactory = Schema:analyzer_factory(Field),
    AnalyzerArgs = Schema:analyzer_args(Field),

    %% Analyze the field...
    qilr_analyzer:analyze(AnalyzerPid, FieldValue, AnalyzerFactory, AnalyzerArgs).


%% @private Given a list of tokens, build a gb_tree mapping words to a
%% list of word positions.
get_term_positions(Terms) ->
    F = fun(Term, {Pos, Acc}) ->
        case gb_trees:lookup(Term, Acc) of
            {value, Positions} ->
                {Pos + 1, gb_trees:update(Term, [Pos|Positions], Acc)};
            none ->
                {Pos + 1, gb_trees:insert(Term, [Pos], Acc)}
        end
    end,
    {_, Tree} = lists:foldl(F, {1, gb_trees:empty()}, Terms),
    Tree.

%% @private
%% Given a term and a list of positions, generate a list of
%% properties.
build_props(Term, PositionTree) ->
    case gb_trees:lookup(Term, PositionTree) of
        none ->
            [];
        {value, Positions} ->
            [
                {word_pos, Positions},
                {freq, length(Positions)}
            ]
    end.

%% Returns a Riak object.
get_obj(RiakClient, DocIndex, DocID) ->
    Bucket = to_binary(DocIndex),
    Key = to_binary(DocID),
    RiakClient:get(Bucket, Key).

%% Returns a #riak_idx_doc record.
get(RiakClient, DocIndex, DocID) ->
    case get_obj(RiakClient, DocIndex, DocID) of
        {ok, Obj} -> 
            riak_object:get_value(Obj);
        Other ->
            Other
    end.

%% Write the object to Riak.
put(RiakClient, IdxDoc) ->
    #riak_idx_doc { id=DocID, index=DocIndex } = IdxDoc,
    DocBucket = to_binary(DocIndex),
    DocKey = to_binary(DocID),
    case RiakClient:get(DocBucket, DocKey) of
        {ok, Obj} -> 
            DocObj = riak_object:update_value(Obj, IdxDoc);
        {error, notfound} ->
            DocObj = riak_object:new(DocBucket, DocKey, IdxDoc)
    end,
    RiakClient:put(DocObj).

delete(RiakClient, DocIndex, DocID) ->
    DocBucket = to_binary(DocIndex),
    DocKey = to_binary(DocID),
    RiakClient:delete(DocBucket, DocKey).
