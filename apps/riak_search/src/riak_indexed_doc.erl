%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_indexed_doc).

-export([
    new/4,
    index/1, id/1, 
    fields/1, regular_fields/1, facets/1,
    props/1, add_prop/3, set_props/2, clear_props/1, 
    postings/1,
    fold_terms/3,
    to_json/1, from_json/1, 
    to_mochijson2/1, to_mochijson2/2,
    analyze/1, analyze/2,
    get_obj/3, get/3, put/2, delete/3
]).

-include("riak_search.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(riak_search_utils, [to_binary/1]).

%% Create a new indexed doc
new(Id, Fields, Props, Index) ->
    {ok, Schema} = riak_search_config:get_schema(Index),
    {RegularFields, FacetFields} = normalize_fields(Fields, Schema),
    #riak_idx_doc{id=Id, fields=RegularFields, facets=FacetFields, props=Props, index=Index}.

fields(#riak_idx_doc{fields=Fields, facets=Facets}) ->
    Fields ++ Facets.

regular_fields(#riak_idx_doc{fields=Fields}) ->
    Fields.

facets(#riak_idx_doc{facets=Facets}) ->
    Facets.

index(#riak_idx_doc{index=Index}) ->
    Index.

id(#riak_idx_doc{id=Id}) ->
    Id.

props(#riak_idx_doc{props=Props}) ->
    Props.

add_prop(Name, Value, #riak_idx_doc{props=Props}=Doc) ->
    Doc#riak_idx_doc{props=[{Name, Value}|Props]}.

set_props(Props, Doc) ->
    Doc#riak_idx_doc{props=Props}.

clear_props(Doc) ->
    Doc#riak_idx_doc{props=[]}.

postings(Doc) ->
    %% Construct a list of index/field/term/docid/props from analyzer result.
    %% 
    #riak_idx_doc{index = Index, id = Id, facets = Facets} = Doc,
    VisitTerms = fun(FieldName, Term, Pos, Acc) ->
                         Props = build_props(Pos, Facets),
                         [{Index, FieldName, Term, Id, Props} | Acc]
                 end,
    fold_terms(VisitTerms, [], Doc).

%% Fold over each of the field/terms calling the folder function with
%% Fun(FieldName, Term, Pos, TermsAcc)
fold_terms(Fun, Acc0, Doc) ->
    VisitFields = 
        fun({FieldName, TermPos}, FieldsAcc) ->
                VisitTerms = 
                    fun({Term, Pos}, TermsAcc) ->
                            Fun(FieldName, Term, Pos, TermsAcc)
                    end,
                lists:foldl(VisitTerms, FieldsAcc, TermPos)
        end,
    lists:foldl(VisitFields, Acc0, Doc#riak_idx_doc.field_terms).
     
     

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
    #riak_idx_doc{index=Index, fields=Fields}=IdxDoc,
    {ok, Schema} = riak_search_config:get_schema(Index),
    
    %% For each Field = {FieldName, FieldValue}, split the FieldValue
    %% into terms and build a list of positions for those terms.
    F2 = fun({FieldName, FieldValue}, Acc2) ->
                 {ok, Terms} = analyze_field(FieldName, FieldValue, Schema, AnalyzerPid),
                 [{FieldName, get_term_positions(Terms)} | Acc2]
         end,
    FieldTerms = lists:foldl(F2, [], Fields),
    {ok, IdxDoc#riak_idx_doc{field_terms = FieldTerms}}.

%% Normalize the list of input fields against the schema
%% - drop any skip fields
%% - replace any aliased fields with the correct name
%% - combine duplicate field names into a single field (separate by spaces)
%% - split out into regular and facets fields
normalize_fields(DocFields, Schema) ->
    %% Split up the fields into regular fields and facet fields,
    %% dropping any skipped fields.
    Fun = fun({InFieldName, FieldValue}, {Regular, Facets}=Acc) ->
                  FieldDef = Schema:find_field(InFieldName),
                  case Schema:is_skip(FieldDef) of
                      true ->
                          Acc;
                      false ->
                          Field = {normalize_field_name(InFieldName, FieldDef, Schema), 
                                   to_binary(FieldValue)},
                          case Schema:is_field_facet(FieldDef) of
                              true ->
                                  {Regular, [Field | Facets]};
                              false ->
                                  {[Field | Regular], Facets}
                          end
                  end
          end,
    {RevRegular, RevFacets} = lists:foldl(Fun, {[], []}, DocFields),
    
    %% Aliasing makes it possible to have multiple entries in
    %% RevRegular.  Combine multiple entries for the same field name
    %% into a single field.
    {merge_fields(lists:reverse(RevRegular)), 
     merge_fields(lists:reverse(RevFacets))}.

%% @private
%% Normalize the field name - if an alias of a regular field
%% then replace it with the defined name.  Dynamic field names
%% are just passed through.
normalize_field_name(FieldName, FieldDef, Schema) ->
    case Schema:is_dynamic(FieldDef) of
        true ->
            FieldName;
        _ ->
            Schema:field_name(FieldDef)
    end.

%% @private
%% Merge fields of the same name, with spaces between them
merge_fields(DocFields) ->
    %% Use lists:keysort as it gives stable ordering of values.  If multiple
    %% fields are given they'll be combined in order which is probably least
    %% suprising for users.
    lists:foldl(fun merge_fields_folder/2, [], lists:keysort(1, DocFields)).

%% @private
%% Merge field data with previous if the names match.  Input must be sorted.
merge_fields_folder({FieldName, NewFieldData}, [{FieldName, FieldData} | Fields]) ->
    [{FieldName, <<FieldData/binary, " ", NewFieldData/binary>>} | Fields];
merge_fields_folder(New, Fields) ->
    [New | Fields].
      

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
    gb_trees:to_list(Tree).

%% @private
%% Given a term and a list of positions, generate a list of
%% properties.
build_props(Positions, Facets) ->
    [{word_pos, Positions},
     {freq, length(Positions)} | Facets].

%% Returns a Riak object.
get_obj(RiakClient, DocIndex, DocID) ->
    Bucket = idx_doc_bucket(DocIndex),
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
    DocBucket = idx_doc_bucket(DocIndex),
    DocKey = to_binary(DocID),
    case RiakClient:get(DocBucket, DocKey) of
        {ok, Obj} -> 
            DocObj = riak_object:update_value(Obj, IdxDoc);
        {error, notfound} ->
            DocObj = riak_object:new(DocBucket, DocKey, IdxDoc)
    end,
    RiakClient:put(DocObj).

delete(RiakClient, DocIndex, DocID) ->
    DocBucket = idx_doc_bucket(DocIndex),
    DocKey = to_binary(DocID),
    RiakClient:delete(DocBucket, DocKey).


idx_doc_bucket(Bucket) when is_binary(Bucket) ->
    <<"_rsid_", Bucket/binary>>;
idx_doc_bucket(Bucket) ->
    idx_doc_bucket(to_binary(Bucket)).

-ifdef(TEST).

normalize_fields_test() ->
    SchemaProps = [{version, 1},{default_field, "afield"}],
    FieldDefs =  [{field, [{name, "skipme"},
                           {alias, "skipmetoo"},
                           skip]},
                  {field, [{name, "afield"},
                           {alias, "afieldtoo"}]},
                  {field, [{name, "anotherfield"},
                           {alias, "anotherfieldtoo"}]},
                  {field, [{name, "afacet"},
                           {alias, "afacettoo"},
                           facet]},
                  {field, [{name, "anotherfacet"},
                           {alias, "anotherfacettoo"},
                           {facet, true}]}],
    
    SchemaDef = {schema, SchemaProps, FieldDefs},
    {ok, Schema} = riak_search_schema_parser:from_eterm(is_skip_test, SchemaDef),

    ?assertEqual({[], []}, normalize_fields([], Schema)),    
    ?assertEqual({[{"afield",<<"data">>}], []}, normalize_fields([{"afield","data"}], Schema)),
    ?assertEqual({[{"afield",<<"data">>}], []}, normalize_fields([{"afieldtoo","data"}], Schema)),
    ?assertEqual({[{"afield",<<"one two three">>}], []}, 
                 normalize_fields([{"afieldtoo","one"},
                                   {"afield","two"},
                                   {"afieldtoo", "three"}], Schema)),
    ?assertEqual({[{"anotherfield", <<"abc def ghi">>},
                   {"afield",<<"one two three">>}],
                  [{"afacet", <<"first second">>}]},
                 normalize_fields([{"anotherfield","abc"},
                                   {"afieldtoo","one"},
                                   {"skipme","skippable terms"},
                                   {"anotherfieldtoo", "def"},
                                   {"afield","two"},
                                   {"skipmetoo","not needed"},
                                   {"anotherfield","ghi"},
                                   {"afieldtoo", "three"},
                                   {"afacet", "first"},
                                   {"afacettoo", "second"}], Schema)).

-endif. % TEST
