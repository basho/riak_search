%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_indexed_doc).

-export([
    new/4,
    index/1, id/1,
    idx_doc_bucket/1,
    fields/1, fields/2,
    regular_fields/1, regular_fields/2,
    inline_fields/1, inline_fields/2,
    props/1, add_prop/3, set_props/2, clear_props/1,
    postings/1,
    to_mochijson2/3,
    to_pairs/3,
    analyze/1,
    new_obj/2, get_obj/3, put_obj/2, get/3, put/2, put/3,
    delete/2, delete/3,
    remove_entries/4
]).

-include("riak_search.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Create a new indexed doc
-spec new(any(), any(), search_fields(), any()) -> #riak_idx_doc{}.
new(Index, Id, Fields, Props) ->
    {ok, Schema} = riak_search_config:get_schema(Index),
    {RegularFields, InlineFields} = normalize_fields(Fields, Schema),
    #riak_idx_doc{ index=Index,
                   id=Id,
                   fields=RegularFields,
                   inline_fields=InlineFields,
                   props=Props }.

fields(IdxDoc) -> fields(IdxDoc, all).

fields(IdxDoc, FL) -> regular_fields(IdxDoc, FL) ++ inline_fields(IdxDoc, FL).

regular_fields(IdxDoc) -> regular_fields(IdxDoc, all).

regular_fields(#riak_idx_doc{fields=Fields}, FL) -> filter_fields(Fields, FL).

inline_fields(IdxDoc) -> inline_fields(IdxDoc, all).

inline_fields(#riak_idx_doc{inline_fields=InlineFields}, FL) ->
    filter_fields(InlineFields, FL).

%% @private
-spec filter_fields([{binary(), any(), any()}], all | [binary()]) ->
                           search_fields().
filter_fields(Fields, all) -> [{Name, Val} || {Name, Val, _} <- Fields];
filter_fields(Fields, FL) ->
    FS = ordsets:from_list(FL),
    [{Name, Val} || {Name, Val, _} <- Fields, ordsets:is_element(Name, FS)].

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

%% Construct a list of [{Index, Field, Term, Id, Props, Timestamp}]
%% from previously analyzed results.
postings(IdxDoc) ->
    %% Get some values.
    DocIndex = ?MODULE:index(IdxDoc),
    DocId = ?MODULE:id(IdxDoc),
    InlineFields = [{FieldName, Terms} || {FieldName, _, Terms} <- IdxDoc#riak_idx_doc.inline_fields],
    K = riak_search_utils:current_key_clock(),

    %% Fold over each regular field, and then fold over each term in
    %% that field.
    F1 = fun({FieldName, _, TermPos}, FieldsAcc) ->
                 F2 = fun({Term, Positions}, Acc) ->
                              Props = build_props(Positions, InlineFields),
                              GeneratedTerm = {DocIndex, FieldName, Term, DocId, Props, K},
                              TermLength = erlang:external_size(GeneratedTerm),
                              case TermLength < 32000 of
                                true ->
                                  [GeneratedTerm | Acc];
                                false ->
                                  lager:error("Encountered Large Posting (~w) when indexing Bucket \"~s\", Key \"~s\", and Field Name \"~s\", Skipping.", [TermLength, DocIndex, DocId, FieldName]),
                                  Acc
                              end
                      end,
                 lists:foldl(F2, FieldsAcc, TermPos)
         end,
    lists:foldl(F1, [], IdxDoc#riak_idx_doc.fields).

to_mochijson2(XForm, IdxDoc=#riak_idx_doc{id=Id, index=Index, props=Props}, FL) ->
    Fields = riak_indexed_doc:fields(IdxDoc, FL),
    {struct, [{id, Id},
              {index, Index},
              {fields, {struct, [XForm(Field)
                                 || Field <- lists:keysort(1, Fields)]}},
              {props, {struct, Props}}]}.

%% @doc This is for PB encoding
to_pairs(UK, IdxDoc=#riak_idx_doc{id=Id}, FL) ->
    [{UK, Id}|?MODULE:fields(IdxDoc, FL)].

%% Parse a #riak_idx_doc{} record
%% Return {ok, [{Index, FieldName, Term, DocID, Props}]}.
analyze(IdxDoc)
  when is_record(IdxDoc, riak_idx_doc) andalso IdxDoc#riak_idx_doc.analyzed_flag == true ->
    %% Don't re-analyze an already analyzed idx doc.
    IdxDoc;
analyze(IdxDoc) when is_record(IdxDoc, riak_idx_doc) ->
    %% Extract fields, get schema...
    DocIndex = ?MODULE:index(IdxDoc),
    RegularFields = ?MODULE:regular_fields(IdxDoc),
    Inlines = ?MODULE:inline_fields(IdxDoc),
    {ok, Schema} = riak_search_config:get_schema(DocIndex),

    %% For each Field = {FieldName, FieldValue, _}, split the FieldValue
    %% into terms and build a list of positions for those terms.
    F1 = fun({FieldName, FieldValue}, Acc2) ->
                {ok, Terms} = analyze_field(FieldName, FieldValue, Schema),
                [{FieldName, FieldValue, get_term_positions(Terms)} | Acc2]
        end,
    NewFields = lists:foldl(F1, [], RegularFields),


    F2 = fun({FieldName, FieldValue}, Acc2) ->
                {ok, Terms} = analyze_field(FieldName, FieldValue, Schema),
                Terms1 = lists:usort(Terms),
                [{FieldName, FieldValue, Terms1} | Acc2]
        end,
    NewInlineFields = lists:foldl(F2, [], Inlines),

    %% For each Inline = {FieldName, FieldValue, _}, split the FieldValue
    %% into terms and build a list of positions for those terms.
    IdxDoc#riak_idx_doc{ fields=NewFields, inline_fields=NewInlineFields, analyzed_flag=true }.

%% Normalize the list of input fields against the schema
%% - drop any skip fields
%% - replace any aliased fields with the correct name
%% - combine duplicate field names into a single field (separate by spaces)
normalize_fields(DocFields, Schema) ->
    Fun = fun({InFieldName, FieldValue}, {Regular, Inlines}) when is_binary(InFieldName), is_binary(FieldValue) ->
                  FieldDef = Schema:find_field(InFieldName),
                  case Schema:is_skip(FieldDef) of
                      true ->
                          {Regular, Inlines};
                      false ->
                          %% Create the field. Use an empty list
                          %% placeholder for term positions. This gets
                          %% filled when we analyze the document.
                          NormFieldName = normalize_field_name(InFieldName, FieldDef, Schema),
                          NormFieldValue = FieldValue,
                          Field = {NormFieldName, NormFieldValue, []},
                          %% If 'inline' is set to false, then store
                          %% as a regular field. If inline is set to
                          %% 'true' then store as both a regular AND
                          %% an inline field. If field is set to
                          %% 'only' then store as only an inline
                          %% field.
                          case Schema:field_inline(FieldDef) of
                              false ->
                                  {[Field|Regular], Inlines};
                              true ->
                                  {[Field|Regular], [Field|Inlines]};
                              only ->
                                  {Regular, [Field|Inlines]}
                          end
                  end;
             ({InFieldName, FieldValue}, _) ->
                  throw({expected_binaries, InFieldName, FieldValue})
          end,
    {RevRegular, RevInlines} = lists:foldl(Fun, {[], []}, DocFields),

    %% Aliasing makes it possible to have multiple entries in
    %% RevRegular.  Combine multiple entries for the same field name
    %% into a single field.
    {merge_fields(lists:reverse(RevRegular)),
     merge_fields(lists:reverse(RevInlines))}.

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
merge_fields_folder({FieldName, NewFieldData, NewTermPos}, [{FieldName, FieldData, TermPos} | Fields]) ->
    Field = {FieldName, <<FieldData/binary, " ", NewFieldData/binary>>, TermPos ++ NewTermPos},
    [Field | Fields];
merge_fields_folder(New, Fields) ->
    [New | Fields].


%% @private
%% Parse a FieldValue into a list of terms.
%% Return {ok, [Terms}}.
analyze_field(FieldName, FieldValue, Schema) ->
    %% Get the field...
    Field = Schema:find_field(FieldName),
    AnalyzerFactory = Schema:analyzer_factory(Field),
    AnalyzerArgs = Schema:analyzer_args(Field),
    %% Analyze the field...
    riak_search:analyze(FieldValue, AnalyzerFactory, AnalyzerArgs).


%% @private Given a list of tokens, build a gb_tree mapping words to a
%% list of word positions.
get_term_positions(Terms) ->
    %% Use a table to accumulate a list of term positions.
    Table = ets:new(positions, [duplicate_bag]),
    F1 = fun(skip, Pos) ->
                Pos + 1;
            (Term, Pos) ->
                ets:insert(Table, [{Term, Pos}]),
                Pos + 1
        end,
    lists:foldl(F1, 0, Terms),

    %% Look up the keys for the table...
    F2 = fun(Term) ->
                 {Term, [Pos || {_, Pos} <- ets:lookup(Table, Term)]}
         end,
    Keys = riak_search_utils:ets_keys(Table),
    Positions = [F2(X) || X <- Keys],

    %% Delete the table and return.
    ets:delete(Table),
    Positions.

%% @private
%% Given a term and a list of positions, generate a list of
%% properties.
build_props(Positions, Inlines) ->
    [{p, Positions}| Inlines].

%% Returns a Riak object.
get_obj(RiakClient, DocIndex, DocID) ->
    Bucket = idx_doc_bucket(DocIndex),
    Key = DocID,
    RiakClient:get(Bucket, Key).

%% Returns a #riak_idx_doc record.
get(RiakClient, DocIndex, DocID) ->
    case get_obj(RiakClient, DocIndex, DocID) of
        {ok, Obj} ->
            riak_object:get_value(Obj);
        Other ->
            Other
    end.

new_obj(DocIndex, DocID) ->
    Bucket = idx_doc_bucket(DocIndex),
    Key = DocID,
    riak_object:new(Bucket, Key, undefined).

%% Write the object to Riak.
put(RiakClient, IdxDoc) ->
    put(RiakClient, IdxDoc, []).

put(RiakClient, IdxDoc, Opts) ->
    DocIndex = index(IdxDoc),
    DocID = id(IdxDoc),
    Bucket = idx_doc_bucket(DocIndex),
    Key = DocID,
    case RiakClient:get(Bucket, Key) of
        {ok, Obj} ->
            DocObj = riak_object:update_value(Obj, IdxDoc);
        {error, notfound} ->
            DocObj = riak_object:new(Bucket, Key, IdxDoc)
    end,
    RiakClient:put(DocObj, Opts).

put_obj(RiakClient, RiakObj) ->
    RiakClient:put(RiakObj).

delete(RiakClient, IdxDoc) ->
    delete(RiakClient, index(IdxDoc), id(IdxDoc)).

delete(RiakClient, DocIndex, DocID) ->
    DocBucket = idx_doc_bucket(DocIndex),
    DocKey = DocID,
    case RiakClient:delete(DocBucket, DocKey) of
        ok -> ok;
        {error, notfound} -> ok;
        Other -> Other
    end.

%% Remove any old index entries if they exist
%% -spec remove_old_entries(riak_client(), search_client(), index(), docid()) -> ok.
remove_entries(RiakClient, SearchClient, Index, DocId) ->
    case riak_indexed_doc:get(RiakClient, Index, DocId) of
        {error, notfound} ->
            ok;
        OldIdxDoc ->
            SearchClient:delete_doc_terms(OldIdxDoc)
    end.

idx_doc_bucket(Bucket) when is_binary(Bucket) ->
    <<"_rsid_", Bucket/binary>>.

-ifdef(TEST).

normalize_fields_test() ->
    SchemaProps = [{version, 1},{default_field, <<"afield">>}],
    FieldDefs =  [{field, [{name, <<"skipme">>},
                           {alias, <<"skipmetoo">>},
                           skip]},
                  {field, [{name, <<"afield">>},
                           {alias, <<"afieldtoo">>}]},
                  {field, [{name, <<"anotherfield">>},
                           {alias, <<"anotherfieldtoo">>}]},
                  {field, [{name, <<"inline">>},
                           {alias, <<"inlinetoo">>},
                           inline]},
                  {field, [{name, <<"anotherinline">>},
                           {alias, <<"anotherinlinetoo">>},
                           {inline, true}]}],

    SchemaDef = {schema, SchemaProps, FieldDefs},
    {ok, Schema} = riak_search_schema_parser:from_eterm(<<"is_skip_test">>, SchemaDef),

    ?assertEqual({[], []},
                 normalize_fields([], Schema)),

    ?assertEqual({[{<<"afield">>,<<"data">>, []}], []},
                 normalize_fields([{<<"afield">>,<<"data">>}], Schema)),

    ?assertEqual({[{<<"afield">>,<<"data">>, []}], []},
                 normalize_fields([{<<"afieldtoo">>,<<"data">>}], Schema)),

    ?assertEqual({[{<<"afield">>,<<"one two three">>, []}], []},
                 normalize_fields([{<<"afieldtoo">>,<<"one">>},
                                   {<<"afield">>,<<"two">>},
                                   {<<"afieldtoo">>, <<"three">>}], Schema)),

    {Fields, InlineFields} =
        normalize_fields([{<<"anotherfield">>,<<"abc">>},
                          {<<"afieldtoo">>,<<"one">>},
                          {<<"skipme">>,<<"skippable terms">>},
                          {<<"anotherfieldtoo">>,<<"def">>},
                          {<<"afield">>,<<"two">>},
                          {<<"skipmetoo">>,<<"not needed">>},
                          {<<"anotherfield">>,<<"ghi">>},
                          {<<"afieldtoo">>,<<"three">>},
                          {<<"inline">>,<<"first">>},
                          {<<"inlinetoo">>,<<"second">>}], Schema),

    ?assert(lists:member({<<"anotherfield">>, <<"abc def ghi">>, []}, Fields)),
    ?assert(lists:member({<<"afield">>,<<"one two three">>, []}, Fields)),
    ?assert(lists:member({<<"inline">>, <<"first second">>, []}, InlineFields)).

-endif. % TEST
