-module(riak_solr_schema, [Name, Version, DefaultField, FieldsAndFacets, DefaultOp]).

-include_lib("riak_solr/include/riak_solr.hrl").

-export([
         %% Properties...
         name/0, 
         version/0, 
         fields_and_facets/0,
         facets/0,
         fields/0, 
         default_field/0, 
         default_op/0, 

         %% Field properties...
         field_name/1,
         field_type/1,
         is_field_required/1,
         is_field_facet/1,
         
         %% Field lookup
         find_field_or_facet/1,
         find_field/1,
         find_facet/1,
         
         %% Validation
         validate_commands/1
        ]).

name() ->
    Name.

version() ->
    Version.

fields_and_facets() ->
    FieldsAndFacets.

fields() ->
    [X || X <- FieldsAndFacets, X#riak_solr_field.facet == false].

facets() ->
    [X || X <- FieldsAndFacets, X#riak_solr_field.facet == true].

default_field() ->
    DefaultField.

default_op() ->
    DefaultOp.

field_name(Field) ->
    Field#riak_solr_field.name.

field_type(Field) ->
    Field#riak_solr_field.type.

is_field_required(Field) ->
    Field#riak_solr_field.required == true.

is_field_facet(Field) ->
    Field#riak_solr_field.facet == true.

%% Return the field or facet matching the specified name, or 'undefined'.
find_field_or_facet(FName) ->
    case lists:keyfind(FName, #riak_solr_field.name, FieldsAndFacets) of
        Field when is_record(Field, riak_solr_field) -> 
            Field;
        false -> 
            undefined
        end.

%% Return the field matching the specified name, or 'undefined'
find_field(FName) ->
    Field = find_field_or_facet(FName),
    case Field#riak_solr_field.facet /= true of
        true  -> Field;
        false -> undefiend
    end.

%% Return the facet matching the specified name, or 'undefined'
find_facet(FName) ->
    Field = find_field_or_facet(FName),
    case Field#riak_solr_field.facet == true of
        true  -> Field;
        false -> undefiend
    end.

%% Verify that the schema names match. If so, then validate required
%% and optional fields. Otherwise, return an error.
validate_commands(Commands) ->
    Docs = proplists:get_value(docs, Commands),
    Required = [F || F <- FieldsAndFacets, F#riak_solr_field.required =:= true],
    validate_required_fields(Docs, Required).

%% @private
%% Validate for required fields in the list of documents.
%% Return 'ok', or an {error, Error} if a field is missing.
validate_required_fields([Doc|Rest], Required) ->
    case validate_required_fields_1(Doc, Required) of
        ok -> validate_required_fields(Rest, Required);
        Other -> Other
    end.

%% @private
%% Validate required fields in a single document. 
%% Return 'ok' or an {error, Error} if a field is missing.
validate_required_fields_1(Doc, []) -> 
    ok;
validate_required_fields_1(Doc, [Field|Rest]) ->
    FieldName = Field#riak_solr_field.name,
    case list:keyfind(FieldName, 1, Doc) of
        Value when Value /= false ->
            validate_required_fields_1(Doc, Rest);
        false ->
            {error, {reqd_field_missing, FieldName}}
    end.
