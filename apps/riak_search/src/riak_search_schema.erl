%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_schema, [Name, Version, NVal, DefaultField, UniqueKey, FieldsAndFacets, DefaultOp, AnalyzerFactory]).
-export([
    %% Properties...
    name/0,
    set_name/1,
    version/0,
    n_val/0,
    fields_and_facets/0,
    facets/0,
    fields/0,
    unique_key/0,
    default_field/0,
    set_default_field/1,
    default_op/0,
    analyzer_factory/0,
    set_default_op/1,

    %% Field properties...
    field_name/1,
    field_type/1,
    padding_size/1,
    padding_char/1,
    is_dynamic/1,
    is_field_required/1,
    analyzer_factory/1,
    analyzer_args/1,
    is_field_facet/1,
    is_skip/1,
    aliases/1,
    %% field_types/0,

    %% Field lookup
    find_field/1,

    %% Validation
    validate_commands/2
]).

-include("riak_search.hrl").

name() ->
    riak_search_utils:to_binary(Name).

set_name(NewName) ->
    ?MODULE:new(NewName, Version, NVal, DefaultField, UniqueKey, FieldsAndFacets, DefaultOp, AnalyzerFactory).

version() ->
    Version.

n_val() ->
    NVal.

fields_and_facets() ->
    FieldsAndFacets.

fields() ->
    [X || X <- FieldsAndFacets, X#riak_search_field.facet == false].

facets() ->
    [X || X <- FieldsAndFacets, X#riak_search_field.facet == true].

unique_key() ->
    UniqueKey.

default_field() ->
    DefaultField.

analyzer_factory() when is_list(AnalyzerFactory) ->
    list_to_binary(AnalyzerFactory);
analyzer_factory() ->
    AnalyzerFactory.

set_default_field(NewDefaultField) ->
    ?MODULE:new(Name, Version, NVal, NewDefaultField, UniqueKey, FieldsAndFacets, DefaultOp, AnalyzerFactory).

default_op() ->
    DefaultOp.

set_default_op(NewDefaultOp) ->
    ?MODULE:new(Name, Version, NVal, DefaultField, UniqueKey, FieldsAndFacets, NewDefaultOp, AnalyzerFactory).

field_name(Field) ->
    Field#riak_search_field.name.

field_type(Field) ->
    Field#riak_search_field.type.

padding_size(Field) ->
    Field#riak_search_field.padding_size.

padding_char(Field) ->
    Field#riak_search_field.padding_char.

is_dynamic(Field) ->
    Field#riak_search_field.dynamic == true.

is_field_required(Field) ->
    Field#riak_search_field.required == true.

analyzer_factory(Field) ->
    Field#riak_search_field.analyzer_factory.

analyzer_args(Field) ->
    Field#riak_search_field.analyzer_args.

is_field_facet(Field) ->
    Field#riak_search_field.facet == true.

is_skip(Field) ->
    Field#riak_search_field.skip == true.

aliases(Field) ->
    [element(2, T) || T <- Field#riak_search_field.aliases].

%% Return the field matching the specified name, or 'undefined'
find_field(FName) ->
    find_field(FName, FieldsAndFacets).

find_field(FName, []) ->
    throw({error, missing_field, FName});
find_field(FName, [Field|Fields]) ->
    case Field#riak_search_field.dynamic of
        true ->
            case re:run(FName, Field#riak_search_field.name) of
                {match, _} ->
                    Field;
                nomatch ->
                    find_field(FName, Fields)
            end;
        false ->
            case FName == Field#riak_search_field.name orelse 
                matches_alias(FName, Field#riak_search_field.aliases) of
                true ->
                    Field;
                false ->                 
                    find_field(FName, Fields)
            end
    end.

%% Return true if the name matches an alias
matches_alias(_FName, []) ->
    false;
matches_alias(FName, [{exact, Alias}|Aliases]) ->
    case FName of
        Alias ->
            true;
        _ ->
            matches_alias(FName, Aliases)
    end;
matches_alias(FName, [{re, _Alias, MP}|Aliases]) ->
    case re:run(FName, MP) of
        {match, _} ->
            true;
        nomatch ->
            matches_alias(FName, Aliases)
    end.

%% Verify that the schema names match. If so, then validate required
%% and optional fields. Otherwise, return an error.
validate_commands(add, Docs) ->
    Required = [F || F <- FieldsAndFacets, F#riak_search_field.required =:= true],
    validate_required_fields(Docs, Required);
validate_commands(_, _) ->
    ok.

%% @private
%% Validate for required fields in the list of documents.
%% Return 'ok', or an {error, Error} if a field is missing.
validate_required_fields([], _) ->
    ok;
validate_required_fields([Doc|Rest], Required) ->
    case validate_required_fields_1(Doc, Required) of
        ok -> validate_required_fields(Rest, Required);
        Other -> Other
    end.

%% @private
%% Validate required fields in a single document.
%% Return 'ok' or an {error, Error} if a field is missing.
validate_required_fields_1(_, []) ->
    ok;
validate_required_fields_1(Doc, [Field|Rest]) ->
    FieldName = Field#riak_search_field.name,
    case dict:find(FieldName, Doc) of
        {ok, _Value} ->
            validate_required_fields_1(Doc, Rest);
        error ->
            {error, {reqd_field_missing, FieldName}}
    end.

              
