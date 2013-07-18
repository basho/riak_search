%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_schema).
-export([
    new/8,
    %% Properties...
    name/1,
    set_name/2,
    version/1,
    n_val/1,
    fields_and_inlines/1,
    inline_fields/1,
    fields/1,
    unique_key/1,
    default_field/1,
    set_default_field/2,
    default_op/1,
    analyzer_factory/1,
    set_default_op/2,

    %% Field properties...
    field_name/2,
    field_type/2,
    padding_size/2,
    padding_char/2,
    is_dynamic/2,
    is_field_required/2,
    analyzer_factory/2,
    analyzer_args/2,
    field_inline/2,
    is_skip/2,
    aliases/2,

    %% Field lookup
    find_field/2,

    %% Validation
    validate_commands/3
]).

-include("riak_search.hrl").

new(Name, Version, NVal, DefaultField, UniqueKey, Fields, DefaultOp, AnalyzerFactory) ->
    {?MODULE, [Name,Version,NVal,DefaultField,UniqueKey,Fields,DefaultOp,AnalyzerFactory]}.

name({?MODULE, [Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    riak_search_utils:to_binary(Name).

set_name(NewName, {?MODULE, [_Name,Version,NVal,DefaultField,UniqueKey,Fields,DefaultOp,AnalyzerFactory]}) ->
    ?MODULE:new(NewName, Version, NVal, DefaultField, UniqueKey, Fields, DefaultOp, AnalyzerFactory).

version({?MODULE, [_Name,Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Version.

n_val({?MODULE, [_Name,_Version,NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    NVal.

fields_and_inlines({?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Fields.

fields({?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,Fields,_DefaultOp,_AnalyzerFactory]}) ->
    [X || X <- Fields, X#riak_search_field.inline == false].

inline_fields({?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,Fields,_DefaultOp,_AnalyzerFactory]}) ->
    [X || X <- Fields, X#riak_search_field.inline == true].

unique_key({?MODULE, [_Name,_Version,_NVal,_DefaultField,UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    UniqueKey.

default_field({?MODULE, [_Name,_Version,_NVal,DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    DefaultField.

analyzer_factory({?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,AnalyzerFactory]})
  when is_list(AnalyzerFactory) ->
    list_to_binary(AnalyzerFactory);
analyzer_factory({?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,AnalyzerFactory]}) ->
    AnalyzerFactory.

set_default_field(NewDefaultField, {?MODULE, [Name,Version,NVal,_DefaultField,UniqueKey,Fields,DefaultOp,AnalyzerFactory]}) ->
    ?MODULE:new(Name, Version, NVal, NewDefaultField, UniqueKey, Fields, DefaultOp, AnalyzerFactory).

default_op({?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,DefaultOp,_AnalyzerFactory]}) ->
    DefaultOp.

set_default_op(NewDefaultOp, {?MODULE, [Name,Version,NVal,DefaultField,UniqueKey,Fields,_DefaultOp,AnalyzerFactory]}) ->
    ?MODULE:new(Name, Version, NVal, DefaultField, UniqueKey, Fields, NewDefaultOp, AnalyzerFactory).

field_name(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.name.

field_type(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.type.

padding_size(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.padding_size.

padding_char(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.padding_char.

is_dynamic(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.dynamic == true.

is_field_required(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.required == true.

analyzer_factory(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.analyzer_factory.

analyzer_args(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.analyzer_args.

field_inline(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.inline.

is_skip(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Field#riak_search_field.skip == true.

aliases(Field, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}) ->
    [element(2, T) || T <- Field#riak_search_field.aliases].

%% Return the field matching the specified name, or 'undefined'
find_field(FName, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,Fields,_DefaultOp,_AnalyzerFactory]}=THIS) ->
    find_field(FName, Fields, THIS).

find_field(FName, [Field|Rest], {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,_Fields,_DefaultOp,_AnalyzerFactory]}=THIS) ->
    case Field#riak_search_field.dynamic of
        true ->
            case re:run(FName, Field#riak_search_field.name) of
                {match, _} ->
                    Field;
                nomatch ->
                    find_field(FName, Rest, THIS)
            end;
        false ->
            case FName == Field#riak_search_field.name orelse
                matches_alias(FName, Field#riak_search_field.aliases) of
                true ->
                    Field;
                false ->
                    find_field(FName, Rest, THIS)
            end
    end;
find_field(FName, [], _) ->
    throw({error, missing_field, FName}).

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
validate_commands(add, Docs, {?MODULE, [_Name,_Version,_NVal,_DefaultField,_UniqueKey,Fields,_DefaultOp,_AnalyzerFactory]}) ->
    Required = [F || F <- Fields, F#riak_search_field.required =:= true],
    validate_required_fields(Docs, Required);
validate_commands(_, _, _) ->
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
