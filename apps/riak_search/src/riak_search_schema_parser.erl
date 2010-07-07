-module(riak_search_schema_parser).
-export([from_eterm/2]).

-include_lib("qilr/include/qilr.hrl").
-include("riak_search.hrl").

%% Given an Erlang term (see riak_search/priv/search.def for example)
%% parse the term into a riak_search_schema parameterized module.
from_eterm(SchemaName, {schema, SchemaProps, FieldDefs}) ->
    %% Read fields...
    Version = proplists:get_value(version, SchemaProps),
    DefaultField = proplists:get_value(default_field, SchemaProps),
    SchemaAnalyzer = proplists:get_value(analyzer_factory, SchemaProps),
    DefaultOp = proplists:get_value(default_op, SchemaProps, "or"),

    %% Verify that version is defined...
    Version /= undefined orelse
        throw({error, {malformed_schema, version, {schema, SchemaProps}}}),

    %% Verify that default field is defined...
    DefaultField /= undefined orelse
        throw({error, {malformed_schema, default_field, {schema, SchemaProps}}}),

    %% Verify that DefaultOp is either "and" or "or"...
    lists:member(DefaultOp, ["and", "or"]) orelse 
        throw({error, {malformed_schema, default_op, {schema, SchemaProps}}}),

    {ok, Fields} = parse_fields(FieldDefs, SchemaAnalyzer, []),
    {ok, riak_search_schema:new(SchemaName, Version, DefaultField, Fields, 
                                DefaultOp, SchemaAnalyzer)}.


parse_fields([], _SchemaAnalyzer, Fields) ->
    {ok, Fields};
parse_fields({fields, Fields}, SchemaAnalyzer, Fields) ->
    parse_fields(Fields, SchemaAnalyzer, Fields);
parse_fields([{FieldClass, FieldProps}|T], SchemaAnalyzer, Fields) 
when FieldClass == field orelse FieldClass == dynamic_field ->
    %% Read fields...
    IsDynamic = (FieldClass == dynamic_field),
    Name = proplists:get_value(name, FieldProps),
    Type = proplists:get_value(type, FieldProps, string),
    IsRequired = (not IsDynamic) andalso (proplists:get_value(required, FieldProps, false) == true),
    DefaultPaddingSize = get_default_padding_size(Type),
    PaddingSize = proplists:get_value(padding_size, FieldProps, DefaultPaddingSize),
    DefaultPaddingChar = get_default_padding_char(Type),
    PaddingChar = proplists:get_value(padding_char, FieldProps, DefaultPaddingChar),
    DefaultFieldAnalyzer = get_default_field_analyzer(Type, SchemaAnalyzer),
    FieldAnalyzer = proplists:get_value(analyzer_factory, FieldProps, DefaultFieldAnalyzer),
    Facet = proplists:get_value(facet, FieldProps, false),

    %% Verify that name exists...
    Name /= undefined orelse
        throw({error, {malformed_schema, name, FieldProps}}),

    %% Verify type...
    valid_type(Type) orelse
        throw({error, {malformed_schema, type, FieldProps}}),

    %% Calculate Name...
    case FieldClass of 
        field  -> 
            NewName = Name;
        dynamic_field ->
            NewName = calculate_name_pattern(Name)
    end,

    %% Create the field...
    Field = #riak_search_field {
      name=NewName, 
      type=Type, 
      padding_size=PaddingSize,
      padding_char=PaddingChar,
      required=IsRequired, 
      dynamic=IsDynamic, 
      analyzer_factory=FieldAnalyzer, 
      facet=Facet
     },

    %% Add the field...
    parse_fields(T, SchemaAnalyzer, [Field|Fields]).

get_default_padding_size(Type) ->
    case Type == integer orelse Type == date of
        true -> 10;
        _    -> 0
    end.

get_default_padding_char(Type) ->
    case Type == integer orelse Type == date of
        true -> $0;
        _    -> $\s
    end.

get_default_field_analyzer(Type, SchemaAnalyzer) ->
    case Type == integer orelse Type == date of
        true  -> ?WHITESPACE_ANALYZER;
        false -> SchemaAnalyzer
    end.

%% Return true if this is a type we know and love.
valid_type(string)  -> true;
valid_type(integer) -> true;
valid_type(date)    -> true;
valid_type(_Other)  -> false.

%% A name pattern can have one (and only one) wildcard. If we find a
%% wildcard, replace it with a the regex ".*" So
calculate_name_pattern(Name) ->
    MaxOneWildcard = string:chr(Name, $*) == string:chr(Name, $*),
    case MaxOneWildcard of
        true -> 
            calculate_name_pattern_1(Name);
        false -> 
            throw({error, {bad_dynamic_name, Name}})
    end.

%% Replace "*" with ".*" in a string.
calculate_name_pattern_1([]) -> 
    [];
calculate_name_pattern_1([$*|T]) -> 
    [$.,$*|calculate_name_pattern_1(T)];
calculate_name_pattern_1([H|T]) -> 
    [H|calculate_name_pattern_1(T)].
