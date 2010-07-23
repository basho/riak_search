%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_schema_parser).
-export([from_eterm/2]).

-include_lib("qilr/include/qilr.hrl").
-include("riak_search.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Given an Erlang term (see riak_search/priv/search.def for example)
%% parse the term into a riak_search_schema parameterized module.
from_eterm(SchemaName, {schema, SchemaProps, FieldDefs}) ->
    %% Read fields...
    Version = proplists:get_value(version, SchemaProps),
    DefaultField = proplists:get_value(default_field, SchemaProps),
    SchemaAnalyzer = proplists:get_value(analyzer_factory, SchemaProps),
    DefaultOp = list_to_atom(proplists:get_value(default_op, SchemaProps, "or")),

    %% Verify that version is defined...
    Version /= undefined orelse
        throw({error, {malformed_schema, version, {schema, SchemaProps}}}),

    %% Verify that default field is defined...
    DefaultField /= undefined orelse
        throw({error, {malformed_schema, default_field, {schema, SchemaProps}}}),

    %% Verify that DefaultOp is either "and" or "or"...
    lists:member(DefaultOp, ['and', 'or']) orelse 
        throw({error, {malformed_schema, default_op, {schema, SchemaProps}}}),

    {ok, Fields} = parse_fields(FieldDefs, SchemaAnalyzer, []),
    {ok, riak_search_schema:new(SchemaName, Version, DefaultField, Fields, 
                                DefaultOp, SchemaAnalyzer)}.


parse_fields([], _SchemaAnalyzer, Fields) ->
    {ok, lists:reverse(Fields)};
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
    FieldAnalyzerArgs = proplists:get_value(analyzer_args, FieldProps, undefined),
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
    Field0 = #riak_search_field {
      name=NewName, 
      type=Type, 
      padding_size=PaddingSize,
      padding_char=normalize_padding_char(PaddingChar, Name),
      required=IsRequired, 
      dynamic=IsDynamic, 
      analyzer_factory=FieldAnalyzer,
      analyzer_args=FieldAnalyzerArgs,
      facet=Facet
     },
    Field = Field0#riak_search_field{analyzer_args = calculate_analyzer_args(Field0)},
 
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
        true ->
            ?INTEGER_ANALYZER;
        _ ->
            SchemaAnalyzer
    end.

%% Return true if this is a type we know and love.
valid_type(string)  -> true;
valid_type(integer) -> true;
valid_type(date)    -> true;
valid_type(_Other)  -> false.

%% Single char string
normalize_padding_char([Char], _Field) when is_integer(Char) ->
    Char;
%% $0 type entry
normalize_padding_char(Char, _Field) when is_integer(Char) ->
    Char;
normalize_padding_char(_BadValue, Field) ->
    throw({error, {bad_padding_char, Field}}).

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

%% Calculate the arguments to send across to qilr for the analyzer_factory
calculate_analyzer_args(Field=#riak_search_field{analyzer_args=Args}) when 
      Args =/= undefined ->
    case is_list(Args) andalso lists:all(fun is_string/1, Args) of
        true ->
            Args;
        false ->
            throw({error, {analyzer_args_must_be_strings, Field#riak_search_field.name}})
    end;
calculate_analyzer_args(Field) ->
    case Field#riak_search_field.analyzer_factory of
        ?INTEGER_ANALYZER ->
            case Field#riak_search_field.padding_char of
                $0 ->
                    [integer_to_list(Field#riak_search_field.padding_size)];
                _ ->
                    throw({error, {integer_fields_only_pads_with_zeros, 
                                   Field#riak_search_field.name}})
            end;
        _ ->
            undefined
    end.

is_string([]) ->
    true;
is_string([C|T]) when is_integer(C), C >= 0, C =< 255 ->
    is_string(T);
is_string(_) ->
    false.

-ifdef(TEST).

is_string_test() ->
    ?assertEqual(true, is_string("")),
    ?assertEqual(true, is_string("a")),
    ?assertEqual(true, is_string("a b c")),
    ?assertEqual(false, is_string(undefined)),
    ?assertEqual(false, is_string(["nested string"])),
    ?assertEqual(false, is_string([0, 256])), % char out of range
    ?assertEqual(false, is_string(<<"binaries are not strings">>)).

calculate_analyzer_args_test() ->
    ZeroArgs=#riak_search_field{analyzer_args=[]},
    ?assertEqual([], calculate_analyzer_args(ZeroArgs)),

    OneArgs=#riak_search_field{analyzer_args=["abc"]},
    ?assertEqual(["abc"], calculate_analyzer_args(OneArgs)),

    TwoArgs=#riak_search_field{analyzer_args=["abc","123"]},
    ?assertEqual(["abc","123"], calculate_analyzer_args(TwoArgs)),


    ?assertThrow({error, {analyzer_args_must_be_strings, _}},
        calculate_analyzer_args(#riak_search_field{analyzer_args=atom})),

    ?assertThrow({error, {analyzer_args_must_be_strings, _}},
        calculate_analyzer_args(#riak_search_field{analyzer_args=[atomlist]})),

    ?assertThrow({error, {analyzer_args_must_be_strings, _}},
        calculate_analyzer_args(#riak_search_field{analyzer_args="barestr"})),

    ?assertThrow({error, {analyzer_args_must_be_strings, _}},
        calculate_analyzer_args(#riak_search_field{analyzer_args=123})),

    ?assertThrow({error, {analyzer_args_must_be_strings, _}},
        calculate_analyzer_args(#riak_search_field{analyzer_args= <<"bin">>})).
    
normalize_padding_char_test() ->
    ?assertEqual($0, normalize_padding_char($0, fld)),
    ?assertEqual($0, normalize_padding_char("0", fld)),

    ?assertThrow({error, {bad_padding_char, _}},
                 normalize_padding_char(a, fld)),
    ?assertThrow({error, {bad_padding_char, fld}}, 
                 normalize_padding_char(<<"0">>, fld)),
    ?assertThrow({error, {bad_padding_char, fld}}, 
                 normalize_padding_char("00", fld)).
    


-endif. % TEST
