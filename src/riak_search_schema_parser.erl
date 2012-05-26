%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_schema_parser).
-export([
         from_eterm/2
        ]).

-include("riak_search.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(riak_search_utils, [to_list/1, to_atom/1, to_binary/1, to_boolean/1, to_integer/1]).


%% Given an Erlang term (see riak_search/priv/search.def for example)
%% parse the term into a riak_search_schema parameterized module.
from_eterm(SchemaName, {schema, SchemaProps, FieldDefs}) when is_binary(SchemaName) ->
    %% Read fields...
    Version = to_list(proplists:get_value(version, SchemaProps)),
    NVal = to_integer(proplists:get_value(n_val, SchemaProps, 3)),
    DefaultField = to_binary(proplists:get_value(default_field, SchemaProps)),
    UniqueKey = to_binary(proplists:get_value(unique_key, SchemaProps, "id")),
    SchemaAnalyzer = proplists:get_value(analyzer_factory, SchemaProps),
    DefaultOp = to_atom(proplists:get_value(default_op, SchemaProps, "or")),

    %% Verify that version is defined...
    Version /= undefined orelse
        throw({error, {malformed_schema, version, {schema, SchemaProps}}}),

    %% Verify that the unique key is defined...
    UniqueKey /= undefined orelse
        throw({error, {malformed_schema, unique_key, {schema, SchemaProps}}}),

    %% Verify that default field is defined...
    DefaultField /= undefined orelse
        throw({error, {malformed_schema, default_field, {schema, SchemaProps}}}),

    %% Verify that DefaultOp is either "and" or "or"...
    lists:member(DefaultOp, ['and', 'or']) orelse
        throw({error, {malformed_schema, default_op, {schema, SchemaProps}}}),

    {ok, Fields} = parse_fields(FieldDefs, SchemaAnalyzer, []),
    {ok, riak_search_schema:new(SchemaName, Version, NVal, DefaultField, UniqueKey,
                                Fields, DefaultOp, SchemaAnalyzer)}.

parse_fields([], _SchemaAnalyzer, Fields) ->
    {ok, lists:reverse(Fields)};
parse_fields({fields, Fields}, SchemaAnalyzer, Fields) ->
    parse_fields(Fields, SchemaAnalyzer, Fields);
parse_fields([{FieldClass, FieldProps}|T], SchemaAnalyzer, Fields)
when FieldClass == field orelse FieldClass == dynamic_field ->
    %% Read fields...
    IsDynamic = (FieldClass == dynamic_field),
    Name = to_binary(proplists:get_value(name, FieldProps)),
    Type = to_atom(proplists:get_value(type, FieldProps, string)),
    IsRequired = (not IsDynamic) andalso (proplists:get_value(required, FieldProps, false) == true),
    IsSkip = to_boolean(proplists:get_value(skip, FieldProps, false)),
    Aliases = [to_binary(X) || X <- proplists:get_all_values(alias, FieldProps)],
    DefaultPaddingSize = to_integer(get_default_padding_size(Type)),
    PaddingSize = to_integer(proplists:get_value(padding_size, FieldProps, DefaultPaddingSize)),
    DefaultPaddingChar = get_default_padding_char(Type),
    PaddingChar = proplists:get_value(padding_char, FieldProps, DefaultPaddingChar),
    DefaultFieldAnalyzer = get_default_field_analyzer(Type, SchemaAnalyzer),
    FieldAnalyzer = proplists:get_value(analyzer_factory, FieldProps, DefaultFieldAnalyzer),
    FieldAnalyzerArgs = proplists:get_value(analyzer_args, FieldProps, undefined),
    Inline = proplists:get_value(inline, FieldProps, false),

    %% Verify that name exists...
    Name /= undefined orelse
        throw({error, {malformed_schema, name, FieldProps}}),

    %% Verify type...
    valid_type(Type) orelse
        throw({error, {malformed_schema, type, FieldProps}}),

    %% Make sure no aliases on dynamic fields
    (IsDynamic == true andalso Aliases /= []) andalso
        throw({error, {malformed_schema, no_dynamic_field_aliases, FieldProps}}),

    %% Calculate Name...
    case FieldClass of
        field  ->
            NewName = Name;
        dynamic_field ->
            NewName = calculate_name_pattern_regex(Name)
    end,

    %% Create the field...
    Field0 = #riak_search_field {
      name=NewName,
      aliases=[calculate_alias_pattern(A) || A <- lists:usort(Aliases)],
      type=Type,
      padding_size=PaddingSize,
      padding_char=normalize_padding_char(PaddingChar, Name),
      required=IsRequired,
      dynamic=IsDynamic,
      skip=IsSkip,
      analyzer_factory=FieldAnalyzer,
      analyzer_args=FieldAnalyzerArgs,
      inline=Inline
     },
    NewAnalyzerArgs = calculate_analyzer_args(Field0),
    Field = Field0#riak_search_field{analyzer_args = NewAnalyzerArgs },

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
    if
        Type == integer ->
            ?INTEGER_ANALYZER;
        Type == date    ->
            ?NOOP_ANALYZER;
        true ->
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

%% Checks to see if an alias is exact or a wildcard (and if
%% it is a wildcard, precompile the pattern to match)
calculate_alias_pattern(Alias) ->
    IsWildcard = binary_contains($*, Alias),
    case IsWildcard of
        false ->
            {exact, Alias};
        true ->
            case re:compile(calculate_name_pattern_regex(Alias)) of
                {ok, MP} ->
                    {re, Alias, MP};
                {error, ErrSpec} ->
                    throw({error, {bad_alias_wildcard, {Alias, ErrSpec}}})
            end
    end.

binary_contains(H, <<H,_/binary>>) ->
    true;
binary_contains(H, <<_,T/binary>>) ->
    binary_contains(H, T);
binary_contains(_, <<>>) ->
    false.

%% A name pattern must have a wildcard. Check for it and
%% replace it with the regex ".*"
calculate_name_pattern_regex(Name) ->
    list_to_binary("^" ++ calculate_name_pattern_regex_1(Name) ++ "$").
calculate_name_pattern_regex_1(<<$*, T/binary>>) ->
    [$.,$*|calculate_name_pattern_regex_1(T)];
calculate_name_pattern_regex_1(<<H, T/binary>>) ->
    [H|calculate_name_pattern_regex_1(T)];
calculate_name_pattern_regex_1(<<>>) ->
    [].

%% Calculate the arguments for the analyzer_factory
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


bad_alias_regexp_test() ->
    SchemaProps = [{version, 1},{default_field, <<"field">>}],
    FieldDefs =  [{field, [{name, <<"badaliaswildre">>},
                           {alias, <<"[*">>}]}],
    SchemaDef = {schema, SchemaProps, FieldDefs},
    ?assertThrow({error, {bad_alias_wildcard,
                           {<<"[*">>,
                            {"missing terminating ] for character class",5}}}},
                 from_eterm(<<"bad_alias_regexp_test">>, SchemaDef)).

alias_on_dynamic_field_invalid_test() ->
    SchemaProps = [{version, 1},{default_field, <<"field">>}],
    FieldDefs =  [{dynamic_field, [{name, <<"field_*">>},
                                   {alias, <<"analias">>}]}],
    SchemaDef = {schema, SchemaProps, FieldDefs},
    ?assertThrow({error, {malformed_schema, no_dynamic_field_aliases, _FieldProps}},
                 from_eterm(<<"bad_alias_regexp_test">>, SchemaDef)).



-endif. % TEST
