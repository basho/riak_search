-module(riak_search_schema_parser).
-export([from_eterm/1]).

-include("riak_search.hrl").

from_eterm({schema, SchemaProps, FieldDefs}) ->
    Name = proplists:get_value(name, SchemaProps),
    Version = proplists:get_value(version, SchemaProps),
    DefaultField = proplists:get_value(default_field, SchemaProps),
    AnalyzerFactory = proplists:get_value(analyzer_factory, SchemaProps),
    DefaultOp = case proplists:get_value(default_op, SchemaProps, "or") of
                    Op when Op =:= "and" orelse Op =:= "or" ->
                        Op;
                    _ ->
                        {error, {malformed_schema, {schema, SchemaProps}}}
                end,
    case DefaultOp of
        {error, _} ->
            DefaultOp;
        _ ->
            case Name =:= undefined orelse Version =:= undefined orelse
                DefaultField =:= undefined of
                true ->
                    {error, {malformed_schema, {schema, SchemaProps}}};
                false ->
                    {DynFields, Fields} = parse_fields(FieldDefs, [], []),
                    {ok, riak_search_schema:new(Name, Version, DefaultField, Fields,
                                                DynFields, DefaultOp, AnalyzerFactory)}
            end
    end.


parse_fields([], DynAccum, Accum) ->
    {DynAccum, Accum};
parse_fields({fields, Fields}, DynAccum, Accum) ->
    parse_fields(Fields, DynAccum, Accum);
parse_fields([{dynamic_field, FieldProps}=Field0|T], DynAccum, Accum) ->
    Name = proplists:get_value(name, FieldProps),
    Type = proplists:get_value(type, FieldProps),
    case valid_type(Type) of
        ok ->
            if
                Name =:= undefined ->
                    {error, {missing_field_name, Field0}};
                true ->
                    NamePattern = case name_type(Name) of
                                      all ->
                                          ".*";
                                      suffix ->
                                          [$.|Name];
                                      prefix ->
                                          [N] = string:tokens(Name, "*"),
                                          N ++ ".*"
                                  end,
                    F = #riak_search_field{name=NamePattern, type=Type, required=false, dynamic=true, facet=false},
                    parse_fields(T, [F|DynAccum], Accum)
            end;
        Error ->
            Error
    end;
parse_fields([{field, FieldProps}=Field0|T], DynAccum, Accum) ->
    Name = proplists:get_value(name, FieldProps),
    Type = proplists:get_value(type, FieldProps, string),
    Reqd = proplists:get_value(required, FieldProps, false) /= false,
    Facet = proplists:get_value(facet, FieldProps, false) /= false,
    case valid_type(Type) of
        ok ->
            if
                Name =:= undefined ->
                    {error, {missing_field_name, Field0}};
                true ->
                    F = #riak_search_field{name=Name, type=Type, required=Reqd, facet=Facet },
                    parse_fields(T, DynAccum, [F|Accum])
            end;
        Error ->
            Error
    end.

valid_type(string) ->
    ok;
valid_type(integer) ->
    ok;
valid_type(date) ->
    ok;
valid_type(Type) ->
    {error, {bad_field_type, Type}}.

name_type("*") ->
    all;
name_type(Name) ->
    EndOfName = length(Name),
    case string:chr(Name, $*) of
        1 ->
            suffix;
        EndOfName ->
            prefix;
        _ ->
            throw({error, {bad_dynamic_name, Name}})
    end.
