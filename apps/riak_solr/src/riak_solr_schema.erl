-module(riak_solr_schema).

-include_lib("riak_solr/include/riak_solr.hrl").

-export([new_schemadef/1]).

new_schemadef({schema, SchemaProps, FieldDefs}) ->
    Name = proplists:get_value(name, SchemaProps),
    Version = proplists:get_value(version, SchemaProps),
    case Name =:= undefined orelse Version =:= undefined of
        true ->
            {error, {malformed_schema, {schema, SchemaProps}}};
        false ->
            parse_fields(#riak_solr_schemadef{name=Name,
                                              version=Version}, FieldDefs)
    end.

%% Internal functions
parse_fields(Schema, []) ->
    Schema;
parse_fields(Schema, {fields, Fields}) ->
    parse_fields(Schema, Fields);
parse_fields(Schema, [{field, FieldProps}=Field0|T]) ->
    Name = proplists:get_value(name, FieldProps),
    Type = proplists:get_value(type, FieldProps, string),
    Reqd = proplists:get_value(required, FieldProps, false),
    case valid_type(Type) of
        ok ->
            if
                Name =:= undefined ->
                    {error, {missing_field_name, Field0}};
                true ->
                    parse_fields(add_field(Schema, Name, Type, Reqd), T)
            end;
        Error ->
            Error
    end.

add_field(#riak_solr_schemadef{fields=Fields}=Schema, Name, Type, Reqd) ->
    Field = #riak_solr_field{name=Name,
                             type=Type,
                             required=not(Reqd =:= false)},
    Schema#riak_solr_schemadef{fields=dict:store(Name, Field, Fields)}.

valid_type(string) ->
    ok;
valid_type(boolean) ->
    ok;
valid_type(integer) ->
    ok;
valid_type(float) ->
    ok;
valid_type(Type) ->
    {error, {bad_field_type, Type}}.
