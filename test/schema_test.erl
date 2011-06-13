%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%
%% riak_search_schema tests have to be in a separate module as
%% riak_search_schema is parameterized so it is impossible to 
%% create *_test/0 functions.
%% 
-module(schema_test).
-include_lib("eunit/include/eunit.hrl").

find_alias_test() ->
    SchemaProps = [{version, 1},{default_field, <<"field">>}],
    FieldDefs =  [{field, [{name, <<"noalias">>}]},
                  {field, [{name, <<"exactalias">>},
                           {alias, <<"anexactalias">>}]},
                  {field, [{name, <<"exactaliases">>},
                           {alias, <<"a1">>},
                           {alias, <<"a2">>}]},
                  {field, [{name, <<"regexpaliases">>},
                           {alias, <<"xa*">>},
                           {alias, <<"xb*">>},
                           {alias, <<"*_id">>}]},
                  {field, [{name, <<"xmlfield">>},
                           {alias, <<"tag1_tag2">>},
                           {alias, <<"tag1_tag2@*">>}]}],
    SchemaDef = {schema, SchemaProps, FieldDefs},
    {ok, Schema} = riak_search_schema_parser:from_eterm(<<"alias_test">>, SchemaDef),
    LookupFieldName = fun(Alias) -> Schema:field_name(Schema:find_field(Alias)) end,
    ?assertEqual(<<"noalias">>, LookupFieldName(<<"noalias">>)),
    
    ?assertEqual(<<"exactalias">>, LookupFieldName(<<"exactalias">>)), 
    ?assertEqual(<<"exactalias">>, LookupFieldName(<<"anexactalias">>)),
 
    ?assertEqual(<<"exactaliases">>, LookupFieldName(<<"exactaliases">>)), 
    ?assertEqual(<<"exactaliases">>, LookupFieldName(<<"a1">>)),
    ?assertEqual(<<"exactaliases">>, LookupFieldName(<<"a2">>)),

    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"regexpaliases">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"xa">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"xa1">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"xa123456789 dsfds78f9ds7f89s f">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"xb">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"xb1">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"xb123456789 dsfds78f9ds7f89s f">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"an_id">>)), 
    ?assertEqual(<<"regexpaliases">>, LookupFieldName(<<"another_id">>)), 
    
    ?assertThrow({error, missing_field, <<"notafield">>}, LookupFieldName(<<"notafield">>)).
    
 
alias_test() ->
    SchemaProps = [{version, 1},{default_field, <<"field">>}],
    FieldDefs =  [{field, [{name, <<"noalias">>}]},
                  {field, [{name, <<"aliases">>},
                           {alias, <<"anexactalias">>},
                           {alias, <<"wildcard*">>}]}],
    SchemaDef = {schema, SchemaProps, FieldDefs},
    {ok, Schema} = riak_search_schema_parser:from_eterm(<<"alias_test">>, SchemaDef),
    LookupAliases = fun(Alias) -> Schema:aliases(Schema:find_field(Alias)) end,
    ?assertEqual([], LookupAliases(<<"noalias">>)),
    ?assertEqual([<<"anexactalias">>, <<"wildcard*">>], LookupAliases(<<"aliases">>)).

is_skip_test() ->
    SchemaProps = [{version, 1},{default_field, <<"field">>}],
    FieldDefs =  [{field, [{name, <<"keepme1">>}]},
                  {field, [{name, <<"keepme2">>}, {skip, false}]},
                  {field, [{name, <<"skipme1">>}, skip]},
                  {field, [{name, <<"skipme2">>}, {skip, true}]}],
    SchemaDef = {schema, SchemaProps, FieldDefs},
    {ok, Schema} = riak_search_schema_parser:from_eterm(<<"is_skip_test">>, SchemaDef),
    IsSkip = fun(FName) -> Schema:is_skip(Schema:find_field(FName)) end,
    ?assertEqual(false, IsSkip(<<"keepme1">>)),
    ?assertEqual(false, IsSkip(<<"keepme2">>)),
    ?assertEqual(true,  IsSkip(<<"skipme1">>)),
    ?assertEqual(true,  IsSkip(<<"skipme2">>)).
   
