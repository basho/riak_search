-module(testing_config).

-include_lib("riak_solr/include/riak_solr.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_test_() ->
    [{setup, fun() -> start_config_server() end,
      fun({ok, Pid}) -> exit(Pid, kill) end,
      [fun() ->
               Schema = riak_solr_config:get_schema("schema1"),
               validate_schema(schema1, Schema) end]}].

%% Helper functions
start_config_server() ->
    application:load(riak_solr),
    application:set_env(riak_solr, schema_dir, "./../test/test_data"),
    riak_solr_config:start_link().

validate_schema(schema1, #riak_solr_schemadef{name="schema1", version="1.1",
                                              fields=Fields}) ->
    FirstName = dict:fetch("first_name", Fields),
    LastName = dict:fetch("last_name", Fields),
    Paygrade = dict:fetch("paygrade", Fields),
    Cookies = dict:fetch("likes_cookies", Fields),
    ?assertMatch("first_name", FirstName#riak_solr_field.name),
    ?assertMatch(string, FirstName#riak_solr_field.type),
    ?assertMatch(true, FirstName#riak_solr_field.required),
    ?assertMatch("last_name", LastName#riak_solr_field.name),
    ?assertMatch(string, LastName#riak_solr_field.type),
    ?assertMatch(true, LastName#riak_solr_field.required),
    ?assertMatch("paygrade", Paygrade#riak_solr_field.name),
    ?assertMatch(integer, Paygrade#riak_solr_field.type),
    ?assertMatch(true, Paygrade#riak_solr_field.required),
    ?assertMatch("likes_cookies", Cookies#riak_solr_field.name),
    ?assertMatch(boolean, Cookies#riak_solr_field.type),
    ?assertMatch(false, Cookies#riak_solr_field.required).
