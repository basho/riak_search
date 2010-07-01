-module(riak_solr_output).

-include_lib("riak_search/include/riak_search.hrl").
-include("riak_solr.hrl").

-export([xml_response/6, json_response/6]).

-import(riak_search_utils, [to_atom/1,
                            to_integer/1,
                            to_binary/1,
                            to_boolean/1,
                            to_float/1]).


xml_response(Schema, _SortBy, ElapsedTime, SQuery, NumFound, Docs) ->
    TFun = mustache:compile(solr_xml),
    Ctx = dict:from_list([{qtime, ElapsedTime}, {squery, SQuery},
                          {num_found, NumFound}, {start_row, SQuery#squery.query_start},
                          {results, Docs}, {schema, Schema}]),
    mustache:render(solr_xml, TFun, Ctx).

json_response(Schema, _SortBy, ElapsedTime, SQuery, NumFound, []) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, to_binary(Schema:default_op())},
                                       {<<"wt">>, <<"json">>}]}}]}},
                 {<<"response">>,
                  {struct, [
                            {<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.query_start}]}}],
    mochijson2:encode({struct, Response});
json_response(Schema, SortBy, ElapsedTime, SQuery, NumFound, Docs0) ->
    F = fun({Name, Value}) ->
        case Schema:find_field_or_facet(Name) of
            Field when is_record(Field, riak_search_field) ->
                Type = Field#riak_search_field.type;
            undefined ->
                Type = unknown
        end,
        convert_type(Value, Type)
    end,
    Docs = riak_solr_sort:sort(Docs0, SortBy),
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, to_binary(Schema:default_op())},
                                       {<<"wt">>, <<"json">>}]}}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.query_start},
                            {<<"docs">>, [riak_indexed_doc:to_mochijson2(F, Doc) || Doc <- Docs]}]}}],
    mochijson2:encode({struct, Response}).

%% Internal functions
convert_type(FieldValue, unknown) ->
    convert_type(FieldValue, string);
convert_type(FieldValue, string) ->
    to_binary(FieldValue);
convert_type(FieldValue, integer) ->
    to_integer(FieldValue);
convert_type(FieldValue, float) ->
    to_float(FieldValue);
convert_type(FieldValue, boolean) ->
    to_boolean(FieldValue);
convert_type(_FieldValue, Other) ->
    throw({unhandled_type, Other}).
