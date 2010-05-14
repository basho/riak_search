-module(riak_solr_searcher_wm).

-include_lib("riak_search/include/riak_search.hrl").
-include("riak_solr.hrl").

-export([init/1, allowed_methods/2, malformed_request/2]).
-export([content_types_provided/2, to_json/2]).

-record(state, {schema, sq}).
-record(squery, {q, q_op, df, wt, start, rows}).

-include_lib("webmachine/include/webmachine.hrl").

-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").

init(_) ->
    {ok, #state{}}.

allowed_methods(Req, State) ->
    {['GET'], Req, State}.

malformed_request(Req, State) ->
    case get_schema(Req) of
        undefined ->
            {true, Req, State};
        SchemaName ->
            case riak_solr_config:get_schema(SchemaName) of
                {ok, Schema} ->
                    case parse_query(Schema, Req) of
                        {ok, SQuery} ->
                            {false, Req, State#state{schema=Schema, sq=SQuery}};
                        _Error ->
                            {true, Req, State}
                    end
            end
    end.

content_types_provided(Req, State) ->
    {[{"application/json", to_json}], Req, State}.

to_json(Req, #state{schema=Schema, sq=SQuery}=State) ->
    #squery{q=QText}=SQuery,
    DefaultField = get_default_field(SQuery, Schema),
    {ok, Client} = riak_search:local_client(),
    StartTime = erlang:now(),
    Docs = Client:doc_search(Schema:name(), DefaultField, QText),
    ElapsedTime = erlang:trunc(timer:now_diff(erlang:now(), StartTime) / 1000),
    {build_json_response(Schema, ElapsedTime, SQuery, Docs), Req, State}.

%% Internal functions
build_json_response(_Schema, ElapsedTime, SQuery, []) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"q">>, list_to_binary(SQuery#squery.q)},
                           {<<"q.op">>, atom_to_binary(SQuery#squery.q_op, utf8)},
                           {<<"wt">>, <<"json">>}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, 0}]}}],
    mochijson2:encode({struct, Response});
build_json_response(Schema, ElapsedTime, SQuery, Docs0) ->
    F = fun({Name, Value}) -> Field = Schema:find_field(Name),
                              convert_type(Value, Field#riak_solr_field.type) end,
    Docs = truncate_results(SQuery#squery.start + 1, SQuery#squery.rows, Docs0),
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"q">>, list_to_binary(SQuery#squery.q)},
                           {<<"q.op">>, atom_to_binary(SQuery#squery.q_op, utf8)},
                           {<<"wt">>, <<"json">>}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, length(Docs0)},
                            {<<"start">>, SQuery#squery.start},
                            {<<"docs">>, [riak_indexed_doc:to_mochijson2(F, Doc) || Doc <- Docs]}]}}],
    mochijson2:encode({struct, Response}).

truncate_results(Start, _Rows, Docs) when Start > length(Docs) ->
    [];
truncate_results(Start, Rows, Docs) ->
    lists:sublist(Docs, Start, Rows).

get_default_field(#squery{df=""}, Schema) ->
    R = Schema:default_field(),
    R;
get_default_field(#squery{df=DefaultField}, _Schema) ->
    R = DefaultField,
    R.

get_schema(Req) ->
    case wrq:path_info(index, Req) of
        undefined ->
            case wrq:get_qs_value("index", Req) of
                undefined ->
                    app_helper:get_env(riak_solr, default_schema, undefined);
                Index ->
                    Index
            end;
        Index ->
            Index
    end.

parse_query(Schema, Req) ->
    Query = #squery{q=wrq:get_qs_value("q", "", Req),
                    q_op=list_to_atom(wrq:get_qs_value("q.op", Schema:default_op(), Req)),
                    df=wrq:get_qs_value("df", "", Req),
                    wt="json",
                    start=list_to_integer(wrq:get_qs_value("start", "0", Req)),
                    rows=list_to_integer(wrq:get_qs_value("rows", "10", Req))},
    case Query#squery.q =:= "" of
        true ->
            {error, missing_query};
        false ->
            {ok, Query}
    end.

convert_type(FieldValue, string) ->
    riak_search_utils:to_binary(FieldValue);
convert_type("true", boolean) ->
    true;
convert_type("false", boolean) ->
    false;
convert_type("yes", boolean) ->
    true;
convert_type("no", boolean) ->
    false;
convert_type(FieldValue, integer) ->
    list_to_integer(FieldValue);
convert_type(FieldValue, float) ->
    list_to_float(FieldValue).
