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
                    end;
                Error ->
                    error_logger:error_msg("Could not parse schema '~s'.~n~p~n", [SchemaName, Error]),
                    throw(Error)
            end
    end.

content_types_provided(Req, State) ->
    {[{"application/json", to_json}], Req, State}.

to_json(Req, #state{schema=Schema, sq=SQuery}=State) ->
    #squery{q=QText, start=QStart, rows=QRows}=SQuery,
    DefaultField = get_default_field(SQuery, Schema),
    {ok, Client} = riak_search:local_client(),
    StartTime = erlang:now(),
    {NumFound, Docs} = Client:doc_search(Schema:name(), DefaultField, QText, QStart, QRows),
    ElapsedTime = erlang:trunc(timer:now_diff(erlang:now(), StartTime) / 1000),
    {build_json_response(Schema, ElapsedTime, SQuery, NumFound, Docs), Req, State}.

%% Internal functions
build_json_response(_Schema, ElapsedTime, SQuery, NumFound, []) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, list_to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, atom_to_binary(SQuery#squery.q_op, utf8)},
                                       {<<"wt">>, <<"json">>}]}}]}},
                 {<<"response">>,
                  {struct, [
                            {<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.start}]}}],
    mochijson2:encode({struct, Response});
build_json_response(Schema, ElapsedTime, SQuery, NumFound, Docs) ->
    F = fun({Name, Value}) -> 
        case Schema:find_field(Name) of
            Field when is_record(Field, riak_solr_field) ->
                Type = Field#riak_solr_field.type;
            undefined ->
                error_logger:info_msg("Field '~s' is not defined, defaulting to type 'string'.~n", [Name]),
                Type = string
        end,
        convert_type(Value, Type)
    end,
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, list_to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, atom_to_binary(SQuery#squery.q_op, utf8)},
                                       {<<"wt">>, <<"json">>}]}}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.start},
                            {<<"docs">>, [riak_indexed_doc:to_mochijson2(F, Doc) || Doc <- Docs]}]}}],
    mochijson2:encode({struct, Response}).

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
