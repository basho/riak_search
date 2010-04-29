-module(riak_solr_searcher_wm).

-include_lib("riak_search/include/riak_search.hrl").

-export([init/1, allowed_methods/2, malformed_request/2]).
-export([content_types_provided/2, to_json/2]).

-record(state, {schema, sq}).
-record(squery, {q, df, wt, start, rows}).

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
                    case parse_query(Req) of
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
    #squery{q=QText, rows=Rows, start=Start}=SQuery,
    DefaultField = get_default_field(SQuery, Schema),
    {ok, Client} = riak_search:local_client(),
    StartTime = erlang:now(),
    io:format("Index: ~p, Default: ~p, Query: ~p~n", [Schema:name(), DefaultField, QText]),
    Docs = Client:doc_search(Schema:name(), DefaultField, QText),
    ElapsedTime = erlang:trunc(timer:now_diff(erlang:now(), StartTime) / 1000),
    {build_json_response(ElapsedTime, QText, Start, Rows, Docs), Req, State}.

%% Internal functions
build_json_response(ElapsedTime, QText, _Start, _Rows, []) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"q">>, list_to_binary(QText)},
                           {<<"wt">>, <<"json">>}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, 0}]}}],
    mochijson2:encode({struct, Response});
build_json_response(ElapsedTime, QText, Start, Rows, Docs0) ->
    Docs = truncate_results(Start + 1, Rows, Docs0),
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"q">>, QText},
                           {<<"wt">>, <<"json">>}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, length(Docs0)},
                            {<<"start">>, Start},
                            {<<"docs">>, [riak_indexed_doc:to_mochijson2(Doc) || Doc <- Docs]}]}}],
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
            wrq:get_qs_value(index, Req);
        Index ->
            Index
    end.

parse_query(Req) ->
    Query = #squery{q=wrq:get_qs_value("q", "", Req),
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
