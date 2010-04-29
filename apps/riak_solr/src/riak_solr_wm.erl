-module(riak_solr_wm).

-include_lib("riak_search/include/riak_search.hrl").

-export([init/1, allowed_methods/2, malformed_request/2]).
-export([content_types_provided/2, process_post/2, to_json/2]).

-record(state, {method, body, schema, sq}).
-record(squery, {q, df, wt, start, rows}).

-include_lib("webmachine/include/webmachine.hrl").

-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").

init(_) ->
    {ok, #state{}}.

allowed_methods(Req, State) ->
    {['GET', 'POST'], Req, State#state{method=wrq:method(Req)}}.

malformed_request(Req, #state{method='POST'}=State) ->
    case get_schema(Req) of
        undefined ->
            {true, Req, State};
        SchemaName ->
            case riak_solr_config:get_schema(SchemaName) of
                {ok, Schema} ->
                    case wrq:req_body(Req) of
                        undefined ->
                            {true, Req, State};
                        Body ->
                            {false, Req, State#state{body=Body,
                                                     schema=Schema}}
                    end;
                _ ->
                    {false, Req, State}
            end
    end;
malformed_request(Req, #state{method='GET'}=State) ->
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
                    end;
                _ ->
                    {false, Req, State}
            end
    end.

content_types_provided(Req, State) ->
    {[{"application/json", to_json}], Req, State}.

process_post(Req, #state{schema=SchemaName, body=Body}=State) ->
    case riak_solr_config:get_schema(SchemaName) of
        {ok, Schema} ->
            case catch riak_solr_xml_xform:xform(Schema:name(), Body) of
                {'EXIT', _} ->
                    {false, Req, State};
                Commands0 ->
                    case Schema:validate_commands(Commands0) of
                        {ok, Commands} ->
                            Cmd = proplists:get_value(cmd, Commands0),
                            handle_command(Cmd, Schema, Commands, Req, State);
                        _Error ->
                            {false, Req, State}
                    end
            end;
        Error ->
            error_logger:error_msg("Error retrieving schema: ~p~n", [Error]),
            {false, Req, State}
    end.

to_json(Req, #state{schema=Schema, sq=SQuery}=State) ->
    #squery{q=QText, rows=Rows, start=Start}=SQuery,
    DefaultField = get_default_field(SQuery, Schema),
    {ok, Client} = riak_search:local_client(),
    Start = erlang:now(),
    Docs = Client:doc_search(Schema:name(), DefaultField, QText),
    ElapsedTime = erlang:trunc(timer:now_diff(erlang:now(), Start) / 1000),
    {build_json_response(ElapsedTime, QText, Start, Rows, Docs), Req, State}.

%% Internal functions
build_json_response(ElapsedTime, QText, _Start, _Rows, []) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"q">>, QText},
                           {<<"wt">>, <<"json">>}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, 0}]}}],
    mochijson2:encode({struct, Response});
build_json_response(ElapsedTime, QText, Start, Rows, Docs0) ->
    Docs = truncate_results(Start, Rows, Docs0),
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

get_default_field(#squery{df=undefined}, Schema) ->
    Schema:default_field();
get_default_field(#squery{df=DefaultField}, _Schema) ->
    DefaultField.

handle_command(add, Index, Commands, Req, State) ->
    {ok, Client} = riak_search:local_client(),
    [Client:index_doc(build_idx_doc(Index, Doc)) || Doc <- Commands],
    {true, Req, State}.

build_idx_doc(Index, Doc0) ->
    Id = dict:fetch("id", Doc0),
    Doc = dict:erase(Id, Doc0),
    #riak_idx_doc{id=Id, index=Index,
                  fields=dict:to_list(Doc), props=[]}.
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
