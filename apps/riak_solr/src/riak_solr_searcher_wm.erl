%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_searcher_wm).
-export([init/1, allowed_methods/2, malformed_request/2]).
-export([content_types_provided/2, to_json/2, to_xml/2]).

-import(riak_search_utils, [
    to_atom/1, to_integer/1, to_binary/1, to_boolean/1, to_float/1]).

-include("riak_solr.hrl").
-include("riak_search.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(state, {client,
                wt,
                schema,
                squery,
                query_ops,
                sort,
                presort
}).

-define(DEFAULT_RESULT_SIZE, 10).
-define(DEFAULT_TIMEOUT, 60000).

init(_) ->
    {ok, Client} = riak_search:local_client(),
    {ok, #state{ client=Client }}.

allowed_methods(Req, State) ->
    %% Hackish, but its what Solr clients expect. If we have an
    %% incoming 'POST' request, then transform it to a 'GET'.
    case wrq:method(Req) of
        'POST' ->
            Body = wrq:req_body(Req),
            Parsed = mochiweb_util:parse_qs(Body),
            NewReq = Req#wm_reqdata{req_qs=Parsed, method='GET'};
        _ ->
            NewReq = Req
    end,
    {['GET'], NewReq, State}.

malformed_request(Req, State) ->
    %% Try to get the schema...
    Index = get_index_name(Req),
    case riak_search_config:get_schema(Index) of
        {ok, Schema0} ->
            case parse_squery(Req) of
                {ok, SQuery} ->
                    %% Update schema defaults...
                    Schema = replace_schema_defaults(SQuery, Schema0),

                    %% Try to parse the query
                    Client = State#state.client,
                    try
                        {ok, QueryOps} = Client:parse_query(Schema, SQuery#squery.q),
                        {false, Req, State#state{schema=Schema, squery=SQuery, query_ops=QueryOps,
                                                 sort=wrq:get_qs_value("sort", "none", Req),
                                                 wt=wrq:get_qs_value("wt", "standard", Req),
                                                 presort=to_atom(string:to_lower(wrq:get_qs_value("presort", "score", Req)))}}
                    catch _ : Error ->
                        {true, riak_solr_error:log_error(Req, Error), State}
                    end;
                _Error ->
                    {true, Req, State}
            end;
        Error ->
            error_logger:error_msg("Could not parse schema '~s'.~n~p~n", [Index, Error]),
            {true, Req, State}
    end.

content_types_provided(Req, #state{wt=WT}=State) ->
    Types = case WT of
                "standard" ->
                    [{"text/xml", to_xml}];
                "xml" ->
                    [{"text/xml", to_xml}];
                "json" ->
                    [{"application/json", to_json}];
                "ruby" ->
                    [{"application/json", to_json}];
                _ ->
                    []
            end,
    {Types, Req, State}.

to_json(Req, #state{sort=SortBy}=State) ->
    #state{schema=Schema, squery=SQuery}=State,
    %% Run the query...
    {ElapsedTime, NumFound, MaxScore, Docs} = run_query(State),
    %% Generate output
    {riak_solr_output:json_response(Schema, SortBy, ElapsedTime, SQuery, NumFound, MaxScore, Docs), Req, State}.

to_xml(Req, #state{sort=SortBy}=State) ->
    #state{schema=Schema, squery=SQuery}=State,
    %% Run the query...
    {ElapsedTime, NumFound, MaxScore, Docs} = run_query(State),
    %% Generate output
    {riak_solr_output:xml_response(Schema, SortBy, ElapsedTime, SQuery, NumFound, MaxScore, Docs), Req, State}.

run_query(#state{client=Client, schema=Schema, squery=SQuery,
                 query_ops=QueryOps, presort=Presort}) ->
    #squery{query_start=QStart, query_rows=QRows}=SQuery,

    %% Run the query...
    StartTime = erlang:now(),
    {NumFound, MaxScore, Docs} = Client:search_doc(Schema, QueryOps, QStart, QRows, Presort, ?DEFAULT_TIMEOUT),
    ElapsedTime = erlang:round(timer:now_diff(erlang:now(), StartTime) / 1000),
    {ElapsedTime, NumFound, MaxScore, Docs}.

%% @private
%% Pull the index name from the request. If not found, then use the
%% name defined in the riak_solr - default_index configuration
%% setting. If still not found, use the default of "search".
get_index_name(Req) ->
    case wrq:path_info(index, Req) of
        undefined ->
            DefaultIndex = app_helper:get_env(riak_solr, default_index, ?DEFAULT_INDEX),
            wrq:get_qs_value("index", DefaultIndex, Req);
        Index ->
            Index
    end.

%% @private
%% Pull values out of the query string, return either {ok, SQueryRec}
%% or {error, missing_query}.
parse_squery(Req) ->
    %% Parse the query parts...
    Query = wrq:get_qs_value("q", "", Req),
    case Query == "" of
        true ->
            {error, missing_query};
        false ->
            DefaultField = wrq:get_qs_value("df", undefined, Req),
            DefaultOp = case wrq:get_qs_value("q.op", undefined, Req) of
                            undefined ->
                                undefined;
                            Other ->
                                to_atom(string:to_lower(Other))
                        end,
            QueryStart = to_integer(wrq:get_qs_value("start", 0, Req)),
            QueryRows = to_integer(wrq:get_qs_value("rows", ?DEFAULT_RESULT_SIZE, Req)),
            SQuery = #squery{q=Query,
                             default_op=DefaultOp,
                             default_field=DefaultField,
                             query_start=QueryStart,
                             query_rows=QueryRows},
            {ok, SQuery}
    end.


%% @private
%% Override the provided schema with a new default field, if one is
%% supplied in the query string.
replace_schema_defaults(SQuery, Schema0) ->
    Schema1 = case SQuery#squery.default_op of
                  undefined ->
                      Schema0;
                  Op ->
                      Schema0:set_default_op(Op)
              end,
    case SQuery#squery.default_field of
        undefined ->
            Schema1;
        Field ->
            Schema1:set_default_field(to_binary(Field))
    end.
