%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_solr_searcher_wm).
-export([init/1, allowed_methods/2, malformed_request/2, forbidden/2]).
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
                presort,
                filter_ops,
                fl}).

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

validate_state(#state{schema=Schema, fl=FL, sort=Sort}=State) ->
    UK = binary_to_list(Schema:unique_key()),
    if
        FL == UK andalso Sort /= "none" ->
            throw({error, fl_id_with_sort, UK});
        true ->
            State
    end.

malformed_request(Req, State) ->
    %% Try to get the schema...
    Index = get_index_name(Req),
    case riak_search_config:get_schema(Index) of
        {ok, Schema0} ->
            case parse_squery(Req) of
                {ok, SQuery} ->
                    Schema = riak_search_utils:replace_schema_defaults(SQuery,
                                                                       Schema0),

                    %% Try to parse the query
                    Client = State#state.client,
                    try
                        {ok, QueryOps} = Client:parse_query(Schema, SQuery#squery.q),
                        {ok, FilterOps} = Client:parse_filter(Schema, SQuery#squery.filter),
                        {false, Req, validate_state(State#state{schema=Schema,
                                                                squery=SQuery,
                                                                query_ops=QueryOps,
                                                                filter_ops=FilterOps,
                                                                sort=wrq:get_qs_value("sort", "none", Req),
                                                                wt=wrq:get_qs_value("wt", "standard", Req),
                                                                presort=to_atom(string:to_lower(wrq:get_qs_value("presort", "score", Req))),
                                                                fl=wrq:get_qs_value("fl", "*", Req)})}
                    catch _ : Error ->
                            Msg = riak_search_utils:err_msg(Error),
                            lager:error(Msg),
                            Req1 = wrq:set_resp_header("Content-Type", "text/plain",
                                                       Req),
                            Req2 = wrq:append_to_response_body(list_to_binary(Msg),
                                                               Req1),

                            {true, Req2, State}
                    end;
                _Error ->
                    {true, Req, State}
            end;
        Error ->
            lager:error("Could not parse schema '~s' : ~p", [Index, Error]),
            {true, Req, State}
    end.

forbidden(ReqData, Context) ->
    case riak_core_security:is_enabled() of
        true ->
            {true,
                wrq:set_resp_body(<<"Riak Search 1.0 is deprecated in Riak 2.0"
                    " and is not compatible with security.">>,
                    wrq:set_resp_header("Content-Type", "text/plain", ReqData)),
                Context};
        false ->
            Class = {riak_search, query},
            riak_kv_wm_utils:is_forbidden(ReqData, Class, Context)
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

parse_fl(FL) ->
    if FL == "*" -> all;
       true -> re:split(FL, "[, ]")
    end.

to_json(Req, #state{sort=SortBy, fl=FL}=State) ->
    #state{schema=Schema, squery=SQuery}=State,
    FL2 = parse_fl(FL),
    State2 = State#state{fl=FL2},

    {ElapsedTime, NumFound, MaxScore, DocsOrIDs} = run_query(State2),
    {riak_solr_output:json_response(Schema, SortBy, ElapsedTime, SQuery,
                                    NumFound, MaxScore, DocsOrIDs,
                                    FL2),
     Req, State}.

to_xml(Req, #state{sort=SortBy, fl=FL}=State) ->
    #state{schema=Schema, squery=SQuery}=State,
    FL2 = parse_fl(FL),
    State2 = State#state{fl=FL2},

    {ElapsedTime, NumFound, MaxScore, DocsOrIDs} = run_query(State2),
    {riak_solr_output:xml_response(Schema, SortBy, ElapsedTime, SQuery,
                                   NumFound, MaxScore, DocsOrIDs, FL2),
     Req, State}.

run_query(#state{client=Client, schema=Schema, squery=SQuery,
                 query_ops=QueryOps, filter_ops=FilterOps, presort=Presort,
                 fl=FL}) ->
    riak_search_utils:run_query(Client, Schema, SQuery, QueryOps, FilterOps,
                                Presort, FL).

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
            Filter = wrq:get_qs_value("filter", "", Req),
            DefaultField = wrq:get_qs_value("df", undefined, Req),
            DefaultOp = to_atom(wrq:get_qs_value("q.op", undefined, Req)),
            QueryStart = to_integer(wrq:get_qs_value("start", 0, Req)),
            QueryRows = to_integer(wrq:get_qs_value("rows", ?DEFAULT_RESULT_SIZE, Req)),
            SQuery = #squery{q=Query,
                             filter=Filter,
                             default_op=DefaultOp,
                             default_field=DefaultField,
                             query_start=QueryStart,
                             query_rows=QueryRows},
            {ok, SQuery}
    end.
