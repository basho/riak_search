%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_indexer_wm).
-export([init/1, allowed_methods/2, malformed_request/2, forbidden/2]).
-export([process_post/2]).

-include("riak_search.hrl").
-include_lib("webmachine/include/webmachine.hrl").


-record(state, {solr_client, method, schema, command, entries}).

init(_) ->
    {ok, SolrClient} = riak_search:local_solr_client(),
    {ok, #state{ solr_client=SolrClient }}.

allowed_methods(Req, State) ->
    {['POST'], Req, State#state{method=wrq:method(Req)}}.

malformed_request(Req, State) ->
    %% Try to get the schema...
    Index = get_index_name(Req),
    case riak_search_config:get_schema(Index) of
        {ok, Schema} ->
            %% Try to parse the body...
            SolrClient = State#state.solr_client,
            Body = wrq:req_body(Req),
            try
                {ok, Command, Entries} = SolrClient:parse_solr_xml(Schema, Body),
                {false, Req, State#state { schema=Schema, command=Command, entries=Entries }}
            catch _ : Error ->
                    Msg = riak_search_utils:err_msg(Error),
                    lager:error(Msg),
                    Req1 = wrq:set_resp_header("Content-Type", "text/plain",
                                               Req),
                    Req2 = wrq:append_to_response_body(list_to_binary(Msg),
                                                       Req1),
                    {true, Req2, State}
            end;
        Error ->
            lager:error("Could not parse schema for index '~s': ~p",
                        [Index, Error]),
            {true, Req, State}
    end.

forbidden(RD, Ctx) ->
    case riak_core_security:is_enabled() of
        true ->
            RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
            {true, wrq:set_resp_body(<<"Riak Search 1.0 is"
                                             " deprecated in Riak 2.0 and is"
                                             " not compatible with"
                                             " security.">>, RD1), Ctx};
        false ->
            {false, RD, Ctx}
    end.

process_post(Req, State = #state{ solr_client=SolrClient, schema=Schema, command=Command, entries=Entries }) ->
    try
        SolrClient:run_solr_command(Schema, Command, Entries),
        %% Hard coding 200 to be like Solr
        {{halt, 200}, Req, State}
    catch _ : Error ->
        lager:error("Error processing post: ~p", [Error]),
        {false, Req, State}
    end.

get_index_name(Req) ->
    case wrq:path_info(index, Req) of
        undefined ->
            DefaultIndex = app_helper:get_env(riak_solr, default_index, ?DEFAULT_INDEX),
            wrq:get_qs_value("index", DefaultIndex, Req);
        Index ->
            Index
    end.
