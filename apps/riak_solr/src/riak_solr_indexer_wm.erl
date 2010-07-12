%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_indexer_wm).
-export([init/1, allowed_methods/2, malformed_request/2]).
-export([process_post/2]).

-include_lib("riak_search/include/riak_search.hrl").
-include_lib("webmachine/include/webmachine.hrl").


-record(state, {solr_client, method, schema, command, entries}).
-define(DEFAULT_INDEX, "search").

init(_) ->
    {ok, SolrClient} = riak_solr_app:local_client(),
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
                error_logger:error_msg("Could not parse docs for index'~s'.~n~p~n~p~n",
                                       [Index, Error, erlang:get_stacktrace()]),
                {true, Req, State}
            end;
        Error ->
            error_logger:error_msg("Could not parse schema for index'~s'.~n~p~n~p~n",
                                   [Index, Error, erlang:get_stacktrace()]),
            {true, Req, State}
    end.

process_post(Req, State = #state{ solr_client=SolrClient, schema=Schema, command=Command, entries=Entries }) ->
    try
        SolrClient:run_solr_command(Schema, Command, Entries),
        %% Hard coding 200 to be like Solr
        {{halt, 200}, Req, State}
    catch _ : Error ->
        Msg = "Error in riak_solr_indexer_wm:process_post/2: ~p~n~p~n",
        error_logger:error_msg(Msg, [Error, erlang:get_stacktrace()]),
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
