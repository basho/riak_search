-module(riak_solr_indexer_wm).
-export([init/1, allowed_methods/2, malformed_request/2]).
-export([process_post/2]).

-include_lib("riak_search/include/riak_search.hrl").
-include_lib("webmachine/include/webmachine.hrl").


-record(state, {solr_client, method, schema, command, entries}).
-define(DEFAULT_INDEX, "search").

init(_) ->
    io:format("(~p) ~p: init~n", [self(), ?MODULE]),
    {ok, SolrClient} = riak_solr_app:local_client(),
    {ok, #state{ solr_client=SolrClient }}.

allowed_methods(Req, State) ->
    {['POST'], Req, State#state{method=wrq:method(Req)}}.

malformed_request(Req, State) ->
    io:format("(~p) ~p: malformed_request~n", [self(), ?MODULE]),
    %% Try to get the schema...
    Index = get_index_name(Req),
    io:format("(~p) ~p:~p here~n", [self(), ?MODULE, ?LINE]),
    case riak_search_config:get_schema(Index) of
        {ok, Schema} ->
            %% Try to parse the body...
            SolrClient = State#state.solr_client,
            Body = wrq:req_body(Req),
            io:format("(~p) ~p:~p here~n", [self(), ?MODULE, ?LINE]),
            try
                {ok, Command, Entries} = SolrClient:parse_solr_xml(Schema, Body),
                io:format("(~p) ~p:~p here~n", [self(), ?MODULE, ?LINE]),
                {false, Req, State#state { schema=Schema, command=Command, entries=Entries }}
            catch _ : Error ->
                error_logger:error_msg("Could not parse docs '~s'.~n~p~n", [Index, Error]),
                {true, Req, State}
            end;
        Error ->
            error_logger:error_msg("Could not parse schema '~s'.~n~p~n", [Index, Error]),
            {true, Req, State}
    end.

process_post(Req, State = #state{ solr_client=SolrClient, schema=Schema, command=Command, entries=Entries }) ->
    io:format("(~p) ~p: process_post~n", [self(), ?MODULE]),
    try
        io:format("(~p) ~p: Running command~n", [self(), ?MODULE]),
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
