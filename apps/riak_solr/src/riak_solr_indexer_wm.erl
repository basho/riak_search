-module(riak_solr_indexer_wm).
-export([init/1, allowed_methods/2, malformed_request/2]).
-export([process_post/2]).

-include_lib("riak_search/include/riak_search.hrl").
-include_lib("webmachine/include/webmachine.hrl").


-record(state, {client, method, command, docs}).
-define(DEFAULT_INDEX, "search").

init(_) ->
    {ok, Client} = riak_search:local_client(),
    {ok, #state{ client=Client }}.

allowed_methods(Req, State) ->
    {['POST'], Req, State#state{method=wrq:method(Req)}}.

malformed_request(Req, State = #state { client=Client }) ->
    %% Try to get the schema...
    Index = get_index_name(Req),
    case riak_solr_config:get_schema(Index) of
        {ok, Schema} ->
            %% Try to parse the body...
            Client = State#state.client,
            Body = wrq:req_body(Req),
            try
                {ok, Command, Docs} = Client:parse_solr_xml(Schema, Body),
                {false, Req, State#state { command=Command, docs=Docs }}
            catch _ : Error ->
                error_logger:error_msg("Could not parse docs '~s'.~n~p~n", [Index, Error]),
                {true, Req, State}
            end;
        Error ->
            error_logger:error_msg("Could not parse schema '~s'.~n~p~n", [Index, Error]),
            {true, Req, State}
    end.

process_post(Req, State = #state{ client=Client, command=Command, docs=Docs }) ->
    try
        Client:run_solr_command(Command, Docs),
        {true, Req, State}
    catch _ : Error ->
        Msg = "Error in riak_solr_indexer_wm:process_post/2: ~p~n~p~n",
        error_logger:error_msg(Msg, [Error, erlang:get_stacktrace()]),
        {false, Req, State}
    end.

get_index_name(Req) ->
    case wrq:path_info(index, Req) of
        undefined ->
            DefaultIndex = app_helper:get_env(riak_solr, default_index, ?DEFAULT_INDEX),
            wrq:get_qs_value(index, DefaultIndex, Req);
        Index ->
            Index
    end.
