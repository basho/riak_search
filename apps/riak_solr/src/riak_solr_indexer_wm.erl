-module(riak_solr_indexer_wm).

-include_lib("riak_search/include/riak_search.hrl").

-export([init/1, allowed_methods/2, malformed_request/2]).
-export([process_post/2]).

-record(state, {method, body, schema, sq}).

-include_lib("webmachine/include/webmachine.hrl").

-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").

init(_) ->
    {ok, #state{}}.

allowed_methods(Req, State) ->
    {['POST'], Req, State#state{method=wrq:method(Req)}}.

malformed_request(Req, State) ->
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
                            case size(Body) of
                                0 ->
                                    {true, Req, State};
                                _ ->
                                    {false, Req, State#state{body=Body,
                                                             schema=Schema}}
                            end
                    end;
                Error ->
                    error_logger:error_msg("Could not parse schema '~s'.~n~p~n", [SchemaName, Error]),
                    {false, Req, State}
            end
    end.

process_post(Req, #state{schema=Schema, body=Body}=State) ->
    case catch riak_solr_xml_xform:xform(Body) of
        {'EXIT', _Error} ->
            {false, Req, State};
        Commands0 ->
            case Schema:validate_commands(Commands0) of
                {ok, Commands} ->
                    Cmd = proplists:get_value(cmd, Commands0),
                    handle_command(Cmd, Schema, Commands, Req, State);
                _Error ->
                    {false, Req, State}
            end
    end.

%% Internal functions
handle_command(add, Schema, Commands, Req, State) ->
    {ok, Client} = riak_search:local_client(),
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    [Client:index_doc(AnalyzerPid, build_idx_doc(Schema:name(), Doc)) || Doc <- Commands],
    qilr_analyzer:close(AnalyzerPid),
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
