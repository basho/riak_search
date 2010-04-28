-module(riak_solr_wm).

-include_lib("riak_search/include/riak_search.hrl").

-export([init/1, allowed_methods/2, malformed_request/2]).
-export([content_types_provided/2, process_post/2]).

-record(state, {method, body, index, q, pq, rows}).

-include_lib("webmachine/include/webmachine.hrl").

-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").

init(_) ->
    {ok, #state{}}.

allowed_methods(Req, State) ->
    {['GET', 'POST'], Req, State#state{method=wrq:method(Req),
                                       index=get_index(Req)}}.

malformed_request(Req, #state{method='POST'}=State) ->
    case get_index(Req) of
        undefined ->
            {true, Req, State};
        Index ->
            case wrq:req_body(Req) of
                undefined ->
                    {true, Req, State};
                Body ->
                    {false, Req, State#state{body=Body,
                                             index=Index}}
            end
    end;
malformed_request(Req, State) ->
    case get_index(Req) of
        undefined ->
            {true, Req, State};
        Index ->
            {false, Req, State#state{index=Index}}
    end.

content_types_provided(Req, State) ->
    {[{"application/json", to_json},
      {"text/xml", to_xml}], Req, State}.

process_post(Req, #state{index=Index, body=Body}=State) ->
    case riak_solr_config:get_schema(Index) of
        {ok, Schema} ->
            case catch riak_solr_xml_xform:xform(Schema:name(), Body) of
                {'EXIT', _} ->
                    {false, Req, State};
                Commands0 ->
                    case Schema:validate_commands(Commands0) of
                        {ok, Commands} ->
                            Cmd = proplists:get_value(cmd, Commands0),
                            handle_command(Cmd, Index, Commands, Req, State);
                        _Error ->
                            {false, Req, State}
                    end
            end;
        Error ->
            error_logger:error_msg("Error retrieving schema: ~p~n", [Error]),
            {false, Req, State}
    end.

%% Internal functions
handle_command(add, Index, Commands, Req, State) ->
    Docs = [build_idx_doc(Index, Doc) || Doc <- Commands],
    [D|_] = Docs,
    io:format("D: ~p~n", [D]),
    {ok, Client} = riak_search:local_client(),
    Client:index_doc(D),
%% [store(Client, build_idx_doc(Index, Doc)) ||
%%  Doc <- proplists:get_value(docs, Commands)],
    {true, Req, State}.
build_idx_doc(Index, Doc0) ->
    Id = dict:fetch("id", Doc0),
    Doc = dict:erase(Id, Doc0),
    #riak_idx_doc{id=Id, index=Index,
                  fields=dict:to_list(Doc), props=[]}.
get_index(Req) ->
    case wrq:path_info(index, Req) of
        undefined ->
            wrq:get_qs_value(index, Req);
        Index ->
            Index
    end.
