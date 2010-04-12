-module(riak_solr_wm).

-export([init/1, allowed_methods/2, content_types_provided/2]).
-export([malformed_request/2, to_xml/2]).

-record(search_query, {index, q, pq, rows}).

-include_lib("webmachine/include/webmachine.hrl").

-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").

init(_) ->
    {ok, []}.

allowed_methods(Req, State) ->
    {['GET'], Req, State}.

content_types_provided(Req, State) ->
    {[{"text/xml", to_xml},
      {"application/xml", to_xml}], Req, State}.

malformed_request(Req, State) ->
    Query = wrq:get_qs_value("q", "", Req),
    Index = case wrq:path_info(index, Req) of
                undefined ->
                    ?DEFAULT_INDEX;
                Index0 ->
                    Index0
            end,
    {IsMalformed, NewState} = case parse_query(Query) of
                                  {ok, P} ->
                                      Rows = list_to_integer(wrq:get_qs_value("rows", "10", Req)),
                                      {false, #search_query{index=Index, q=Query, pq=P, rows=Rows}};
                                  _ ->
                                      %% TODO: Send a descriptive error message
                                      {true, State}
                              end,
    {IsMalformed, Req, NewState}.

to_xml(Req, #search_query{q=OrigQuery, index=Index, pq=Query, rows=Rows}=State) ->
    Start = erlang:now(),
    SearchResult = riak_search:execute(Query, Index, ?DEFAULT_FIELD, []),
    End = erlang:now(),
    case SearchResult of
        {ok, Results0} ->
            _Results = lists:sublist(Results0, Rows),
            {results_to_xml(timer:now_diff(End, Start), OrigQuery, Rows), Req, State};
        Error ->
            error_logger:error_msg("Error processing query: ~p", [Error]),
            {{halt, 500}, Req, State}
    end.

%% Internal functions
parse_query("") ->
    {error, empty_query};
parse_query(Query) ->
    qilr_parse:string(Query).

results_to_xml(ElapsedTime, OrigQuery, Results) ->
    R0 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    R0 ++ build_response_header(ElapsedTime, OrigQuery, Results).

build_response_header(ElapsedTime, OrigQuery, Rows) ->
    Template = "<response><lst name=\"responseHeader\">" ++
               "<int name=\"status\">0</int>" ++
               "<int name=\"QTime\">~s</int>" ++
               "<lst name=\"params\">" ++
               "<str name=\"q\">~s</str>" ++
               "<str name=\"rows\">~s</str>" ++
               "</lst></lst></response>",
    io_lib:format(Template, [integer_to_list(erlang:round(ElapsedTime / 1000)),
                             OrigQuery,
                             integer_to_list(Rows)]).
