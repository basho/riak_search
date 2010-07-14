-module(riak_solr_error).

-export([log_error/2]).

log_error(Req, {error, missing_field, FieldName}) ->
    ErrMsg = lists:flatten(io_lib:format("Request references undefined field: ~p~n", [FieldName])),
    error_logger:error_msg(ErrMsg),
    Req1 = wrq:set_resp_header("Content-Type", "text/plain", Req),
    wrq:append_to_response_body(list_to_binary(ErrMsg), Req1);
log_error(Req, Error) ->
    error_logger:error_msg("Unable to parse request: ~p~n~p~n", [Error, erlang:get_stacktrace()]),
    Req.
