-module(riak_solr_error).

-export([log_error/2]).

log_error(Req, {error, missing_field, FieldName}) ->
    ErrMsg = lists:flatten(
               io_lib:format("Request references undefined field: ~p~n",
                             [FieldName])),
    lager:error(ErrMsg),
    Req1 = wrq:set_resp_header("Content-Type", "text/plain", Req),
    wrq:append_to_response_body(list_to_binary(ErrMsg), Req1);
log_error(Req, Error) ->
    lager:error("Unable to parse request: ~p", [Error]),
    Req.
