-module(riak_solr_wm).

-export([init/1, allowed_methods/2]).
-export([content_types_provided/2, to_text/2]).

-record(state, {method, index, q, pq, rows}).

-include_lib("webmachine/include/webmachine.hrl").

-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").

init(_) ->
    {ok, #state{}}.

allowed_methods(Req, State) ->
    {['GET', 'POST'], Req, State}.

content_types_provided(Req, State) ->
    {[{"text/plain", to_text}], Req, State}.

to_text(Req, State) ->
    {<<"Hello, world!">>, Req, State}.
