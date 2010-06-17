-module(search).
-export([
    search/1, search/2,
    search_doc/1, search_doc/2,
    explain/1, explain/2,
    index_dir/1, index_dir/2,
    graph/1
]).

-define(IS_CHAR(C), ((C >= $A andalso C =< $Z) orelse (C >= $a andalso C =< $z))).
-define(DEFAULT_INDEX, "search").
-include("riak_search.hrl").

search(Q) ->
    search(?DEFAULT_INDEX, Q).

search(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Q) of
        {ok, Ops} ->
            Client:search(Index, Ops, 0, 10000, 60000);
        {error, Error} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [Q, Error]),
            {error, Error}
    end.

search_doc(Q) ->
    search_doc(?DEFAULT_INDEX, Q).

search_doc(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Q) of
        {ok, Ops} ->
            Client:search_doc(Index, Ops, 0, 10000, 60000);
        {error, Error} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [Q, Error]),
            {error, Error}
    end.

explain(Q) ->
    explain(?DEFAULT_INDEX, Q).

explain(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Q) of
        {ok, Ops} ->
            Client:explain(Index, Ops);
        {error, Error} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [Q, Error]),
            {error, Error}
    end.

graph(Q) ->
    graph(?DEFAULT_INDEX, Q).

graph(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:query_as_graph(explain(Index, Q)),
    ok.

%% Full text index the specified directory of plain text files.
index_dir(Directory) ->
    index_dir(?DEFAULT_INDEX, Directory).

%% Full text index the specified directory of plain text files.
index_dir(IndexOrSchema, Directory) ->
    {ok, Client} = riak_search:local_client(),
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    Index = Schema:name(),
    Field = Schema:default_field(),
    F = fun(BaseName, Body) ->
        Fields = [{Field, binary_to_list(Body)}],
        IdxDoc = riak_indexed_doc:new(BaseName, Index),
        IdxDoc2 = riak_indexed_doc:set_fields(Fields, IdxDoc),
        Terms = Client:parse_idx_doc(AnalyzerPid, IdxDoc2),
        [begin
            {Index, Field, Term, Value, Props} = X,
            Client:index_term(Index, Field, Term, Value, Props)
        end || X <- Terms],
        Client:store_idx_doc(IdxDoc2)
    end,
    riak_search_utils:index_recursive(F, Directory),
    qilr_analyzer:close(AnalyzerPid),
    ok.
