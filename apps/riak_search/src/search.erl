-module(search).
-export([
    search/1, search/2,
    search_doc/1, search_doc/2,
    explain/1, explain/2,
    index_doc/1, index_doc/2,
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
    Client:search(Index, Q).

search_doc(Q) ->
    search_doc(?DEFAULT_INDEX, Q).

search_doc(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:search_doc(Index, Q).

explain(Q) ->
    explain(?DEFAULT_INDEX, Q).

explain(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:explain(Index, Q).

graph(Q) ->
    graph(?DEFAULT_INDEX, Q).

graph(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:query_as_graph(Client:explain(Index, Q)),
    ok.

%% Full text index the specified file or directory, which is expected
%% to contain a solr formatted file.
index_doc(Directory) ->
    index_doc(?DEFAULT_INDEX, Directory).

%% Full text index the specified file or directory, which is expected
%% to contain a solr formatted file.
index_doc(IndexOrSchema, Directory) ->
    {ok, Client} = riak_search:local_client(),
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    Schema = to_schema(IndexOrSchema),
    F = fun(_BaseName, Body) ->
        Client:index_doc(Schema, Body)
    end,
    index_recursive(F, Directory),
    qilr_analyzer:close(AnalyzerPid),
    ok.
        
%% Full text index the specified directory of plain text files.
index_dir(Directory) ->
    index_dir(?DEFAULT_INDEX, Directory).

%% Full text index the specified directory of plain text files.
index_dir(IndexOrSchema, Directory) ->
    {ok, Client} = riak_search:local_client(),
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    Schema = to_schema(IndexOrSchema),
    Index = Schema:name(),
    Field = Schema:default_field(),
    F = fun(BaseName, Body) ->
        Fields = [{Field, binary_to_list(Body)}],
        IdxDoc = riak_indexed_doc:new(BaseName, Index),
        IdxDoc2 = riak_indexed_doc:set_fields(Fields, IdxDoc),
        Client:index_idx_doc(AnalyzerPid, IdxDoc2)
    end,
    index_recursive(F, Directory),
    qilr_analyzer:close(AnalyzerPid),
    ok.

%% @private Recursively index the provided file or directory, running
%% the specified function on the body of any files.
index_recursive(Callback, Directory) ->
    io:format(" :: Indexing directory: ~s~n", [Directory]),
    Files = filelib:wildcard(Directory),
    io:format(" :: Found ~p files...~n", [length(Files)]),

    F = fun(File) -> index_recursive_file(Callback, File) end,
    plists:map(F, Files, {processes, 4}),
    ok.

%% Full-text index the specified file.
index_recursive_file(Callback, File) ->
    Basename = filename:basename(File),
    io:format(" :: Indexing file: ~s~n", [Basename]),
    case file:read_file(File) of
        {ok, Bytes} ->
            Callback(Basename, Bytes);
        {error, eisdir} ->
            io:format("following directory: ~p~n", [File]),
            index_recursive(Callback, filename:join(File, "*"));
        Err ->
            io:format("index_file(~p): error: ~p~n", [File, Err])
    end.

to_schema(IndexOrSchema) ->
    case is_tuple(IndexOrSchema) andalso element(1, IndexOrSchema) == riak_solr_schema of
        true  -> 
            IndexOrSchema;
        false -> 
            {ok, Schema} = riak_solr_config:get_schema(IndexOrSchema),
            Schema
    end.
