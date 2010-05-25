-module(search).
-export([search/1,
         doc_search/1,
         explain/1,
         graph/1,
         index_dir/1
]).
-define(IS_CHAR(C), ((C >= $A andalso C =< $Z) orelse (C >= $a andalso C =< $z))).
-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").
-define(DEFAULT_FACETS, ["search.color", "search.direction", "search.subterm", "search.subterm"]).
-include("riak_search.hrl").

search(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:search("search", Q).

doc_search(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:doc_search("search", Q).

explain(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:explain("search", Q).

graph(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:query_as_graph(Client:explain("search", Q)),
    ok.

%% Full-text index the files within the specified directory.
index_dir(Directory) ->
    {ok, Schema} = riak_solr_config:get_schema(?DEFAULT_INDEX),
    index_dir(Directory, Schema).

index_dir(Directory, Schema) ->
    io:format(" :: Indexing directory: ~s~n", [Directory]),

    %% Get a list of files in the directory, and index them...
    Directory1 = case string:str(Directory, "*") of
        0 -> filename:join([Directory, "*"]);
        _ -> Directory
    end,
    Files = filelib:wildcard(Directory1),
    io:format(" :: Found ~p files...~n", [length(Files)]),

    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    F = fun(File) -> index_file(AnalyzerPid, File, Schema) end,
    plists:map(F, Files, {processes, 4}),
    qilr_analyzer:close(AnalyzerPid),
    ok.

%% Full-text index the specified file.
index_file(AnalyzerPid, File, Schema) ->
    Index = Schema:name(),
    Field = Schema:default_field(),
    
    Basename = filename:basename(File),
    io:format(" :: Indexing file: ~s~n", [Basename]),
    {ok, SearchClient} = riak_search:local_client(),
    case file:read_file(File) of
        {ok, Bytes} ->
            %% Build the document...
            Fields = [{Field, binary_to_list(Bytes)}],
            IdxDoc = riak_indexed_doc:new(Basename, Index),
            IdxDoc2 = riak_indexed_doc:set_fields(Fields, IdxDoc),

            %% Index the document...
            SearchClient:index_doc(AnalyzerPid, IdxDoc2);
        {error, eisdir} ->
            io:format("following directory: ~p~n", [File]),
            index_dir(File, Schema);
        Err ->
            io:format("index_file(~p, ~p, ~p): error: ~p~n",
                [File, Index, Field, Err])
    end,
    ok.
