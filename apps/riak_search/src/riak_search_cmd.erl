-module(riak_search_cmd).
-include("riak_search.hrl").

-export([
    command/1
]).

-import(riak_search_utils, [to_list/1]).

-define(DEFAULT_INDEX, "search").


usage() ->
    Usage = "~nUsage:

    search-cmd set_schema [INDEX] SCHEMAFILE : Set schema for an index.
    search-cmd show_schema [INDEX]           : Display the schema for an index.
    search-cmd shell [INDEX]                 : Start the interactive Search shell.
    search-cmd search [INDEX] QUERY          : Perform a search.
    search-cmd search_doc [INDEX] QUERY      : Perform a document search.
    search-cmd explain [INDEX] QUERY         : Display an execution plan.
    search-cmd index [INDEX] PATH            : Index files in a path.
    search-cmd delete [INDEX] PATH           : De-index files in a path.
    search-cmd solr [INDEX] PATH             : Run the Solr file.
    ",
    io:format(Usage).

%% Set Schema
command(["set_schema", SchemaFile]) ->
    set_schema(?DEFAULT_INDEX, SchemaFile);
command(["set_schema", Index, SchemaFile]) ->
    set_schema(Index, SchemaFile);

%% Show Schema
command(["show_schema"]) ->
    show_schema(?DEFAULT_INDEX);
command(["show_schema", Index]) ->
    show_schema(Index);

%% Shell
command(["shell"]) -> 
    shell(?DEFAULT_INDEX);
command(["shell", Index]) ->
    shell(Index);

%% Search
command(["search", Query]) ->
    search(?DEFAULT_INDEX, Query);
command(["search", Index, Query]) ->
    search(Index, Query);

%% Serach Doc
command(["search_doc", Query]) ->
    search_doc(?DEFAULT_INDEX, Query);
command(["search_doc", Index, Query]) ->
    search_doc(Index, Query);

%% Explain
command(["explain", Query]) ->
    explain(?DEFAULT_INDEX, Query);
command(["explain", Index, Query]) ->
    explain(Index, Query);

%% Index
command(["index", Path]) ->
    index(?DEFAULT_INDEX, Path);
command(["index", Index, Path]) ->
    index(Index, Path);

command(["delete", Path]) ->
    delete(?DEFAULT_INDEX, Path);
command(["delete", Index, Path]) ->
    delete(Index, Path);

command(["solr", Path]) ->
    solr(?DEFAULT_INDEX, Path);
command(["solr", Index, Path]) ->
    solr(Index, Path);

command(_) ->
    usage().

set_schema(Index, SchemaFile) -> 
    IndexB = list_to_binary(Index),
    io:format("~n :: Updating schema for '~s'...~n", [IndexB]),
    case file:read_file(SchemaFile) of
        {ok, B} ->
            case riak_search_config:parse_raw_schema(B) of
                {ok, _Schema} ->
                    {ok, Client} = riak:local_client(),
                    riak_search_config:put_raw_schema(Client, IndexB, B),
                    riak_search_config:clear(),
                    io:format(" :: Done.~n");

                Error ->
                    io:format(" :: PARSING ERROR: ~p~n", [Error])
            end;
        _Error ->
            io:format(" :: ERROR: Could not read '~s'.~n", [SchemaFile])
    end.

show_schema(Index) -> 
    IndexB = list_to_binary(Index),
    io:format("~n :: Fetching schema for '~s'...~n", [Index]),
    {ok, Client} = riak:local_client(),
    case riak_search_config:get_raw_schema(Client, IndexB) of
        {ok, B} -> 
            RawSchemaBinary = B;
        undefined ->
            io:format(" :: Using default schema.~n"),
            {ok, B} = riak_search_config:get_raw_schema_default(),
            RawSchemaBinary = B
    end,
    io:format("~n~s~n", [RawSchemaBinary]).
    

shell(Index) -> 
    riak_search_shell:start(Index).

search(Index, Query) -> 
    io:format("~n :: Searching for '~s' in ~s...~n~n", [Query, Index]),
    io:format("------------------------------~n~n"),
    case search:search(Index, Query) of
        {Length, Results} ->
            F = fun(X) ->
                {DocID, Props} = X,
                IndexS = to_list(Index),
                DocIDS = to_list(DocID),
                io:format("index/id: ~s/~s~n", [IndexS, DocIDS]),
                [io:format("~p -> ~p~n", [Key, Value]) || 
                {Key, Value} <- Props],
                io:format("~n------------------------------~n~n")
            end,
            [F(X) || X <- Results],
            io:format(" :: Found ~p results.~n", [Length]);
        Other ->
            io:format(" :: ERROR: ~p", [Other])
    end.

search_doc(Index, Query) -> 
    io:format("~n :: Searching for '~s' in ~s...~n~n", [Query, Index]),
    io:format("------------------------------~n~n"),
    case search:search_doc(Index, Query) of
        {Length, MaxScore, Results} ->
            F = fun(X) ->
                %% Index.
                IndexS = to_list(X#riak_idx_doc.index),
                DocIDS = to_list(X#riak_idx_doc.id),
                io:format("index/id: ~s/~s~n", [IndexS, DocIDS]),

                %% Fields...
                [io:format("~p => ~p~n", [Key, Value]) ||
                {Key, Value} <- X#riak_idx_doc.fields],
                io:format("~n"),

                %% Properties...
                [io:format("~p -> ~p~n", [Key, Value]) ||
                {Key, Value} <- X#riak_idx_doc.props],
                io:format("------------------------------~n~n")
            end,
            [F(X) || X <- Results],
            io:format(" :: Found ~p results.~n", [Length]),
            io:format(" :: Maximum score ~p.~n", [MaxScore]);
        Other ->
            io:format("ERROR: ~p", [Other])
    end.

explain(Index, Query) -> 
    io:format("~n :: Explaining query '~s' in ~s...~n~n", [Query, Index]),
    Plan = search:explain(Index, Query),
    io:format("~p", [Plan]).

index(Index, Path) -> 
    io:format("~n :: Indexing path '~s' in ~s...~n~n", [Path, Index]),
    search:index_dir(Index, Path).

delete(Index, Path) -> 
    io:format("~n :: De-Indexing path '~s' in ~s...~n~n", [Path, Index]),
    search:delete_dir(Index, Path).

solr(Index, Path) -> 
    io:format("~n :: Indexing Solr document(s) '~s' in ~s...~n~n", [Path, Index]),
    solr_search:index_dir(Index, Path).
