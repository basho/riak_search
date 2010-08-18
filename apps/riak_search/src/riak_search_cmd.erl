%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_cmd).
-include("riak_search.hrl").

-export([
    %% Used by bin/search-cmd
    command/1,

    %% Used by riak_search_test:test/N
    set_schema/2
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
    search-cmd install BUCKET                : Install kv/search integration hook
    search-cmd uninstall BUCKET              : Uninstall kv/search integration hook
    search-cmd test PATH                     : Run a test package
    ",
    io:format(Usage).

%% Set Schema
command([CurDir, "set_schema", SchemaFile]) ->
    SchemaFile1 = filename:join(CurDir, SchemaFile),
    set_schema(?DEFAULT_INDEX, SchemaFile1);
command([CurDir, "set_schema", Index, SchemaFile]) ->
    SchemaFile1 = filename:join(CurDir, SchemaFile),
    set_schema(Index, SchemaFile1);

%% Show Schema
command([_CurDir, "show_schema"]) ->
    show_schema(?DEFAULT_INDEX);
command([_CurDir, "show_schema", Index]) ->
    show_schema(Index);

%% Shell
command([_CurDir, "shell"]) -> 
    shell(?DEFAULT_INDEX);
command([_CurDir, "shell", Index]) ->
    shell(Index);

%% Search
command([_CurDir, "search", Query]) ->
    search(?DEFAULT_INDEX, Query);
command([_CurDir, "search", Index, Query]) ->
    search(Index, Query);

%% Serach Doc
command([_CurDir, "search_doc", Query]) ->
    search_doc(?DEFAULT_INDEX, Query);
command([_CurDir, "search_doc", Index, Query]) ->
    search_doc(Index, Query);

%% Explain
command([_CurDir, "explain", Query]) ->
    explain(?DEFAULT_INDEX, Query);
command([_CurDir, "explain", Index, Query]) ->
    explain(Index, Query);

%% Index
command([CurDir, "index", Path]) ->
    Path1 = filename:join(CurDir, Path),
    index(?DEFAULT_INDEX, Path1);
command([CurDir, "index", Index, Path]) ->
    Path1 = filename:join(CurDir, Path),
    index(Index, Path1);

command([CurDir, "delete", Path]) ->
    Path1 = filename:join(CurDir, Path),
    delete(?DEFAULT_INDEX, Path1);
command([CurDir, "delete", Index, Path]) ->
    Path1 = filename:join(CurDir, Path),
    delete(Index, Path1);

command([CurDir, "solr", Path]) ->
    Path1 = filename:join(CurDir, Path),
    solr(?DEFAULT_INDEX, Path1);
command([CurDir, "solr", Index, Path]) ->
    Path1 = filename:join(CurDir, Path),
    solr(Index, Path1);

command([CurDir, "test", Path]) ->
    Path1 = filename:join(CurDir, Path),
    test(Path1);

command([_CurDir, "install", Bucket]) ->
    riak_search_kv_hook:install(riak_search_utils:to_binary(Bucket));
command([_CurDir, "uninstall", Bucket]) ->
    riak_search_kv_hook:uninstall(riak_search_utils:to_binary(Bucket));

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
                    case riak_search_config:put_raw_schema(Client, IndexB, B) of
                        ok ->
                            ok = riak_search_config:clear(),
                            io:format(" :: Done.~n");
                        Error ->
                            io:format(" :: STORAGE ERROR: ~p~n", [Error]),
                            erlang:exit(-1)
                    end;
                Error ->
                    io:format(" :: PARSING ERROR: ~p~n", [Error]),
                    erlang:exit(-1)
            end;
        _Error ->
            io:format(" :: ERROR: Could not read '~s'.~n", [SchemaFile]),
            erlang:exit(-1)
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

search(DefaultIndex, Query) -> 
    io:format("~n :: Searching for '~s' in ~s...~n~n", [Query, DefaultIndex]),
    io:format("------------------------------~n~n"),
    case search:search(DefaultIndex, Query) of
        {Length, Results} ->
            F = fun(X) ->
                {Index, DocID, Props} = X,
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

search_doc(DefaultIndex, Query) -> 
    io:format("~n :: Searching for '~s' in ~s...~n~n", [Query, DefaultIndex]),
    io:format("------------------------------~n~n"),
    case search:search_doc(DefaultIndex, Query) of
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

explain(DefaultIndex, Query) -> 
    io:format("~n :: Explaining query '~s' in ~s...~n~n", [Query, DefaultIndex]),
    Plan = search:explain(DefaultIndex, Query),
    io:format("~p", [Plan]).

index(Index, Path) -> 
    io:format("~n :: Indexing path '~s' in ~s...~n~n", [Path, Index]),
    riak_search_dir_indexer:index(Index, Path).

delete(Index, Path) -> 
    io:format("~n :: De-Indexing path '~s' in ~s...~n~n", [Path, Index]),
    search:delete_dir(Index, Path).

solr(Index, Path) -> 
    io:format("~n :: Running Solr document(s) '~s' in ~s...~n", [Path, Index]),
    solr_search:index_dir(Index, Path).

test(Path) ->
    riak_search_test:test(Path).
