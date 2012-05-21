%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

usage() ->
    Usage = "~nUsage:

    search-cmd set-schema [INDEX] SCHEMAFILE : Set schema for an index.
    search-cmd show-schema [INDEX]           : Display the schema for an index.
    search-cmd clear-schema-cache            : Empty the schema cache on all nodes.
    search-cmd search [INDEX] QUERY          : Perform a search.
    search-cmd search INDEX QUERY FILTER     : Perform a search w/ query filter.
    search-cmd search-doc [INDEX] QUERY      : Perform a document search.
    search-cmd search-doc INDEX QUERY FILTER : Perform a document search.
    search-cmd explain [INDEX] QUERY         : Display an execution plan.
    search-cmd index [INDEX] PATH            : Index files in a path.
    search-cmd delete [INDEX] PATH           : De-index files in a path.
    search-cmd solr [INDEX] PATH             : Run the Solr file.
    search-cmd install BUCKET                : Install kv/search integration hook
    search-cmd uninstall BUCKET              : Uninstall kv/search integration hook
    search-cmd test PATH                     : Run a test package~n",
    io:format(Usage).

%% Set Schema
command([CurDir, "set_schema", SchemaFile]) ->
    command([CurDir, "set-schema", SchemaFile]);
command([CurDir, "set-schema", SchemaFile]) ->
    SchemaFile1 = filename:join(CurDir, SchemaFile),
    set_schema(?DEFAULT_INDEX, SchemaFile1);
command([CurDir, "set_schema", Index, SchemaFile]) ->
    command([CurDir, "set-schema", Index, SchemaFile]);
command([CurDir, "set-schema", Index, SchemaFile]) ->
    SchemaFile1 = filename:join(CurDir, SchemaFile),
    set_schema(Index, SchemaFile1);

%% Show Schema
command([_CurDir, "show_schema"]) ->
    command([_CurDir, "show-schema"]);
command([_CurDir, "show-schema"]) ->
    show_schema(?DEFAULT_INDEX);
command([_CurDir, "show_schema", Index]) ->
    command([_CurDir, "show-schema", Index]);
command([_CurDir, "show-schema", Index]) ->
    show_schema(Index);

%% Clear Schema Cache
command([_CurDir, "clear_schema_cache"]) ->
    command([_CurDir, "clear-schema-cache"]);
command([_CurDir, "clear-schema-cache"]) ->
    clear_schema_cache();

%% Search
command([_CurDir, "search", Query]) ->
    search(?DEFAULT_INDEX, Query, "");
command([_CurDir, "search", Index, Query]) ->
    search(Index, Query, "");
command([_CurDir, "search", Index, Query, Filter]) ->
    search(Index, Query, Filter);

%% Serach Doc
command([_CurDir, "search_doc", Query]) ->
    command([_CurDir, "search-doc", Query]);
command([_CurDir, "search_doc", Index, Query]) ->
    command([_CurDir, "search-doc", Index, Query]);
command([_CurDir, "search_doc", Index, Query, Filter]) ->
    command([_CurDir, "search-doc", Index, Query, Filter]);

command([_CurDir, "search-doc", Query]) ->
    search_doc(?DEFAULT_INDEX, Query, "");
command([_CurDir, "search-doc", Index, Query]) ->
    search_doc(Index, Query, "");
command([_CurDir, "search-doc", Index, Query, Filter]) ->
    search_doc(Index, Query, Filter);

%% Explain
command([_CurDir, "explain", Query]) ->
    explain(?DEFAULT_INDEX, Query, "");
command([_CurDir, "explain", Index, Query]) ->
    explain(Index, Query, "");
command([_CurDir, "explain", Index, Query, Filter]) ->
    explain(Index, Query, Filter);

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
    io:format("~n :: Installing Riak Search <--> KV hook on bucket '~s'.~n", [Bucket]),
    riak_search_kv_hook:install(riak_search_utils:to_binary(Bucket));
command([_CurDir, "uninstall", Bucket]) ->
    io:format("~n :: Removing Riak Search <--> KV hook from bucket '~s'.~n", [Bucket]),
    riak_search_kv_hook:uninstall(riak_search_utils:to_binary(Bucket));

command(_) ->
    usage().

set_schema(Index, SchemaFile) ->
    io:format("~n :: Updating schema for '~s'...~n", [Index]),
    case file:read_file(SchemaFile) of
        {ok, B} ->
            case riak_search_config:put_raw_schema(Index, B) of
                ok ->
                    %% Immediately clear the schema cache to avoid
                    %% using old schema.
                    %%
                    %% TODO: Eventually the schema should be gossiped like
                    %% the ring
                    clear_schema_cache(),
                    io:format(" :: Done.~n");
                Error ->
                    io:format(" :: ERROR: ~p~n", [Error]),
                    erlang:exit(-1)
            end;
        _Error ->
            io:format(" :: ERROR: Could not read '~s'.~n", [SchemaFile]),
            erlang:exit(-1)
    end.

show_schema(Index) ->
    io:format("~n%% Schema for '~s'~n", [Index]),
    case riak_search_config:get_raw_schema(Index) of
        {ok, RawSchemaBinary} ->
            io:format("~n~s~n", [RawSchemaBinary]);
        Error ->
            io:format(" :: ERROR: ~p~n", [Error])
    end.

clear_schema_cache() ->
    io:format("~n :: Clearing schema caches...~n"),
    {Results, BadNodes} = riak_core_util:rpc_every_member(
                            riak_search_config, clear, [], 30000),
    io:format(" :: Cleared caches on ~b nodes~n",
              [length([ok || ok <- Results])]),
    case [ R || R <- Results, R =/= ok ] of
        [] -> ok;
        Errors ->
            io:format(" :: Received ~b errors:~n", [length(Errors)]),
            [ io:format("    ~p~n", [E]) || E <- Errors ]
    end,
    case BadNodes of
        [] -> ok;
        _ ->
            io:format(" :: ~p nodes did not respond:~n", [length(BadNodes)]),
            [ io:format("    ~p~n", [N]) || N <- BadNodes ]
    end.

search(DefaultIndex, Query, Filter) ->
    io:format("~n :: Searching for '~s' / '~s' in ~s...~n~n", [Query, Filter,
                                                               DefaultIndex]),
    io:format("------------------------------~n~n"),
    case search:search(DefaultIndex, Query, Filter) of
        {error, Reason} ->
            io:format(" :: ERROR: ~p", [Reason]);
        {Length, Results} ->
            F = fun(X) ->
                {Index, DocID, Props} = X,
                io:format("index/id: ~s/~s~n", [Index, DocID]),
                [io:format("~p -> ~p~n", [Key, Value]) ||
                {Key, Value} <- Props],
                io:format("~n------------------------------~n~n")
            end,
            [F(X) || X <- Results],
            io:format(" :: Found ~p results.~n", [Length])
    end.

search_doc(DefaultIndex, Query, Filter) ->
    io:format("~n :: Searching for '~s' / '~s' in ~s...~n~n", [Query, Filter, DefaultIndex]),
    io:format("------------------------------~n~n"),
    case search:search_doc(DefaultIndex, Query, Filter) of
        {error, Reason} ->
            io:format("ERROR: ~p", [Reason]);
        {Length, MaxScore, Results} ->
            F = fun(X) ->
                %% Index.
                Index = riak_indexed_doc:index(X),
                DocID = riak_indexed_doc:id(X),
                io:format("index/id: ~s/~s~n", [Index, DocID]),

                %% Fields...
                [io:format("~p => ~p~n", [Key, Value]) ||
                {Key, Value} <- riak_indexed_doc:fields(X)],
                io:format("~n"),

                %% Properties...
                [io:format("~p -> ~p~n", [Key, Value]) ||
                {Key, Value} <- riak_indexed_doc:props(X)],
                io:format("------------------------------~n~n")
            end,
            [F(X) || X <- Results],
            io:format(" :: Found ~p results.~n", [Length]),
            io:format(" :: Maximum score ~p.~n", [MaxScore])
    end.

explain(DefaultIndex, Query, Filter) ->
    io:format("~n :: Explaining query '~s' / '~s' in ~s...~n~n", [Query, Filter, DefaultIndex]),
    Plan = search:explain(DefaultIndex, Query, Filter),
    io:format("~p", [Plan]).

index(Index, Path) ->
    io:format("~n :: Indexing path '~s' in ~s...~n~n", [Path, Index]),
    search:index_dir(Index, Path).

delete(Index, Path) ->
    io:format("~n :: De-Indexing path '~s' in ~s...~n~n", [Path, Index]),
    search:delete_dir(Index, Path).

solr(Index, Path) ->
    io:format("~n :: Running Solr document(s) '~s' in ~s...~n", [Path, Index]),
    solr_search:index_dir(Index, Path).

test(Path) ->
    riak_search_test:test(Path).
