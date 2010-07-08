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
    search-cmd test PATH                     : Run a test package
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

command(["test", Path]) ->
    test(Path);

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
    io:format("~n :: Running Solr document(s) '~s' in ~s...~n", [Path, Index]),
    solr_search:index_dir(Index, Path).

test(Path) ->
    io:format("~n :: Running Test Package '~s'...~n", [filename:basename(Path)]),
    Path1 = filename:join(Path, "script.def"),
    case file:consult(Path1) of
        {ok, Terms} -> 
            test_inner(Terms, Path);
        {error, Error} ->
            io:format(" :: ERROR - Could not read '~s' : ~p~n", [Path1, Error])
    end.
        

-define(TEST_INDEX, "test").
test_inner([], _Root) -> 
    ok;

test_inner([Op|Ops], Root) ->
    test_inner(Op, Root),
    test_inner(Ops, Root);

test_inner({echo, Text}, _) ->
    Tokens = string:tokens(Text, "\n"),
    Tokens1 = [string:strip(X, both) || X <- Tokens],
    io:format("~n"),
    [io:format(" :: ~s~n", [X]) || X <- Tokens1, X /= ""];

test_inner({schema, Schema}, Root) ->
    set_schema(?TEST_INDEX, filename:join(Root, Schema));

test_inner({solr, Path}, Root) ->
    solr(?TEST_INDEX, filename:join(Root, Path));

test_inner({index, Path}, Root) ->
    index(?TEST_INDEX, filename:join(Root, Path));

test_inner({delete, Path}, Root) ->
    delete(?TEST_INDEX, filename:join(Root, Path));

test_inner({search, Query, Validators}, _Root) ->
    io:format("~n :: QUERY '~s'~n", [Query]),
    case search:search(?TEST_INDEX, Query) of
        {Length, Results} ->
            case validate_results(Length, Results, Validators) of
                true -> 
                    io:format(" :: PASS.~n");
                false ->
                    io:format(" :: FAIL!~n")
            end;
        Other ->
            io:format(" :: ERROR RUNNING QUERY: ~p~n", [Other])
    end;

test_inner(Other, _Root) ->
    throw({unexpected_test_step, Other}).

validate_results(_Length, _Results, []) -> 
    true;
validate_results(Length, Results, [Validator|Validators]) ->
    validate_results(Length, Results, Validator) andalso
    validate_results(Length, Results, Validators);
validate_results(Length, _Results, {length, ValidLength}) ->
    case Length == ValidLength of
        true -> 
            true;
        false ->
            io:format(" :: - Expected length ~p, got ~p!~n", [Length, ValidLength]),
            false
    end;
validate_results(_Length, _Results, Other) ->
    throw({unexpected_test_validator, Other}).

