%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_test).
-include("riak_search.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-export([test/1]).
-define(TEST_INDEX, "test").

%% This module runs the semi-automated test modules found in the
%% ./tests directory. Each module contains a script.def file
%% containing the test script. 
%%
%% The following test steps are allowed:
%% {echo, Text}     : Echo text to the console.
%% {sleep, Seconds} : Sleep for the specified number of seconds.
%% {schema, Schema} : Set the schema.
%% {solr, Path}     : Execute the provided Solr script through the cmdline interface.
%% {index, Path}    : Index the documents in the provided path.
%% {delete, Path}   : De-Index the documents in the provided path.
%% {search, Query, Validators} : Search on the query, run the validators.
%% {solr_select, Params, Validators} : Search on the query, run the validators.
%% {solr_update, Params, Path} : Execute the provided Solr script through the HTTP interface.
%%
%% Validators:
%% {length, N} : Make sure there are exactly N results.
%% {property, Key, Value} : Make sure the specified property exists in all results.
%%
%% For simplicity, all tests run against the "test" index.


%% Run the test package at Path.
test(Path) ->
    io:format("~n :: Running Test Package '~s'...~n", [filename:basename(Path)]),
    Path1 = filename:join(Path, "script.def"),
    case file:consult(Path1) of
        {ok, Terms} -> 
            test_inner(Terms, Path);
        {error, Error} ->
            io:format(" :: ERROR - Could not read '~s' : ~p~n", [Path1, Error])
    end.

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

test_inner({sleep, Seconds}, _) ->
    io:format("~n :: Sleeping ~p second(s)...~n", [Seconds]),
    timer:sleep(Seconds * 1000);

test_inner({schema, Schema}, Root) ->
    riak_search_cmd:set_schema(?TEST_INDEX, filename:join(Root, Schema));

test_inner({solr, Path}, Root) ->
    io:format("~n :: Running Solr document(s) '~s'...~n", [Path]),
    solr_search:index_dir(?TEST_INDEX, filename:join(Root, Path));

test_inner({index, Path}, Root) ->
    io:format("~n :: Indexing path '~s'...~n~n", [Path]),
    search:index_dir(?TEST_INDEX, filename:join(Root, Path));

test_inner({delete, Path}, Root) ->
    io:format("~n :: De-Indexing path '~s'...~n~n", [Path]),
    search:delete_dir(?TEST_INDEX, filename:join(Root, Path));

test_inner({search, Query, Validators}, _Root) ->
    try search:search(?TEST_INDEX, Query) of
        {Length, Results} ->
            case validate_results(Length, Results, Validators) of
                pass -> 
                    io:format("~n    [√] PASS QUERY » ~s~n", [Query]);
                {fail, Errors} ->
                    io:format("~n    [ ] FAIL QUERY » ~s~n", [Query]),
                    [io:format("        - ~s~n", [X]) || X <- Errors]
            end;
        Error ->
            io:format("~n    [ ] FAIL QUERY » ~s~n", [Query]),
            io:format("        - ERROR: ~p~n", [Error])
    catch 
        _Type : Error ->
            io:format("~n    [ ] FAIL QUERY » ~s~n", [Query]),
            io:format("        - ERROR: ~p : ~p~n", [Error, erlang:get_stacktrace()])
    end;
test_inner({solr_select, Params, Validators}, _Root) ->
    %% Run the query...
    inets:start(),
    Port = app_helper:get_env(riak_core, web_port),
    Query = proplists:get_value(q, Params),
    QS = to_querystring(Params),
    Url = io_lib:format("http://127.0.0.1:~p/solr/~s/select?~s", [Port, ?TEST_INDEX, QS]),
    try http:request(lists:flatten(Url)) of
        {ok, {{_, 200, _}, _, Body}} ->
            Format = proplists:get_value(wt, Params, xml),
            {Length, Results} = parse_solr_select_result(Format, Body),
            case validate_results(Length, Results, Validators) of
                pass -> 
                    io:format("~n    [√] PASS SOLR SELECT » ~s (~s)~n", [Query, QS]);
                {fail, Errors} ->
                    io:format("~n    [ ] FAIL SOLR SELECT » ~s (~s)~n", [Query, QS]),
                    [io:format("        - ~s~n", [X]) || X <- Errors]
            end;
        {ok, {{_, Status, _}, _, _}} ->
            io:format("~n    [ ] FAIL SOLR SELECT » ~s (~s)~n", [Query, QS]),
            io:format("        - Status ~p from ~s~n", [Status, Url]);
        {error, Error} ->
            io:format("~n    [ ] FAIL SOLR SELECT » ~s (~s)~n", [Query, QS]),
            io:format("        - ERROR: ~p~n", [Error])
    catch 
        _Type : Error ->
            io:format("~n    [ ] FAIL SOLR SELECT » ~s (~s)~n", [Query, QS]),
            io:format("        - ERROR: ~p : ~p~n", [Error, erlang:get_stacktrace()])
    end;
test_inner({solr_update, Path, Params}, Root) ->
    io:format("~n :: Running Solr Update '~s' (via HTTP)...~n", [Path]),

    %% Run the update command...
    inets:start(),
    case file:read_file(filename:join(Root, Path)) of
        {ok, Bytes} ->
            Port = app_helper:get_env(riak_core, web_port),
            QueryString = to_querystring(Params),
            Url = io_lib:format("http://127.0.0.1:~p/solr/~s/update?~s", [Port, ?TEST_INDEX, QueryString]),
            Req = {lists:flatten(Url), [], "text/xml", Bytes},
            try http:request(post, Req, [], []) of
                {ok, {{_, 200, _}, _, _}} ->
                    io:format("~n :: Success!");
                {ok, {{_, Status, _}, _, _}} ->
                    io:format("~n :: Solr Update Failed! (Status: ~p, Url: ~s)~n", [Status, Url]),
                    throw({solr_update_error, status, Status});
                {error, Error} ->
                    io:format("~n :: Solr Update Failed! (HTTP Error: ~p)~n", [Error]),
                    throw({solr_update_error, Error})
            catch  
                _Type : Error ->
                    io:format("~n :: Solr Update Failed! (Exception: ~p)~n", [Error]),
                    throw({solr_update_error, Error})
            end;
        {error, Error} ->
            io:format("~n :: Solr Update Failed! (Error: ~p)~n", [Error]),
            throw({solr_update_error, Error})
    end;
test_inner({solr_update, Path}, Root) ->
    test_inner({solr_update, Path, []}, Root);
test_inner(Other, _Root) ->
    io:format("Unexpected test step: ~p~n", [Other]),
    throw({unexpected_test_step, Other}).

validate_results(Length, Results, Validators) ->
    L = validate_results_inner(Length, Results, Validators),
    case [X || X <- lists:flatten(L), X /= pass] of
        []      -> pass;
        Errors  -> {fail, [X || {fail, X} <- Errors]}
    end.
validate_results_inner(_Length, _Results, []) -> 
    [];
validate_results_inner(Length, Results, [Validator|Validators]) ->
    [validate_results_inner(Length, Results, Validator)|
        validate_results_inner(Length, Results, Validators)];
validate_results_inner(Length, _Results, {length, ValidLength}) ->
    case Length == ValidLength of
        true -> 
            pass;
        false ->
            {fail, io_lib:format("Expected ~p result(s), got ~p!", [ValidLength, Length])}
    end;
validate_results_inner(_Length, Results, {property, Key, Value}) ->
    KeyL = riak_search_utils:to_list(Key),
    ValueB = riak_search_utils:to_binary(Value),
    F = fun({_, Props}) ->
        lists:member({KeyL, ValueB}, Props)
    end,
    case lists:all(F, Results) of
        true -> 
            pass;
        false ->
            {fail, io_lib:format("Missing property: ~s -> ~s", [Key, Value])}
    end;
validate_results_inner(_Length, _Results, Other) ->
    io:format("Unexpected test validator: ~p~n", [Other]),
    throw({unexpected_test_validator, Other}).

parse_solr_select_result(json, Body) ->
    {struct, JSON} = mochijson2:decode(Body),
    {struct, Response} = proplists:get_value(<<"response">>, JSON),
    Docs = proplists:get_value(<<"docs">>, Response),
    F = fun({struct, Doc}) ->
        proplists:get_value(<<"id">>, Doc)
    end,
    Results = [F(X) || X <- Docs],
    {length(Results), Results};
parse_solr_select_result(xml, Body) -> 
    {XMLDoc, _Rest} = xmerl_scan:string(Body),
    Matches = xmerl_xpath:string("//response/result/doc/str[@name='id']/text()", XMLDoc),
    Results = [X#xmlText.value || X <- Matches],
    {length(Results), Results}.

to_querystring(Params) ->
    %% Turn params into a querystring...
    F = fun(K, V) ->
        mochiweb_util:quote_plus(K) ++ "=" ++ mochiweb_util:quote_plus(V)
    end,
    QSParts = [F(K, V) || {K, V} <- Params],
    string:join(QSParts, "&").

