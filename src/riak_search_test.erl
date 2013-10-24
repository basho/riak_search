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
-define(MD_CTYPE, <<"content-type">>).

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
%% {index_bucket, Bucket} : Enable indexing hook for bucket
%%
%% {putobj, Bucket, Key, ContentType, Value} : Put a riak object
%%
%% {delobj, Bucket, Key} : Delete a riak object
%%
%% {exists, Bucket, Key, boolean()} : Check whether key exists.
%%
%% {error, Test, Type, Error} : Run the `Test' spec expecting an
%% exception with `Type':`Error'.
%%
%% Validators:
%% {length, N} : Make sure there are exactly N results.
%% {property, Key, Value} : Make sure the specified property exists in all results.
%%
%% For simplicity, all tests run against the "test" index.


%% Run the test package at Path.
%% Returns ok if the test passed, error if it didn't.
test(Path) ->
    io:format("~n :: Running Test Package '~s'...~n", [filename:basename(Path)]),
    Path1 = filename:join(Path, "script.def"),
    case file:consult(Path1) of
        {ok, Terms} ->
            case test_inner(Terms, Path) of
                true -> ok;
                false -> error
            end;
        {error, Error} ->
            io:format(" :: ERROR - Could not read '~s' : ~p~n", [Path1, Error]),
            error
    end.

test_inner([], _Root) ->
    true;

test_inner([Op|Ops], Root) ->
    Bool1 = test_inner(Op, Root),
    Bool2 = test_inner(Ops, Root),
    Bool1 andalso Bool2;

test_inner({echo, Text}, _) ->
    Tokens = string:tokens(Text, "\n"),
    Tokens1 = [string:strip(X, both) || X <- Tokens],
    io:format("~n"),
    [io:format(" :: ~s~n", [X]) || X <- Tokens1, X /= ""],
    true;

test_inner({sleep, Seconds}, _) ->
    io:format("~n :: Sleeping ~p second(s)...~n", [Seconds]),
    timer:sleep(Seconds * 1000),
    true;

test_inner({schema, Schema}, Root) ->
    riak_search_cmd:set_schema(?TEST_INDEX, filename:join(Root, Schema)),
    true;

test_inner({schema, Node, Schema}, Root) ->
    rpc:call(Node, riak_search_cmd, set_schema,
             [?TEST_INDEX, filename:join(Root, Schema)]),
    true;

test_inner({solr, Path}, Root) ->
    io:format("~n :: Running Solr document(s) '~s'...~n", [Path]),
    solr_search:index_dir(?TEST_INDEX, filename:join(Root, Path)),
    true;

test_inner({index, Path}, Root) ->
    io:format("~n :: Indexing path '~s'...~n~n", [Path]),
    search:index_dir(?TEST_INDEX, filename:join(Root, Path)),
    true;

test_inner({delete, Path}, Root) ->
    io:format("~n :: De-Indexing path '~s'...~n~n", [Path]),
    search:delete_dir(?TEST_INDEX, filename:join(Root, Path)),
    true;

test_inner({error, Test, Type, Error}, _Root) ->
    try test_inner(Test, _Root)
    catch
        Type:Error ->
            io:format("~n    [√] PASS ERROR » ~p~n", [Test]),
            true;
        Type2:Error2 ->
            io:format("~n    [ ] FAIL ERROR » ~p ~p ~p~n", [Test, Type2, Error2]),
            false
    end;

test_inner({search, Query, Validators}, _Root) ->
    test_inner({search_node, node(), Query, "", Validators}, _Root);

test_inner({search, Query, Filter, Validators}, _Root) ->
    test_inner({search_node, node(), Query, Filter, Validators}, _Root);

test_inner({search_node, Node, Query, Filter, Validators}, _Root) ->
    Args = case Filter of
               [] -> [?TEST_INDEX, Query];
               _ -> [?TEST_INDEX, Query, Filter]
           end,
    case rpc:call(Node, search, search, Args) of
        {badrpc, Reason} ->
                    io:format("~n    [ ] FAIL QUERY » ~s~n", [Query]),
                    io:format("        - BADRPC: ~p~n", [Reason]),
                    false;
        {Length, Results} ->
            case validate_results(Length, Results, Validators) of
                pass ->
                    io:format("~n    [√] PASS QUERY » ~s~n", [Query]),
                    true;
                {fail, Errors} ->
                    io:format("~n    [ ] FAIL QUERY » ~s~n", [Query]),
                    [io:format("        - ~s~n", [X]) || X <- Errors],
                    false
            end;
        Error ->
            io:format("~n    [ ] FAIL QUERY » ~s~n", [Query]),
            io:format("        - ERROR1: ~p~n", [Error]),
            false
    end;

test_inner({solr_select, Params, Validators}, _Root) ->
    test_inner({solr_select, Params, 200, Validators}, _Root);

test_inner({solr_select, Params, Expect, Validators}, _Root) ->
    {Host, Port} = hd(app_helper:get_env(riak_api, http)),
    test_inner({solr_select, Host, Port, Params, Expect, Validators}, _Root);

test_inner({solr_select, Host, Port, Params, Validators}, _Root) ->
    test_inner({solr_select, Host, Port, Params, 200, Validators}, _Root);

test_inner({solr_select, Host, Port, Params, Expected, Validators}, _Root) ->
    inets:start(),
    Query = proplists:get_value(q, Params),
    QS = to_querystring(Params),
    Url = io_lib:format("http://~s:~p/solr/~s/select?~s", [Host, Port, ?TEST_INDEX, QS]),
    try httpc:request(lists:flatten(Url)) of
        {ok, {{_, 200, _}, _, Body}} ->
            Format = proplists:get_value(wt, Params, xml),
            {Length, Results} = parse_solr_select_result(Format, Body),
            case validate_results(Length, Results, Validators) of
                pass ->
                    io:format("~n    [√] PASS SOLR SELECT » ~s:~p ~s (~s)~n",
                              [Host, Port, Query, QS]),
                    true;
                {fail, Errors} ->
                    io:format("~n    [ ] FAIL SOLR SELECT » ~s:~p ~s (~s)~n",
                              [Host, Port, Query, QS]),
                    [io:format("        - ~s~n", [X]) || X <- Errors],
                    false
            end;
        {ok, {{_, Status, _}, _, _}} when Status == Expected ->
            %% If non-200 assuming we don't care about the body
            io:format("~n    [√] PASS SOLR SELECT » ~s:~p ~s (~s)~n",
                      [Host, Port, Query, QS]),
            true;
        {ok, {{_, Status, _}, _, _}} ->
            io:format("~n    [ ] FAIL SOLR SELECT » ~s:~p ~s (~s)~n",
                      [Host, Port, Query, QS]),
            io:format("        - Status ~p from ~s~n", [Status, Url]),
            false;
        {error, Error} ->
            io:format("~n    [ ] FAIL SOLR SELECT » ~s:~p ~s (~s)~n",
                      [Host, Port, Query, QS]),
            io:format("        - ERROR: ~p~n", [Error]),
            false
    catch
        _Type : Error ->
            io:format("~n    [ ] FAIL SOLR SELECT » ~s:~p ~s (~s)~n",
                      [Host, Port, Query, QS]),
            io:format("        - ERROR: ~p : ~p~n", [Error, erlang:get_stacktrace()]),
            false
    end;

test_inner({solr_update, Path, Params}, Root) ->
    io:format("~n :: Running Solr Update '~s' (via HTTP)...~n", [Path]),

    %% Run the update command...
    inets:start(),
    case file:read_file(filename:join(Root, Path)) of
        {ok, Bytes} ->
            {Hostname, Port} = hd(app_helper:get_env(riak_api, http)),
            QueryString = to_querystring(Params),
            Url = io_lib:format("http://~s:~p/solr/~s/update?~s", [Hostname, Port, ?TEST_INDEX, QueryString]),
            Req = {lists:flatten(Url), [], "text/xml", Bytes},
            try httpc:request(post, Req, [], []) of
                {ok, {{_, 200, _}, _, _}} ->
                    io:format("~n :: Success!"),
                    true;
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

test_inner({index_bucket, Bucket}, _) ->
    ok = riak_search_kv_hook:install(Bucket),
    true;

test_inner({mapred, Bucket, Search, Phases, Validators}, _) ->
    SearchInput = {modfun, riak_search, mapred_search, [Bucket, Search]},
    try riak_kv_mrc_pipe:mapred(SearchInput, Phases) of
        {ok, Results} ->
            case validate_results(length(Results), Results, Validators) of
                pass ->
                    io:format("~n    [√] PASS MAPRED QUERY » ~s~n", [Search]),
                    true;
                {fail, Errors} ->
                    io:format("~n    [ ] FAIL MAPRED QUERY » ~s~n", [Search]),
                    [io:format("        - ~s~n", [X]) || X <- Errors],
                    false
            end;
        Error ->
            io:format("~n    [ ] FAIL MAPRED QUERY » ~s~n", [Search]),
            io:format("        - ERROR1: ~p~n", [Error]),
            false
    catch
        _Type : Error ->
            io:format("~n    [ ] FAIL MAPRED QUERY » ~s~n", [Search]),
            io:format("        - ERROR2: ~p : ~p~n", [Error, erlang:get_stacktrace()]),
            false
    end;

test_inner({mapred, Bucket, Search, Filter, Phases, Validators}, _) ->
    SearchInput = {modfun, riak_search, mapred_search, [Bucket, Search, Filter]},
    try riak_kv_mrc_pipe:mapred(SearchInput, Phases) of
        {ok, Results} ->
            case validate_results(length(Results), Results, Validators) of
                pass ->
                    io:format("~n    [√] PASS MAPRED QUERY » ~s/~s~n", [Search, Filter]),
                    true;
                {fail, Errors} ->
                    io:format("~n    [ ] FAIL MAPRED QUERY » ~s/~s~n", [Search, Filter]),
                    [io:format("        - ~s~n", [X]) || X <- Errors],
                    false
            end;
        Error ->
            io:format("~n    [ ] FAIL MAPRED QUERY » ~s/~s~n", [Search, Filter]),
            io:format("        - ERROR1: ~p~n", [Error]),
            false
    catch
        _Type : Error ->
            io:format("~n    [ ] FAIL MAPRED QUERY » ~s/~s~n", [Search, Filter]),
            io:format("        - ERROR2: ~p : ~p~n", [Error, erlang:get_stacktrace()]),
            false
    end;

test_inner({putobj, Bucket, Key, Ct, Value}, _) ->
    RObj = riak_object:new(Bucket, Key, Value, Ct),
    {ok,C} = riak:local_client(),
    ok = C:put(RObj),
    true;

test_inner({exists, B, K, Bool}, _) ->
    {ok, C} = riak:local_client(),
    case C:get(B, K) of
        {ok, _} ->
            if Bool == true ->
                    io:format("~n    [√] PASS EXISTS » ~s/~s~n", [B, K]),
                    true;
               true ->
                    io:format("~n    [ ] FAIL NON-EXISTS » ~s/~s~n", [B, K]),
                    false
               end;
        _ ->
            if Bool == false ->
                    io:format("~n    [√] PASS NON-EXISTS » ~s/~s~n", [B, K]),
                    true;
               true ->
                    io:format("~n    [ ] FAIL EXISTS » ~s/~s~n", [B, K]),
                    false
            end
    end;

test_inner({delobj, Bucket, Key}, _) ->
    {ok,C} = riak:local_client(),
    case C:delete(Bucket, Key) of
        ok ->
            true;
        {error, notfound} ->
            true;
        _ ->
            false
    end;

test_inner(Other, _Root) ->
    io:format("Unexpected test step: ~p root ~p~n", [Other, _Root]),
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
    F = fun({_, _, Props}) ->
        lists:member({Key, Value}, Props) orelse
        lists:member(Value, proplists:get_value(Key, Props, []))
    end,
    case (length(Results) > 0) andalso lists:all(F, Results) of
        true ->
            pass;
        false ->
            {fail, io_lib:format("Missing property: ~p -> ~p", [Key, Value])}
    end;
validate_results_inner(_Length, Results, {doc, DocID, Validator}) ->
    case lists:keyfind(DocID, 2, Results) of
        false ->
            {fail, io_lib:format("Missing doc ID in results: ~p", [DocID])};
        Result ->
            validate_results_inner(1, [Result], Validator)
    end;
validate_results_inner(Length, Results, {docids, DocIDs}) ->
    validate_results_inner(Length, Results, {result, DocIDs});
validate_results_inner(_Length, Results, {result, ExpectedResult}) ->
    %% Check the returned docids exactly matches the list provided
    case Results of
        ExpectedResult ->
            pass;
        _ ->
            {fail, io_lib:format("Results do not match expected result\n" ++
                                 "    Expected: ~p\n" ++
                                 "    Results:  ~p\n", [ExpectedResult, Results])}
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
    Results = [strip(X#xmlText.value) || X <- Matches],
    {length(Results), Results}.

strip(S) ->
    string:strip(string:strip(S, both), both, $\n).

to_querystring(Params) ->
    %% Turn params into a querystring...
    F = fun(K, V) ->
        mochiweb_util:quote_plus(K) ++ "=" ++ mochiweb_util:quote_plus(V)
    end,
    QSParts = [F(K, V) || {K, V} <- Params],
    string:join(QSParts, "&").

