%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_riaksearch).

-export([
    new/1,
    run/4,
    valgen/3,
    file_to_array/2
]).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).
-record(state, { nodes, fields, terms, queries, 'query', expected}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    MyNode = basho_bench_config:get(riaksearch_node),
    Cookie = basho_bench_config:get(riaksearch_cookie, riak),
    net_kernel:start([MyNode]),
    erlang:set_cookie(node(), Cookie),

    %% Get the nodes...
    Nodes = basho_bench_config:get(riaksearch_remotenodes),
    F = fun(Node) ->
        io:format("Testing connectivity to Node: ~p~n", [Node]),
        pong == net_adm:ping(Node) orelse throw({could_not_reach_node, Node})
    end,
    [F(X) || X <- Nodes],

    %% Load the field array...
    FieldFile = basho_bench_config:get(riaksearch_fieldfile, undefined),
    %% FieldArray = file_to_array(FieldFile, 10),
    FieldArray = file_to_array(FieldFile),
    
    %% Load the word array...
    TermFile = basho_bench_config:get(riaksearch_termfile, undefined),
    %% TermArray = file_to_array(TermFile, 100),
    TermArray = file_to_array(TermFile),

    Query = basho_bench_config:get(riaksearch_query, undefined),
    E = basho_bench_config:get(riaksearch_expected, undefined),
    
    State = #state { 
        nodes=Nodes,
        fields=FieldArray, 
        terms=TermArray,
        queries=queue:new(),
        'query'=Query,
        expected=E
    },
    {ok, State}.

run('index', KeyGen, ValueGen, State) ->
    %% Make the index call...
    Node = choose(State#state.nodes),
    F = fun(_, {DocsAccIn, _}) ->
                ID = list_to_binary(KeyGen()),
                NewRawFields = ValueGen(State#state.fields, State#state.terms),
                Fields = [{list_to_binary(X), list_to_binary(string:join(Y, " "))} || {X, Y} <- NewRawFields],
                {[{ID, Fields}|DocsAccIn], NewRawFields}
        end,
    %% Get a list of docs, plus the last RawFields entry (we only need
    %% one, so accumulating would slow us down.).
    {Docs, RawFields} = lists:foldl(F, {[], []}, lists:seq(1, 10)),
    ok = rpc:call(Node, search, index_docs, [Docs]),

    %% Always keep a buffer of things to query.
    Queries = State#state.queries,
    case queue:len(Queries) > 50 of
        true -> 
            {ok, State};
        false ->
            QueryField = element(1, hd(RawFields)),
            QueryTerm = hd(element(2, hd(RawFields))),
            NewQueries = queue:in({QueryField, QueryTerm}, State#state.queries),
            {ok, State#state { queries=NewQueries }}
    end;

run(search, _KeyGen, _ValueGen,
    S=#state{'query'=Query, expected=E, nodes=Nodes})
  when Query /= undefined ->
    %% Query will be a list of 1-3 args [Index, Query, Filter]
    %% e.g. ["index", "field:bar", "text:hello"]
    Node = choose(Nodes),
    Args = lists:map(fun list_to_binary/1, Query),
    {N, _} = rpc:call(Node, search, search, Args),
    case E of
        undefined -> {ok, S};
        _ ->
            if N == E -> {ok, S};
               true ->
                    {error, ?FMT("Expected ~p but received ~p", [E, N]), S}
            end
    end;

run(search, _KeyGen, _ValueGen, State) ->
    case queue:out(State#state.queries) of
        {{value, {QueryField, QueryTerm}}, NewQueries} ->
            %% Get the next queued query, then search...
            Node = choose(State#state.nodes),
            {_, _} = rpc:call(Node, search, search, [list_to_binary(QueryField ++ ":" ++ QueryTerm)]),
            {ok, State#state { queries=NewQueries }};
        {empty, NewQueries} ->
            %% No queries queued up, so just search for something...
            Node = choose(State#state.nodes),
            QueryField = choose(State#state.fields),
            QueryTerm = choose(State#state.terms),
            {_, _} = rpc:call(Node, search, search, [QueryField ++ ":" ++ QueryTerm]),
            {ok, State#state { queries=NewQueries }}
    end.

%% Given a file, split into newlines, and convert to an array.  Using
%% this because random access on an array is much faster than random
%% access on a list.
file_to_array(FilePath, Limit) ->
    Words = file_to_array(FilePath),
    case length(Words) > Limit of
        true  -> 
            {Words1, _} = lists:split(Limit, Words),
            Words1;
        false -> 
            Words
    end.

file_to_array(undefined) ->
    [];
file_to_array(FilePath) ->
    case file:read_file(FilePath) of
        {ok, Bytes} ->
            List = binary_to_list(Bytes),
            Words = string:tokens(List, "\r\n"),
            array:from_list(Words);
        {error, Reason} = Error ->
            lager:error("Could not read ~p: ~p", [filename:absname(FilePath),
                file:format_error(Reason)]),
            throw({file_to_array, FilePath, Error})
    end.

%% This function is called by the basho_bench setup process. It
%% returns a valgen function that takes an array of Fields and an
%% array of Terms. The valgen function is then called by this module
%% (the driver).
valgen(_ID, MaxFields, MaxTerms) ->
    fun(Fields, Terms) ->
        %% Get the field names...
        NumFields = random:uniform(MaxFields) + 1,
        FieldNames = lists:usort([choose(Fields) || _ <- lists:seq(1, NumFields)]),

        %% Create the fields...
        [{X, construct_field(MaxTerms, Terms)} || X <- FieldNames]
    end.

%% @private
construct_field(MaxTerms, Terms) ->
    %% Get the list of terms...
    NumTerms = random:uniform(MaxTerms) + 1,
    [choose(Terms) || _ <- lists:seq(1, NumTerms)].

%% Choose a random element from the List or Array.
choose(List) when is_list(List) ->
    N = random:uniform(length(List)),
    lists:nth(N, List);
choose(Array) when element(1, Array) == array ->
    N = random:uniform(Array:size()),
    Array:get(N - 1).
        
