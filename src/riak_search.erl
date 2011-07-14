%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search).
-export([
    analyze/3,
    local_client/0,
    local_solr_client/0,
    mapred_search/3
]).
-include("riak_search.hrl").

-define(TIMEOUT, 30000).

local_client() ->
    {ok, Client} = riak:local_client(),
    {ok, riak_search_client:new(Client)}.

local_solr_client() ->
    {ok, RiakClient} = riak:local_client(),
    {ok, SearchClient} = riak_search:local_client(),
    {ok, riak_solr_search_client:new(RiakClient, SearchClient)}.

%% No analyzer is defined. Throw an exception.
analyze(_Text, undefined, _AnalyzerArgs) ->
    lager:error("The analyzer_factory setting is not set. Check your schema."),
    throw({schema_setting_required, analyzer_factory});

%% Handle Erlang-based AnalyzerFactory. Can either be of the form
%% {erlang, Mod, Fun} or {erlang, Mod, Fun, Args}. Function should
%% return {ok, Terms} where Terms is a list of Erlang binaries.
analyze(Text, {erlang, Mod, Fun, AnalyzerArgs}, _) ->
    Mod:Fun(Text, AnalyzerArgs);
analyze(Text, {erlang, Mod, Fun}, AnalyzerArgs) ->
    Mod:Fun(Text, AnalyzerArgs).

%% Used in riak_kv Map/Reduce integration.
mapred_search(FlowPid, [DefaultIndex, Query], Timeout) ->
    mapred_search(FlowPid, [DefaultIndex, Query, ""], Timeout);
    
mapred_search(FlowPid, [DefaultIndex, Query, Filter], Timeout) ->
    {ok, Client} = riak_search:local_client(),

    %% Parse the query...
    case Client:parse_query(DefaultIndex, Query) of
        {ok, Ops1} ->
            QueryOps = Ops1;
        {error, ParseError1} ->
            lager:error("Error running query '~s': ~p", [Query, ParseError1]),
            throw({mapred_search, Query, ParseError1}),
            QueryOps = undefined % Make compiler happy.
    end,

    %% Parse the filter...
    case Client:parse_filter(DefaultIndex, Filter) of
        {ok, Ops2} ->
            FilterOps = Ops2;
        {error, ParseError2} ->
            lager:error("Error running query '~s': ~p", [Filter, ParseError2]),
            throw({mapred_search, Filter, ParseError2}),
            FilterOps = undefined % Make compiler happy.
    end,

    %% Perform a search, funnel results to the mapred job...
    F = fun(Results, Acc) ->
        %% Make the list of BKeys...
        BKeys = [{{Index, DocID}, {struct, Props}}
                 || {Index, DocID, Props} <- Results],
        luke_flow:add_inputs(FlowPid, BKeys),
        Acc
    end,
    ok = Client:search_fold(DefaultIndex, QueryOps, FilterOps, F, ok, Timeout),
    luke_flow:finish_inputs(FlowPid).

