%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search).
-export([
    analyze/3,
    local_client/0,
    mapred_search/3
]).
-include("riak_search.hrl").

-define(TIMEOUT, 30000).

local_client() ->
    {ok, Client} = riak:local_client(),
    {ok, riak_search_client:new(Client)}.

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
mapred_search(FlowOrPipe, [DefaultIndex, Query], Timeout) ->
    mapred_search(FlowOrPipe, [DefaultIndex, Query, ""], Timeout);

mapred_search(FlowPid, [DefaultIndex, Query, Filter], Timeout)
  when is_pid(FlowPid) ->
    {ok, C} = riak_search:local_client(),
    QueryOps = parse_query(C, DefaultIndex, Query),
    FilterOps = parse_filter(C, DefaultIndex, Filter),

    %% Perform a search, funnel results to the mapred job...
    F = fun(Results, Acc) ->
        %% Make the list of BKeys...
        BKeys = [{{Index, DocID}, {struct, Props}}
                 || {Index, DocID, Props} <- Results],
        luke_flow:add_inputs(FlowPid, BKeys),
        Acc
    end,
    ok = C:search_fold(DefaultIndex, QueryOps, FilterOps, F, ok, Timeout),
    luke_flow:finish_inputs(FlowPid);

mapred_search(Pipe, [DefaultIndex, Query, Filter], Timeout) ->
    {ok, C} = riak_search:local_client(),
    QueryOps = parse_query(C, DefaultIndex, Query),
    FilterOps = parse_filter(C, DefaultIndex, Filter),

    %% Perform a search, funnel results to the mapred job...
    Q = queue_work(Pipe),
    F = fun(Results, Acc) ->
                lists:foreach(Q, Results),
                Acc
    end,
    ok = C:search_fold(DefaultIndex, QueryOps, FilterOps, F, ok, Timeout),
    riak_pipe:eoi(Pipe).

parse_query(C, DefaultIndex, Query) ->
    case C:parse_query(DefaultIndex, Query) of
        {ok, Ops1} -> Ops1;
        {error, ParseError1} ->
            lager:error("Error parsing query '~s': ~p", [Query, ParseError1]),
            throw({mapred_search, Query, ParseError1})
    end.

parse_filter(C, DefaultIndex, Filter) ->
    case C:parse_filter(DefaultIndex, Filter) of
        {ok, Ops2} -> Ops2;
        {error, ParseError2} ->
            lager:error("Error parsing filter '~s': ~p", [Filter, ParseError2]),
            throw({mapred_search, Filter, ParseError2})
    end.

queue_work(Pipe) ->
    fun({Index, DocId, Props}) ->
            riak_pipe:queue_work(Pipe, {{Index, DocId}, {struct, Props}})
    end.
