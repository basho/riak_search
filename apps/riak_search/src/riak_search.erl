%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search).
-export([
    local_client/0,
    mapred_search/3
]).
-include("riak_search.hrl").

-define(TIMEOUT, 30000).

local_client() ->
    {ok, Client} = riak:local_client(),
    {ok, riak_search_client:new(Client)}.

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
            M1 = "Error running query '~s': ~p~n",
            error_logger:error_msg(M1, [Query, ParseError1]),
            throw({mapred_search, Query, ParseError1}),
            QueryOps = undefined % Make compiler happy.
    end,

    %% Parse the filter...
    case Client:parse_filter(DefaultIndex, Filter) of
        {ok, Ops2} ->
            FilterOps = Ops2;
        {error, ParseError2} ->
            M2 = "Error running query '~s': ~p~n",
            error_logger:error_msg(M2, [Filter, ParseError2]),
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

