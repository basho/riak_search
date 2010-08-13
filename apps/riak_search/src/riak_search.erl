%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search).
-export([
    client_connect/1,
    local_client/0,
    mapred_search/3
]).
-include("riak_search.hrl").

-define(TIMEOUT, 30000).

client_connect(Node) when is_atom(Node) ->
    {ok, Client} = riak:client_connect(Node),
    {ok, riak_search_client:new(Client)}.

local_client() ->
    {ok, Client} = riak:local_client(),
    {ok, riak_search_client:new(Client)}.

%% Used in riak_kv Map/Reduce integration.
mapred_search(FlowPid, Options, Timeout) ->
    %% Get the Index and Query from properties...
    [DefaultIndex, Query] = Options,

    %% TODO - This is temporary until we fix data types.
    DefaultIndexB = riak_search_utils:to_binary(DefaultIndex),
    QueryL = riak_search_utils:to_list(Query),
    
    
    %% Parse the query...
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(DefaultIndexB, QueryL) of
        {ok, Ops} ->
            QueryOps = Ops;
        {error, ParseError} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [Query, ParseError]),
            throw({mapred_search, Query, ParseError}),
            QueryOps = undefined % Make compiler happy.
    end,

    %% Perform a search, funnel results to the mapred job...
    F = fun(Results, Acc) ->
        %% Make the list of BKeys...
        BKeys = [{Index, DocID} || {Index, DocID, _Props} <- Results],
        luke_flow:add_inputs(FlowPid, BKeys),
        Acc
    end,
    ok = Client:search_fold(DefaultIndex, QueryOps, F, ok, Timeout),
    luke_flow:finish_inputs(FlowPid).

