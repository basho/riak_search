%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_kv_raw_extractor).
-export([extract/2,
         extract_value/2]).

-include("riak_search.hrl").

extract(RiakObject, _Args) ->
    Values = riak_object:get_values(RiakObject),
    lists:flatten([extract_value(V, _Args) || V <- Values]).

extract_value(Data, _Args) ->
    [{?DEFAULT_FIELD, Data}].

