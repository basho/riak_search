%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_kv_raw_extractor).
-export([extract/3,
         extract_value/3]).

-include("riak_search.hrl").

extract(RiakObject, DefaultField, _Args) ->
    Values = riak_object:get_values(RiakObject),
    lists:flatten([extract_value(V, DefaultField, _Args) || V <- Values]).

extract_value(Data, DefaultField, _Args) ->
    [{DefaultField, Data}].

