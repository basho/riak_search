%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_kv_extractor).
-export([extract/2]).

-define(DEFAULT_FIELD, "value").

%% Initial implementation - get the whole object as the default field
extract(RiakObject, _Args) ->
    Data = riak_object:get_value(RiakObject),
    [{?DEFAULT_FIELD, Data}].
    


