%% @doc Support for 2i interface atop Riak Search.
-module(riak_search_2i).
-export([does_contain_idx/1]).
-define(MD_INDEX, <<"index">>).

%% @doc Predicate to determine if the given metadata `MD' contains 2i
%%      indexes.
-spec does_contain_idx(dict()) -> boolean().
does_contain_idx(MD) ->
    dict:is_key(?MD_INDEX, MD).
