-module(riak_search_backend).
-export([behaviour_info/1]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{start,2},
     {stop,1},
     {index,6}];
behaviour_info(_Other) ->
    undefined.
