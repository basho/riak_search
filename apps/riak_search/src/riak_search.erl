-module(riak_search).
-export([
    put/5,
    stream/4,
    range/3
]).
-include("riak_search.hrl").

put(Index, Field, Term, Value, Props) ->
    %% Construct the operation...
    IndexBin = to_binary(Index),
    FieldTermBin = to_binary([Field, ".", Term]),
    Payload = {put, Value, Props},

    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    Client:put(Obj, 0, 0).

stream(Index, Field, Term, FilterFun) ->
    %% Construct the operation...
    IndexBin = to_binary(Index),
    FieldTermBin = to_binary([Field, ".", Term]),
    Ref = make_ref(),
    Payload = {stream, self(), Ref, FilterFun},
    
    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    Client:put(Obj, 0, 0),
    {ok, Ref}.

range(Start, End, Inclusive) ->
    %% Construct the operation...
    Bucket = <<"search_broadcast">>,
    Key = <<"ignored">>,
    Ref = make_ref(),
    Start1 = normalize_range(Start),
    End1 = normalize_range(End),
    Payload = {range, Start1, End1, Inclusive, self(), Ref},

    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(Bucket, Key, Payload),
    Client:put(Obj, 0, 0),
    range_loop(ringsize(), Ref, []).

range_loop(RepliesRemaining, Ref, Acc) ->    
    receive 
        {range_response, List, Ref} when RepliesRemaining > 1 ->
            range_loop(RepliesRemaining - 1, Ref, List ++ Acc);
        {range_response, List, Ref} when RepliesRemaining == 1 ->
            {ok, List ++ Acc};
        Other ->
            error_logger:info_msg("Unexpected response: ~p~n", [Other]),
            range_loop(RepliesRemaining, Ref, Acc)
    after 5000 ->
        error_logger:error_msg("range_loop timed out!"),
        throw({timeout, range_loop})
    end.
    
ringsize() ->
    app_helper:get_env(riak_core, ring_creation_size).


to_binary(L) when is_list(L) -> list_to_binary(L);
to_binary(B) when is_binary(B) -> B.


normalize_range({Index, Field, Term}) ->
    list_to_binary([Index, $., Field, $., Term]);
normalize_range(wildcard_all) -> wildcard_all;
normalize_range(wildcard_one) -> wildcard_one;
normalize_range(all) ->  all.
    
    
