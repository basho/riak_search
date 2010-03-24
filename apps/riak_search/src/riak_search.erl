-module(riak_search).
-export([
    broadcast/1,
    put/5,
    stream/4
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

broadcast(Msg) ->
    {ok, C} = riak:local_client(),
    Ref = make_ref(),
    Obj = riak_object:new(<<"search_broadcast">>, <<"ignored">>, {broadcast, Msg, Ref}),
    C:put(Obj, 0, 0),
    broadcast_recv(ringsize(), Ref, []).

broadcast_recv(RepliesRemaining, Ref, Acc) ->    
    receive 
        {response, Reply, Ref} when RepliesRemaining > 1 ->
            broadcast_recv(RepliesRemaining - 1, Ref, [Reply|Acc]);
        {response, Reply, Ref} when RepliesRemaining == 1 ->
            {ok, [Reply|Acc]};
        Other ->
            error_logger:info_msg("Unexpected response: ~p~n", [Other]),
            broadcast_recv(RepliesRemaining, Ref, Acc)
    after 5000 ->
        error_logger:error_msg("broadcast_recv timed out!"),
        throw({timeout, broadcast_recv})
    end.
    
ringsize() ->
    app_helper:get_env(riak_core, ring_creation_size).

to_binary(L) when is_list(L) -> list_to_binary(L);
to_binary(B) when is_binary(B) -> B.
