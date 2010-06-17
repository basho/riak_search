-module(riak_search_backend).
-export([behaviour_info/1]).
-export([info_response/4, collect_info_response/3]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{start,2},
     {stop,1},
     {index,6}];
behaviour_info(_Other) ->
    undefined.

%% Send a response to an info() request
info_response(Sender, Term, Node, Count) ->
    %% TODO: Decide if this really needs to be a list of terms
    riak_core_vnode:reply(Sender, [{Term, Node, Count}]).

collect_info_response(RepliesRemaining, Ref, Acc) ->
    receive
        {Ref, List} when RepliesRemaining > 1 ->
            io:format("Received: Ref=~p List=~p\n", [Ref, List]),
            collect_info_response(RepliesRemaining - 1, Ref, List ++ Acc);
        {Ref, List} when RepliesRemaining == 1 ->
            io:format("Received: Ref=~p List=~p\n", [Ref, List]),
            {ok, List ++ Acc}
    after 5000 ->
        error_logger:error_msg("range_loop timed out!"),
        throw({timeout, range_loop})
    end.
