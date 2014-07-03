%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_backend).
-export([
         behaviour_info/1
        ]).
-export([
         response_results/2,
         response_done/1,
         response_error/2,
         info_response/2,
         collect_info_response/3,
         collect_info_response/4]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{start,2},
     {stop,1},
     {index, 2},
     {delete, 2},
     {stream,6},
     {range, 8},
     {info,5},
     {fold,3},
     {is_empty,1},
     {drop,1}];
behaviour_info(_Other) ->
    undefined.

%% Send a response to an info() request
info_response(Sender, Result) ->
    %% TODO: Decide if this really needs to be a list of terms
    riak_core_vnode:reply(Sender, Result).

%% Send a response to a stream() request
response_results(Sender, Results) ->
    riak_core_vnode:reply(Sender, {result_vec, Results}).

response_done(Sender) ->
    riak_core_vnode:reply(Sender, done).

response_error(Sender, Reason) ->
    riak_core_vnode:reply(Sender, {error, Reason}).

collect_info_response(RepliesRemaining, Ref, Acc) ->
    collect_info_response(RepliesRemaining, Ref, Acc, 5000).

collect_info_response(0, _Ref, Acc, _Timeout) ->
    {ok, Acc};
collect_info_response(RepliesRemaining, Ref, Acc, Timeout) ->
    receive
        {Ref, List} ->
            collect_info_response(RepliesRemaining - 1, Ref, List ++ Acc, Timeout)
    after Timeout ->
        lager:error("range_loop timed out!"),
        throw({timeout, range_loop})
    end.
