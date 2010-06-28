-module(riak_search).
-export([client_connect/1,
         local_client/0,
         stream/4,
         info/3,
         info_range/5,
         term/2]).
-include("riak_search.hrl").

-define(TIMEOUT, 30000).

client_connect(Node) when is_atom(Node) ->
    {ok, Client} = riak:client_connect(Node),
    {ok, riak_search_client:new(Client)}.

local_client() ->
    {ok, Client} = riak:local_client(),
    {ok, riak_search_client:new(Client)}.

stream(Index, Field, Term, FilterFun) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    %% Calculate the preflist with full N but then only ask the first
    %% node in it.  Preflists are ordered with primaries first followed
    %% by fallbacks, so this will prefer a primary node over a fallback.
    [FirstEntry|_] = riak_core_apl:get_apl(Partition, N),
    Preflist = [FirstEntry],
    riak_search_vnode:stream(Preflist, Index, Field, Term, FilterFun, self()).

info(Index, Field, Term) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
    {ok, Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, []),
    %% TODO: Replace this with a middleman process that returns after 
    %% the first response.
    {ok, hd(Results)}.

info_range(Index, Field, StartTerm, EndTerm, Size) ->
    %% TODO: Duplicating current behavior for now - a PUT against a preflist with
    %%       the N val set to the size of the ring - this will mean no failbacks
    %%       will be available.  Instead should work out the preflist for
    %%       each partition index and find which node is responsible for that partition
    %%       and talk to that.
    Preflist = riak_core_apl:active_owners(),
    {ok, Ref} = riak_search_vnode:info_range(Preflist, Index, Field, StartTerm, EndTerm, 
                                             Size, self()),
    {ok, _Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, []).

%%
%% This code has issues at the moment - as discussed with Kevin.
%% It will probably return incomplete lists in a multi-node setup.
%% There will be multiple senders issuing streams.
%% Making do for now.
%% 
term(Index, Term) ->
    Query = lists:flatten(["term:", riak_search_utils:to_list(Term)]),
    ReplyTo = self(),
    FwdRef = make_ref(),
    spawn(fun() ->
                  {ok, RecvRef} = do_catalog_query(Index, Query, self()),
                  filter_unique_terms(dict:new(), ReplyTo, RecvRef, FwdRef)
          end),
    {ok, FwdRef}.
    
do_catalog_query(Index, Query, ReplyTo) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, <<"unused">>),
    Preflist = riak_core_apl:get_apl(Partition, N),
    riak_search_vnode:catalog_query(Preflist, Query, ReplyTo).
    
   
%% Receive the terms from the catalog process and forward on
%% the first time 
filter_unique_terms(Terms, ReplyTo, RecvRef, FwdRef) ->
    receive
        {RecvRef, {_Partition, Index, Field, Term, _}} ->
            case dict:find(Term, Terms) of
                error ->
                    ReplyTo ! {term, Index, Field, Term, FwdRef},
                    filter_unique_terms(dict:store(Term, true, Terms),
                                        ReplyTo, RecvRef, FwdRef);
                _ ->
                    filter_unique_terms(Terms, ReplyTo, RecvRef, FwdRef)
            end;
        {RecvRef, done} ->
            ReplyTo ! {term, done, FwdRef}
    after 750 ->
            ReplyTo ! {term, done, FwdRef}
    end.
