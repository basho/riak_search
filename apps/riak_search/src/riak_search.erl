-module(riak_search).
-export([client_connect/1,
         local_client/0,
         stream/4,
         multi_stream/2,
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

multi_stream(IFTList, FilterFun) ->
    %% Split the IFTlist into a dictionary keyed by pref lists.
    %% TODO: Improve this to calculate the Vnode indexes from the IFT hash
    %%       value and use those for the dictionary insted.  Less preflist
    %%       lookups.
    N = riak_search_utils:n_val(),
    PlIFTList = lists:foldl(fun({term,{I,F,T},_TP}=IFT, PartIFT) ->
                                    P = riak_search_utils:calc_partition(I, F, T),
                                    Preflist = riak_core_apl:get_apl(P, N),
                                    orddict:append_list(Preflist, [IFT], PartIFT)
                            end, orddict:new(), IFTList),
    Ref = {multi_stream_response, make_ref()},
    Sender = {raw, Ref, self()},
    Sent = orddict:fold(fun(Preflist,IFTs,Acc) ->
                                SendTo = hd(Preflist),
                                riak_search_vnode:multi_stream(SendTo, IFTs, 
                                                               FilterFun, Sender),
                                [SendTo|Acc]
                        end, [], PlIFTList),
    {ok, Ref, length(Sent)}.
        
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
                  {ok, RecvRef, NumReqsSent} = do_catalog_query(Index, Query, self()),
                  filter_unique_terms(NumReqsSent, dict:new(), ReplyTo, RecvRef, FwdRef)
          end),
    {ok, FwdRef}.
    
do_catalog_query(_Index, Query, ReplyTo) ->
    %% Catalog queries are currently node-wide, so only need to send one
    %% per node.  Reduce the preference list to just be one per node.
    %% TODO: This will end up sending all traffic to the first vnode. Catalog
    %% queries should probably be changed to be per-partition.
    Preflist0 = riak_core_apl:active_owners(),
    Nodes = lists:usort([N || {_P,N} <- Preflist0]),
    Preflist = [begin {value,E} = lists:keysearch(N, 2, Preflist0), E end || 
                   N <- Nodes],
    {ok, RecvRef} = riak_search_vnode:catalog_query(Preflist, Query, ReplyTo),
    {ok, RecvRef, length(Preflist)}.
    
   
%% Receive the terms from the catalog process and forward on
%% the first time a term is received
filter_unique_terms(0, _Terms, ReplyTo, _RecvRef, FwdRef) ->
    ReplyTo ! {term, done, FwdRef};
filter_unique_terms(StreamsLeft, Terms, ReplyTo, RecvRef, FwdRef) ->
    receive
        {RecvRef, {_Partition, Index, Field, Term, _}} ->
            case dict:find(Term, Terms) of
                error ->
                    ReplyTo ! {term, Index, Field, Term, FwdRef},
                    filter_unique_terms(StreamsLeft, 
                                        dict:store(Term, true, Terms),
                                        ReplyTo, RecvRef, FwdRef);
                _ ->
                    filter_unique_terms(StreamsLeft, Terms, ReplyTo,
                                        RecvRef, FwdRef)
            end;
        {RecvRef, done} ->
            filter_unique_terms(StreamsLeft-1, Terms, ReplyTo, 
                                RecvRef, FwdRef)
    after 750 ->
            ReplyTo ! {term, done, FwdRef}
    end.
