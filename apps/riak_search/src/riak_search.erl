-module(riak_search).
-export([client_connect/1,
         local_client/0,
         stream/4,
         multi_stream/3,
         info/3,
         term_preflist/3,
         info_range/5,
         info_range_no_count/4,
         catalog_query/1,
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

multi_stream(OpNode, IFTList, FilterFun) ->
    %% TODO: Establish the correct preference list for this operation
    %% If the I/F/T is chosen at random and there are multiple nodes
    %% in the system it is likely that not all terms will be located.
    %% For now, emulate something similar to what the k/v encapsulated
    %% version did.
    %% UPDATE: By the time we get to this point with a multi_stream, it's
    %% guaranteed that all the terms in the multi_stream call reside on
    %% the same target node (passed in via OpNode); we'll establish a
    %% viable partition by getting the preflist for the first term and
    %% finding the first partition that maps to the specified OpNode
    %% and using that.  The vnode itself does not discriminate by Partition
    %% when processing an multi_stream request, so any partition will work
    %% as long as it maps to that node.
    {term, {Index, Field, Term}, _TermProps} = hd(IFTList),
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    P1 = hd(lists:filter(fun({_Partition, Node}) ->
        Node == OpNode end, Preflist)),
    riak_search_vnode:multi_stream(P1, IFTList, FilterFun, self()).

info(Index, Field, Term) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
    {ok, Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, []),
    %% TODO: Replace this with a middleman process that returns after 
    %% the first response.
    {ok, hd(Results)}.

term_preflist(Index, Field, Term) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    riak_core_apl:get_apl(Partition, N).

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

info_range_no_count(Index, Field, StartTerm, EndTerm) ->
    %% Results are of form {"term", 'node@1.1.1.1', Count}
    CatalogQuery = lists:flatten(["index:", Index, " AND field:", Field, " AND term:[",
        StartTerm, " TO ", EndTerm, "]"]),
    {ok, CatalogResults} = catalog_query(CatalogQuery),
    Results = lists:map(fun(CatalogEntry) ->
        %% TODO: use this partition information (as opposed to computing the
        %%       preflist for every term again later...
        {_Partition, Index, Field, Term, Props} = CatalogEntry,
        Node = proplists:get_value(node, Props),
        %%{Node, lists:flatten([Index, ".", Field, ".", Term]), 1}
        {Term, Node, 1}
    end, CatalogResults),
    {ok, Results}.

catalog_query(Query) ->
    ReplyTo = self(),
    FwdRef = make_ref(),
    spawn(fun() ->
                  {ok, RecvRef, NumReqsSent} = do_catalog_query(Query, self()),
                  Results = receive_catalog_query_results(NumReqsSent, ReplyTo, RecvRef, FwdRef, []),
                  ReplyTo ! {catalog_query_results, FwdRef, Results}
          end),
    receive
        {catalog_query_results, FwdRef, Results} ->
            {ok, Results}
        after ?TIMEOUT ->
            {error, timeout}
    end.

receive_catalog_query_results(0, _ReplyTo, _RecvRef, _FwdRef, Acc) ->
    Acc;
receive_catalog_query_results(StreamsLeft, ReplyTo, RecvRef, FwdRef, Acc) ->
    receive
        {RecvRef, {Partition, Index, Field, Term, Props}} ->
            receive_catalog_query_results(StreamsLeft, ReplyTo, RecvRef, FwdRef,
                Acc ++ [{Partition, Index, Field, Term, Props}]);
        {RecvRef, done} ->
            receive_catalog_query_results(StreamsLeft-1, ReplyTo, RecvRef, FwdRef,
                Acc);
        Msg ->
            receive_catalog_query_results(StreamsLeft, ReplyTo, RecvRef, FwdRef,
                Acc ++ [{error, Msg}])
    end.

%%
%% This code has issues at the moment - as discussed with Kevin.
%% It will probably return incomplete lists in a multi-node setup.
%% There will be multiple senders issuing streams.
%% Making do for now.
%% 
term(Index, Term) ->
    %% TODO: this query also needs to specify index: and partition_id: !!
    Query = lists:flatten(["term:", riak_search_utils:to_list(Term)]),
    ReplyTo = self(),
    FwdRef = make_ref(),
    spawn(fun() ->
                  {ok, RecvRef, NumReqsSent} = do_catalog_query(Index, Query, self()),
                  filter_unique_terms(NumReqsSent, dict:new(), ReplyTo, RecvRef, FwdRef)
          end),
    {ok, FwdRef}.

do_catalog_query(Query, ReplyTo) ->
    do_catalog_query("", Query, ReplyTo).
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
    %% TODO: should this be 750 ms?  why is this here?
    after 750 ->
            ReplyTo ! {term, done, FwdRef}
    end.
