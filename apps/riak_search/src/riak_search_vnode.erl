-module(riak_search_vnode).
-export([index/6,
         delete_term/5,
         stream/6,
         info/5,
         info_range/7,
         catalog_query/3]).
-export([start_vnode/1, init/1, handle_command/3, handle_handoff_command/3,
         handoff_starting/2, is_empty/1, delete_and_exit/1,
         handoff_cancelled/1, handle_handoff_data/3]).
-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(vstate, {idx, bmod, bstate}).
-record(index_v1, {index, field, term, value, props}).
-record(delete_v1, {index, field, term, doc_id}).
-record(info_v1, {index, field, term}).
-record(info_range_v1, {index, field, start_term, end_term, size}).
-record(stream_v1, {index, field, term, filter_fun}).
-record(catalog_query_v1, {index, catalog_query}).

index(Preflist, Index, Field, Term, Value, Props) ->
    Req = #index_v1{
      index = Index,
      field = Field,
      term = Term,
      value = Value,
      props = Props
     },
    command(Preflist, Req).

delete_term(Preflist, Index, Field, Term, DocId) ->
    Req = #delete_v1{
      index = Index,
      field = Field,
      term = Term,
      doc_id = DocId
     },
    command(Preflist, Req).

stream(Preflist, Index, Field, Term, FilterFun, ReplyTo) ->
    %% io:format("stream Index=~p, Field=~p, Term=~p, FilterFun=~p, ReplyTo=~p\n", 
    %%           [Index, Field, Term, FilterFun, ReplyTo]),
    Req = #stream_v1{
      index = Index,
      field = Field,
      term = Term,
      filter_fun = FilterFun
     },
    Ref = {info_response, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.

info(Preflist, Index, Field, Term, ReplyTo) ->
    %% io:format("info: Index=~p, Field=~p, Term=~p, ReplyTo=~p\n", [Index, Field, Term, ReplyTo]),
    Req = #info_v1{
      index = Index,
      field = Field,
      term = Term
     },
    Ref = {info_response, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.


info_range(Preflist, Index, Field, StartTerm, EndTerm, Size, ReplyTo) ->
    %% io:format("info_range: Index=~p, Field=~p, StartTerm=~p, EndTerm=~p, Size=~p, ReplyTo=~p\n",
    %%           [Index, Field, StartTerm, EndTerm, Size, ReplyTo]),
    Req = #info_range_v1{
      index = Index,
      field = Field,
      start_term = StartTerm,
      end_term = EndTerm,
      size = Size
     },
    Ref = {info_response, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.

catalog_query(Preflist, CatalogQuery, ReplyTo) ->
    %% io:format("catalog_query: CatalogQuery: ~p, ReplyTo=~p\n",
    %%           [CatalogQuery, ReplyTo]),
    Req = #catalog_query_v1{
      catalog_query = CatalogQuery
     },
    Ref = {catalog_query, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.

%% %% Get replies from all nodes that are willing to stream this
%% %% bucket. If there is one on the local node, then use it, otherwise,
%% %% use the first one that responds.
%% wait_for_ready(0, _Ref, Partition, Node) ->
%%     {ok, Partition, Node};
%% wait_for_ready(RepliesRemaining, Ref, Partition, Node) ->
%%     LocalNode = node(),
%%     receive
%%         {stream_ready, LocalPartition, LocalNode, Ref} ->
%%             {ok, LocalPartition, LocalNode};
%%         {stream_ready, _NewPartition, _NewNode, Ref} when Node /= undefined ->
%%             wait_for_ready(RepliesRemaining - 1, Ref, Partition, Node);
%%         {stream_ready, NewPartition, NewNode, Ref} ->
%%             wait_for_ready(RepliesRemaining -1, Ref, NewPartition, NewNode)
%%     end.


%%
%% Utility functions
%%

%% Issue the command to the riak vnode
command(PrefList, Req) ->
    riak_core_vnode_master:command(PrefList, Req, riak_search_vnode_master).

%% Issue the command to the riak vnode
command(PrefList, Req, Sender) ->
    riak_core_vnode_master:command(PrefList, Req, Sender,
                                   riak_search_vnode_master).

%%
%% Callbacks for riak_core_vnode
%%

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, riak_search_vnode).

init([VNodeIndex]) ->
    BMod = app_helper:get_env(riak_search, search_backend),
    Configuration = app_helper:get_env(riak_search),
    {ok, BState} = BMod:start(VNodeIndex, Configuration),
    {ok, #vstate{idx=VNodeIndex,
                 bmod=BMod,
                 bstate=BState}}.

handle_command(#index_v1{index = Index,
                         field = Field,
                         term = Term,
                         value = Value,
                         props = Props},
               _Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:index(Index, Field, Term, Value, Props, BState), VState);
handle_command(#delete_v1{index = Index,
                          field = Field,
                          term = Term,
                          doc_id = DocId},
               _Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:delete_entry(Index, Field, Term, DocId, BState), VState);
handle_command(#info_v1{index = Index,
                        field = Field,
                        term = Term},
               Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:info(Index, Field, Term, Sender, BState), VState);
handle_command(#info_range_v1{index = Index,
                              field = Field,
                              start_term = StartTerm,
                              end_term = EndTerm,
                              size = Size},
               Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:info_range(Index, Field, StartTerm, EndTerm,
                                  Size, Sender, BState), VState);
handle_command(#stream_v1{index = Index,
                          field = Field,
                          term = Term,
                          filter_fun = FilterFun},
               Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:stream(Index, Field, Term, FilterFun, Sender, BState), VState);

handle_command(#catalog_query_v1{catalog_query = CatalogQuery},
               Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:catalog_query(CatalogQuery, Sender, BState), VState);

%% Request from core_vnode_handoff_sender - fold function
%% expects to be called with {{Bucket,Key},Value}
handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc},_Sender,
               #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:fold(Fun, Acc, BState), VState).

%% Handle a command during handoff - if it's a fold then
%% make sure it runs locally, otherwise forward it on to the
%% correct vnode.
handle_handoff_command(Req=?FOLD_REQ{}, Sender, VState) -> 
    handle_command(Req, Sender, VState);
handle_handoff_command(_Req, _Sender, VState) -> 
    {forward, VState}.

handoff_starting(_TargetNode, VState) ->
    {true, VState}.

handoff_cancelled(VState) ->
    {ok, VState}.

handle_handoff_data({Index,_FieldTerm}, Obj, #vstate{bmod=BMod,bstate=BState}=VState) ->
    %% The previous k/v backend wrapper for Raptor always returned
    %% {error, not_found} on a get request, so it would always
    %% overwrite with handoff data.  Keep this behavior until 
    %% we get a chance to fix.
    {Field, Term, Value, Props} = binary_to_term(Obj),
    noreply = BMod:index(Index, Field, Term, Value, Props, BState),
    {reply, ok, VState}.

is_empty(VState=#vstate{bmod=BMod, bstate=BState}) ->
    {BMod:is_empty(BState), VState}.

delete_and_exit(VState=#vstate{bmod=BMod, bstate=BState}) ->
    ok = BMod:drop(BState),
    {stop, normal, VState}.

bmod_response(noreply, VState) ->
    {noreply, VState};
bmod_response({reply, Reply}, VState) ->
    {reply, Reply, VState};
bmod_response({noreply, NewBState}, VState) ->
    {noreply, VState#vstate{bstate=NewBState}};
bmod_response({reply, Reply, NewBState}, VState) ->
    {reply, Reply, VState#vstate{bstate=NewBState}}.


%% handle_command({delete_entry, Index, Field, Term, DocId}, Sender,
%%                VState=#state{bmod=BMod,bstate=BState}) ->
%%     bmod_response(BMod:delete_entry(Index, Field, Term, Term, DocId, Bstate),VState);
%% handle_command({stream, Index, Field, Term, Partition, FilterFun}, Sender,
%%                VState=#state{bmod=BMod,bstate=BState}) ->
%%     bmod_response(BMod:stream(Index, Field, Term, Partition, Sender, Bstate),VState);
%% handle_command({info, Index, Field, Term}, Sender, 
%%                VState=#state{bmod=BMod,bstate=BState}) ->
%%     bmod_response(BMod:stream(Index, Field, Term, Partition, Sender, Bstate), VState);
%% handle_command({info_range, Index, Field, StartTerm, EndTerm, Size}, Sender, 
%%                VState=#state{bmod=BMod,bstate=BState}) ->
%%     bmod_response(BMod:info_range(Index, Field, StartTerm, EndTerm, Size, Sender, Bstate),VState).

