-module(riak_search_vnode).
-export([index/6,
         delete_term/6,
         stream/6,
         info/5,
         info_range/7,
         catalog_query/3]).
-export([init/1, handle_command/3]).

-record(vstate, {idx, bmod, bstate}).
-record(index_v1, {index, field, term, value, props}).
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

delete_term(_Partition, _Nval, Index, Field, Term, DocId) ->
    io:format("info: Index=~p, Field=~p, Term=~p\n", [Index, Field, Term]),
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    Payload = {delete_entry, Index, Field, Term, DocId},
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    {ok, RiakClient} = riak:local_client(),
    RiakClient:put(Obj, 0).


stream(Preflist, Index, Field, Term, FilterFun, ReplyTo) ->
    io:format("stream Index=~p, Field=~p, Term=~p, FilterFun=~p, ReplyTo=~p\n", 
              [Index, Field, Term, FilterFun, ReplyTo]),
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
    io:format("info: Index=~p, Field=~p, Term=~p, ReplyTo=~p\n", [Index, Field, Term, ReplyTo]),
    Req = #info_v1{
      index = Index,
      field = Field,
      term = Term
     },
    Ref = {info_response, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.


info_range(Preflist, Index, Field, StartTerm, EndTerm, Size, ReplyTo) ->
    io:format("info_range: Index=~p, Field=~p, StartTerm=~p, EndTerm=~p, Size=~p, ReplyTo=~p\n",
              [Index, Field, StartTerm, EndTerm, Size, ReplyTo]),
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
    io:format("catalog_query: CatalogQuery: ~p, ReplyTo=~p\n",
              [CatalogQuery, ReplyTo]),
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
    bmod_response(BMod:catalog_query(CatalogQuery, Sender, BState), VState).


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

