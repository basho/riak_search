-module(riak_search_vnode).
-export([index/6,
         delete_term/6,
         stream/7,
         info/6,
         info_range/6]).
-export([init/1, handle_command/3]).

-record(vstate, {idx, bmod, bstate}).
-record(index_v1, {index, field, term, value, props}).

-export([other_index/6]).
other_index(PrefList, Index, Field, Term, Value, Props) ->
    Req = #index_v1{
      index = Index,
      field = Field,
      term = Term,
      value = Value,
      props = Props
     },
    command(PrefList, Req).

index(_Preflist, Index, Field, Term, Value, Props) ->
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    Payload = {index, Index, Field, Term, Value, Props},
    %% Run the operation...
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    {ok, RiakClient} = riak:local_client(),
    RiakClient:put(Obj, 0).


delete_term(_Partition, _Nval, Index, Field, Term, DocId) ->
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    Payload = {delete_entry, Index, Field, Term, DocId},
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    {ok, RiakClient} = riak:local_client(),
    RiakClient:put(Obj, 0).


stream(_Partition, _Nval, Index, Field, Term, FilterFun, ReplyTo) ->
    %% Construct the operation...
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    {ok, RiakClient} = riak:local_client(),
    Ref = make_ref(),

    %% How many replicas?
    BucketProps = riak_core_bucket:get_bucket(IndexBin),
    NVal = proplists:get_value(n_val, BucketProps),

    %% Figure out which nodes we can stream from.
    Payload1 = {init_stream, self(), Ref},
    Obj1 = riak_object:new(IndexBin, FieldTermBin, Payload1),
    RiakClient:put(Obj1, 0, 0),
    {ok, Partition, Node} = wait_for_ready(NVal, Ref, undefined, undefined),

    %% Run the operation...
    Payload2 = {stream, Index, Field, Term, ReplyTo, Ref, Partition, Node, FilterFun},
    Obj2 = riak_object:new(IndexBin, FieldTermBin, Payload2),
    RiakClient:put(Obj2, 0, 0),
    {ok, Ref}.

info(_Partition, _Nval, Index, Field, Term, ReplyTo) ->
    %% Construct the operation...
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    Ref = make_ref(),
    Payload = {info, Index, Field, Term, ReplyTo, Ref},

    %% Run the operation...
    {ok, RiakClient} = riak:local_client(),
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    RiakClient:put(Obj, 0, 0),
    {ok, Ref}.

info_range(Index, Field, StartTerm, EndTerm, Size, ReplyTo) ->
    %% Construct the operation...
    Bucket = <<"search_broadcast">>,
    Key = <<"ignored">>,
    Ref = make_ref(),
    Payload = {info_range, Index, Field, StartTerm, EndTerm, Size, ReplyTo, Ref},

    %% Run the operation...
    {ok, RiakClient} = riak:local_client(),
    Obj = riak_object:new(Bucket, Key, Payload),
    RiakClient:put(Obj, 0, 0),
    {ok, Ref}.

%% Get replies from all nodes that are willing to stream this
%% bucket. If there is one on the local node, then use it, otherwise,
%% use the first one that responds.
wait_for_ready(0, _Ref, Partition, Node) ->
    {ok, Partition, Node};
wait_for_ready(RepliesRemaining, Ref, Partition, Node) ->
    LocalNode = node(),
    receive
        {stream_ready, LocalPartition, LocalNode, Ref} ->
            {ok, LocalPartition, LocalNode};
        {stream_ready, _NewPartition, _NewNode, Ref} when Node /= undefined ->
            wait_for_ready(RepliesRemaining - 1, Ref, Partition, Node);
        {stream_ready, NewPartition, NewNode, Ref} ->
            wait_for_ready(RepliesRemaining -1, Ref, NewPartition, NewNode)
    end.


%%
%% Utility functions
%%

%% Issue the command to the riak vnode
command(PrefList, Req) ->
    riak_core_vnode_master:command(PrefList, Req, riak_search_vnode_master).

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
    bmod_response(BMod:index(Index, Field, Term, Value, Props, BState), VState).

bmod_response(ok, VState) ->
    {noreply, VState};
bmod_response({ok, NewBState}, VState) ->
    {noreply, VState#vstate{bstate=NewBState}}.


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

