-module(riak_search_vnode).
-export([index/7,
         delete_term/6,
         stream/7,
         info/6,
         info_range/6]).
-export([init/1, handle_command/3]).


index(_Partition, _N, Index, Field, Term, Value, Props) ->
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
%% Callbacks for riak_core_vnode
%%
%% -record(vstate, {bmod,bstate}).  %% Backend module
-record(state, {idx}).

init([Index]) ->
    {ok, #state{idx=Index}}.

handle_command(_Req, _Sender, State=#state{idx=_Idx}) ->
    {reply, {error, not_implemented}, State}.




%% %% TODO: Consider renaming 'start'
%% init([Partition]) ->
%%     %% Discover backend module somehow
%%     Bmod = riak_search_raptor_backend,
%%     %% Discover backend arguments somehow
%%     Bargs = [],
%%     Bstate = Bmod:start(Partition, Bargs),
%%     {ok, #state{bmod=Bmod,bstate=Bstate}}.

%% handle_command({index, Index, Field, Term, Value, Props}, Sender, 
%%                VState=#state{bmod=BMod,bstate=BState}) ->
%%     bmod_response(BMod:index(Index, Field, Term, Value, Props, BState));
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


%% bmod_response(ok, VState) ->
%%     {noreply, VState};
%% bmod_response({ok, NewBState}, VState) ->
%%     {noreply, VState#vstate{bstate=NewBState}}.

%% %%% Or the super-meta version


%% -record(index, {index, field, term, value, props}).
%% -record(delete_entry, {index, field, term, docid}).
%% -record(stream, {index, field, term, partition, filter_fun}).
%% -record(info, {index, field, term}).
%% -record(info_range, index, field, start_term, end_term, size}).

%% handle_command(#index{}=Cmd, _Sender, VState) ->
%%     noreply_bmod(Cmd, VState);
%% handle_command(#delete{}=Cmd, _Sender, VState) ->
%%     noreply_bmod(Cmd, VState);
%% handle_command(#stream{}=Cmd, Sender, VState) ->
%%     reply_bmod(Cmd, Sender, VState);
%% handle_command(#info{}=Cmd, Sender, VState) ->
%%     reply_bmod(Cmd, Sender, VState);
%% handle_command(#info_range{}=Cmd, Sender, VState) ->
%%     reply_bmod(Cmd, Sender, VState);

%% noreply_bmod(Cmd, Sender, VState#vstate{bmod=BMod,bstate=BState}) ->
%%     [Fun | Args] = tuple_to_list(Cmd) ++ [BState],
%%     case apply(Bmod, Fun, Args) of
%%         ok ->
%%             {noreply, VState};
%%         {ok, NewBState} ->
%%             {noreply, VState#vstate{bstate=NewBState}}
%%     end.
%% reply_bmod(Cmd, Sender, VState#vstate{bmod=BMod,bstate=BState}) ->
%%     [Fun | Args] = tuple_to_list(Cmd) ++ [Sender, BState],
%%     case apply(Bmod, Fun, Args) of
%%         ok ->
%%             {noreply, VState};
%%         {ok, NewBState} ->
%%             {noreply, VState#vstate{bstate=NewBState}}
%%     end.

