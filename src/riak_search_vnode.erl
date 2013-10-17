%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_vnode).
-export([index/2,
         delete/2,
         info/5,
         stream/6,
         range/8,
         repair/1,
         repair_status/1,
         repair_filter/1
        ]).
-export([start_vnode/1, init/1, handle_command/3,
         handle_handoff_command/3, handle_handoff_data/2,
         handoff_starting/2, handoff_cancelled/1, handoff_finished/2,
         is_empty/1, delete/1, terminate/2, encode_handoff_item/2,
         handle_exit/3]).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("riak_kv/include/riak_core_pb.hrl").
-include("riak_search.hrl").

-record(vstate, {idx, bmod, bstate}).
-record(index_v1, {iftvp_list}).
-record(delete_v1, {iftv_list}).
-record(info_v1, {index, field, term}).
-record(stream_v1, {index, field, term, filter_fun}).
-record(range_v1, {index, field, start_term, end_term, size, filter_fun}).

-define(HANDOFF_VER,1).

index(IndexNode, IFTVPList) ->
    Req = #index_v1{
      iftvp_list = IFTVPList
     },
    sync_command(IndexNode, Req).

delete(IndexNode, IFTVList) ->
    Req = #delete_v1{
      iftv_list = IFTVList
     },
    sync_command(IndexNode, Req).

info(Preflist, Index, Field, Term, ReplyTo) ->
    Req = #info_v1{
      index = Index,
      field = Field,
      term = Term
     },
    Ref = {info_response, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.

-spec stream(list(), index(), field(), term(), fun(), pid()) ->
                    {ok, stream_ref()}.
stream(Preflist, Index, Field, Term, FilterFun, ReplyTo) ->
    Req = #stream_v1{
      index = Index,
      field = Field,
      term = Term,
      filter_fun = FilterFun
     },
    Ref = {stream_response, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.

range(VNode, Index, Field, StartTerm, EndTerm, Size, FilterFun, ReplyTo) ->
    Req = #range_v1{
      index = Index,
      field = Field,
      start_term = StartTerm,
      end_term = EndTerm,
      size = Size,
      filter_fun = FilterFun
     },
    Ref = {stream_response, make_ref()},
    command([VNode], Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.

%%
%% Utility functions
%%

%% Issue the command to the riak vnode
command(PrefList, Req, Sender) ->
    riak_core_vnode_master:command(PrefList, Req, Sender,
                                   riak_search_vnode_master).

sync_command(IndexNode, Msg) ->
    riak_core_vnode_master:sync_command(IndexNode, Msg,
                                        riak_search_vnode_master, infinity).

%% @doc Repair the index at the given `Partition'.
-spec repair(partition()) ->
                    {ok, Pairs::[{partition(), node()}]} |
                    {down, Down::[{partition(), node()}]} |
                    ownership_change_in_progress.
repair(Partition) ->
    Service = riak_search,
    MP = {?MODULE, Partition},
    FilterModFun = {?MODULE, repair_filter},
    riak_core_vnode_manager:repair(Service, MP, FilterModFun).

%% @doc Given a `Target' partition generate a `Filter' fun to use
%%      during partition repair.
-spec repair_filter(partition()) -> Filter::function().
repair_filter(Target) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_repair:gen_filter(Target,
                                Ring,
                                schema_nval_map(),
                                default_object_nval(),
                                fun object_info/1).

%% @doc Get the status of the repair process for the given `Partition'.
-spec repair_status(partition()) -> no_repair | repair_in_progress.
repair_status(Partition) ->
    riak_core_vnode_manager:repair_status({riak_search_vnode, Partition}).

%%
%% Callbacks for riak_core_vnode
%%

start_vnode(Partition) ->
    riak_core_vnode_master:get_vnode_pid(Partition, riak_search_vnode).


init([VNodeIndex]) ->
    BMod = app_helper:get_env(riak_search, search_backend),
    Configuration = app_helper:get_env(riak_search),
    {ok, BState} = BMod:start(VNodeIndex, Configuration),
    State = #vstate{idx=VNodeIndex, bmod=BMod, bstate=BState},
    Pool = {pool, riak_search_worker, 2, []},

    {ok, State, [Pool]}.

handle_command(#index_v1{iftvp_list = IFTVPList},
               _Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:index(IFTVPList, BState), VState);

handle_command(#delete_v1{iftv_list = IFTVList},
               _Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:delete(IFTVList, BState), VState);

handle_command(#info_v1{index = Index,
                        field = Field,
                        term = Term},
               Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:info(Index, Field, Term, Sender, BState), VState);

handle_command(#stream_v1{index = Index,
                          field = Field,
                          term = Term,
                          filter_fun = FilterFun},
               Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:stream(Index, Field, Term, FilterFun, Sender, BState), VState);

handle_command(#range_v1{index = Index,
                         field = Field,
                         start_term = StartTerm,
                         end_term = EndTerm,
                         size = Size,
                         filter_fun = FilterFun},
               Sender, #vstate{bmod=BMod,bstate=BState}=VState) ->
    bmod_response(BMod:range(Index, Field, StartTerm, EndTerm, Size, FilterFun, Sender, BState), VState);

handle_command(#riak_core_fold_req_v1{} = ReqV1,
               Sender, State) ->
    %% Use make_fold_req() to upgrade to the most recent ?FOLD_REQ
    handle_command(riak_core_util:make_newest_fold_req(ReqV1), Sender, State);

%% Request from core_vnode_handoff_sender - fold function
%% expects to be called with {{Bucket,Key},Value}
handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc},
               Sender,
               #vstate{bmod=BMod,bstate=BState}=VState) ->
    %% TODO: Hardcoding async vs. sync logic for now. In future
    %% something like KV's backend capabilities should be put in
    %% place, or make everything async capable.
    case BMod of
        merge_index_backend ->
            {async, AsyncFoldFun} = BMod:fold(Fun, Acc, BState),
            FinishFun =
                fun(FinalAcc) ->
                        riak_core_vnode:reply(Sender, FinalAcc)
                end,
            {async, {fold, AsyncFoldFun, FinishFun}, Sender, VState};
        _ ->
            bmod_response(BMod:fold(Fun, Acc, BState), VState)
    end.

%% Handle a command during handoff - if it's a fold then
%% make sure it runs locally, otherwise forward it on to the
%% correct vnode.
handle_handoff_command(Req=?FOLD_REQ{}, Sender, VState) ->
    handle_command(Req, Sender, VState);
handle_handoff_command(Req=#riak_core_fold_req_v1{}, Sender, VState) ->
    handle_command(Req, Sender, VState);
handle_handoff_command(_Req, _Sender, VState) ->
    {forward, VState}.

handoff_starting(_TargetNode, VState) ->
    {true, VState}.

handoff_cancelled(VState) ->
    {ok, VState}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

encode_handoff_item({Index,{Field,Term}}, VPKList) ->
    BinObj = term_to_binary({Index,Field,Term,VPKList}),
    <<?HANDOFF_VER:8,BinObj/binary>>.

handle_handoff_data(<<?HANDOFF_VER:8,BinObj/binary>>,
                    #vstate{bmod=BMod,bstate=BState}=VState) ->
    {I,F,T,VPKList} = binary_to_term(BinObj),
    IFTVPKList = [{I,F,T,V,P,K} || {V,P,K} <- VPKList],
    {reply, {indexed, _}, NewBState} = BMod:index(IFTVPKList, BState),
    {reply, ok, VState#vstate { bstate=NewBState }}.

is_empty(VState=#vstate{bmod=BMod, bstate=BState}) ->
    {BMod:is_empty(BState), VState}.

delete(VState=#vstate{bmod=BMod, bstate=BState}) ->
    ok = BMod:drop(BState),
    {ok, VState}.

handle_exit(_, normal, State) ->
    {noreply, State};
handle_exit(_Pid, Reason, State) ->
    %% A linked process has crashed potentially causing pid values,
    %% such as merge index or worker pool, to become obsolete.
    {stop, Reason, State}.

terminate(_Reason, #vstate{bmod=BMod, bstate=BState}) ->
    BMod:stop(BState),
    ok.

bmod_response(noreply, VState) ->
    {noreply, VState};
bmod_response({reply, Reply}, VState) ->
    {reply, Reply, VState};
bmod_response({noreply, NewBState}, VState) ->
    {noreply, VState#vstate{bstate=NewBState}};
bmod_response({reply, Reply, NewBState}, VState) ->
    {reply, Reply, VState#vstate{bstate=NewBState}}.

%% @private
default_object_nval() ->
    riak_core_bucket:n_val(riak_core_bucket_props:defaults()).

%% @private
object_info({I, {F, T}}) ->
    Hash = riak_search_ring_utils:calc_partition(I, F, T),
    {I, Hash}.

%% @private
schema_nval_map() ->
    [{S:name(), S:n_val()} || S <- riak_search_config:get_all_schemas()].
