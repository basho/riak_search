%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_vnode).
-export([index/2,
         delete/2,
         info/5,
         stream/6]).
-export([start_vnode/1, init/1, handle_command/3,
         handle_handoff_command/3, handle_handoff_data/2,
         handoff_starting/2, handoff_cancelled/1, handoff_finished/2,
         is_empty/1, delete/1, terminate/2, encode_handoff_item/2]).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("riak_core/include/riak_core_pb.hrl").
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).


-record(vstate, {idx, bmod, bstate}).
-record(index_v1, {iftvp_list}).
-record(delete_v1, {iftv_list}).
-record(info_v1, {index, field, term}).
-record(stream_v1, {index, field, term, filter_fun}).

-define(HANDOFF_VER,1).

index(Preflist, IFTVPList) ->
    Req = #index_v1{
      iftvp_list = IFTVPList
     },
    sync_spawn_command(Preflist, Req).    

delete(Preflist, IFTVList) ->
    Req = #delete_v1{
      iftv_list = IFTVList
     },
    sync_spawn_command(Preflist, Req).    

info(Preflist, Index, Field, Term, ReplyTo) ->
    Req = #info_v1{
      index = Index,
      field = Field,
      term = Term
     },
    Ref = {info_response, make_ref()},
    command(Preflist, Req, {raw, Ref, ReplyTo}),
    {ok, Ref}.

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


%%
%% Utility functions
%%

%% Issue the command to the riak vnode
command(PrefList, Req, Sender) ->
    riak_core_vnode_master:command(PrefList, Req, Sender,
                                   riak_search_vnode_master).

sync_spawn_command(Preflist, Msg) ->
    F = fun({Index, Node}) ->
                riak_core_vnode_master:sync_spawn_command({Index, Node}, Msg, riak_search_vnode_master)
        end,
    plists:map(F, Preflist, {processes, 4}).



%%
%% Callbacks for riak_core_vnode
%%

start_vnode(Partition) when is_integer(Partition) ->
    riak_core_vnode_master:get_vnode_pid(Partition, riak_search_vnode).


init([VNodeIndex]) ->
    BMod = app_helper:get_env(riak_search, search_backend),
    Configuration = app_helper:get_env(riak_search),
    {ok, BState} = BMod:start(VNodeIndex, Configuration),
    {ok, #vstate{idx=VNodeIndex,
                 bmod=BMod,
                 bstate=BState}}.

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

handoff_finished(_TargetNode, State) ->
    {ok, State}.

encode_handoff_item({Index,{Field,Term}}, VPKList) ->
    BinObj = term_to_binary({Index,Field,Term,VPKList}),
    <<?HANDOFF_VER:8,BinObj/binary>>.
   
handle_handoff_data(<<?HANDOFF_VER:8,BinObj/binary>>,
                    #vstate{bmod=BMod,bstate=BState}=VState) ->
    {Index,Field,Term,VPKList} = binary_to_term(BinObj),
    [noreply = BMod:index_if_newer(Index, Field, Term, DocID, Props, KeyClock, BState) ||
        {DocID, Props, KeyClock} <- VPKList],
    {reply, ok, VState}.

is_empty(VState=#vstate{bmod=BMod, bstate=BState}) ->
    {BMod:is_empty(BState), VState}.

delete(VState=#vstate{bmod=BMod, bstate=BState}) ->
    ok = BMod:drop(BState),
    {ok, VState}.

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
