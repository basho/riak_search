-module(riak_search_repl_helper).

-export([send/2, recv/1]).

%% when sending the object, ensure that both the KV and proxy objexts are sent
send(Obj, C) ->
    B = riak_object:bucket(Obj),
    K = riak_object:key(Obj),
    PO = is_proxy_object(B),
    SHI = proplists:get_value(search, C:get_bucket(B)),

    case SHI of
        true ->
            send_search(PO, Obj, B, K, C);
        false ->
            case PO of
                true -> lager:debug("Outgoing proxy obj ~p/~p", [B, K]);
                false -> lager:debug("Outgoing KV obj ~p/~p", [B, K])
            end,
            ok
    end.

send_search(true, PO, IdxB, K, C) ->
    lager:debug("Outgoing indexed KV obj ~p/~p", [IdxB, K]),
    <<"_rsid_",B/binary>> = IdxB,
    case C:get(B, K) of
        {ok, KVO} ->
            [KVO];
        Other ->
            lager:info("Couldn't find expected KV obj ~p/~p ~p", [B, K, Other]),
            ok
    end;

send_search(false, KVO, B, K, C) ->
    lager:debug("Outgoing indexed KV obj ~p/~p", [B, K]),
    IdxB = <<"_rsid_",B/binary>>,
    case C:get(IdxB, K) of
        {ok, PO} ->
            [PO];
        Other ->
            lager:info("Couldn't find expected proxy obj ~p/~p ~p",
                       [IdxB, K, Other]),
            ok
    end.

is_proxy_object(B) ->
    case binary:matches(B, <<"_rsid_">>) of
        [] -> false;
        _ -> true
    end.

%% check whether to add/delete indexes on repl recv
recv(Object) ->
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    {ok, C} = riak:local_client(),
    SC = riak_search_client:new(C),
    case B of
        <<"_rsid_", Idx/binary>> ->
            case riak_kv_util:is_x_deleted(Object) of
                true ->
                    lager:debug("Incoming deleted proxy obj ~p/~p", [B, K]),
                    riak_indexed_doc:remove_entries(C, SC, Idx, K),
                    ok;
                false ->
                    lager:debug("Incoming proxy obj ~p/~p", [B, K]),
                    IdxDoc = riak_object:get_value(Object),
                    riak_indexed_doc:remove_entries(C, SC, Idx, K),
                    Postings = riak_indexed_doc:postings(IdxDoc),
                    SC:index_terms(Postings),
                    ok
            end;
        _ ->
            ok
    end.
