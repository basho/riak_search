%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(fixup_test).

-include_lib("eunit/include/eunit.hrl").

fixup_test_() ->
    {foreach,
        fun() ->
                application:load(riak_core),
                application:set_env(riak_core, bucket_fixups, [{riak_search,
                            riak_search_kv_hook}]),
                riak_core_bucket:append_bucket_defaults([{precommit, []}]),
                RingEvtPid = maybe_start_link(riak_core_ring_events:start_link()),
                RingMgrPid = maybe_start_link(riak_core_ring_manager:start_link(test)),
                {RingEvtPid, RingMgrPid}

        end,
        fun({RingEvtPid, RingMgrPid}) ->
                stop_pid(RingMgrPid),
                stop_pid(RingEvtPid),
                wait_until_dead(RingMgrPid),
                wait_until_dead(RingEvtPid),
                application:unset_env(riak_core, bucket_fixups),
                application:unset_env(riak_core, default_bucket_props)
        end,
        [
            fun simple/0,
            fun preexisting_search_hook/0,
            fun rolling_upgrade/0,
            fun other_precommit_hook/0,
            fun install_uninstall/0,
            fun blank_bucket/0,
            fun custom_hook_order/0,
            fun duplicate_hook/0
        ]
    }.


%% Minor sin of cut-and-paste.... (again)
wait_until_dead(Pid) when is_pid(Pid) ->
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, _Obj, Info} ->
            Info
    after 10*1000 ->
            exit({timeout_waiting_for, Pid})
    end;
wait_until_dead(_) ->
    ok.

maybe_start_link({ok, Pid}) -> 
    Pid;
maybe_start_link({error, {already_started, _}}) ->
    undefined.

stop_pid(undefined) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, kill).

simple() ->
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(precommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{search, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props2)),
    riak_core_bucket:set_bucket("testbucket", [{search, false}]),
    Props3 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(precommit, Props3)),
    ok.

preexisting_search_hook() ->
    riak_core_bucket:set_bucket("testbucket", [{precommit,
                [riak_search_kv_hook:precommit_def()]}, {search, false}]),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(precommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{search, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props2)),
    ok.

rolling_upgrade() ->
    application:set_env(riak_core, bucket_fixups, []),
    riak_core_bucket:set_bucket("testbucket1", [{precommit,
                [riak_search_kv_hook:precommit_def()]}]),
    application:set_env(riak_core, bucket_fixups, [{riak_search,
                riak_search_kv_hook}]),
    Props = riak_core_bucket:get_bucket("testbucket1"),
    ?assertEqual([riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props)),
    ?assertEqual(undefined,
        proplists:get_value(search, Props)),
    riak_core_bucket:set_bucket("testbucket1", [{foo, bar}]),
    Props2 = riak_core_bucket:get_bucket("testbucket1"),
    ?assertEqual([riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props2)),
    ?assertEqual(true,
        proplists:get_value(search, Props2)),
    ok.

other_precommit_hook() ->
    riak_core_bucket:set_bucket("testbucket", [{precommit,
                [my_precommit_def()]}]),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_precommit_def()],
        proplists:get_value(precommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{search, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_precommit_def(), riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props2)),
    riak_core_bucket:set_bucket("testbucket", [{search, false}]),
    Props3= riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_precommit_def()],
        proplists:get_value(precommit, Props3)),
    ok.

install_uninstall() ->
    riak_search_kv_hook:install("testbucket"),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props)),
    riak_search_kv_hook:uninstall("testbucket"),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(precommit, Props2)),
    ok.

blank_bucket() ->
    application:set_env(riak_core, default_bucket_props, []),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(undefined,
        proplists:get_value(precommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{search, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props2)),
    riak_core_bucket:set_bucket("testbucket", [{search, false}]),
    Props3 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(precommit, Props3)),
    ok.

custom_hook_order() ->
    riak_core_bucket:set_bucket("testbucket", [{search, true}]),
    riak_core_bucket:set_bucket("testbucket", [{precommit,
                [my_precommit_def(), riak_search_kv_hook:precommit_def()]}]),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_precommit_def(), riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{precommit,
                [riak_search_kv_hook:precommit_def(), my_precommit_def()]}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([riak_search_kv_hook:precommit_def(), my_precommit_def()],
        proplists:get_value(precommit, Props2)),
    ok.

duplicate_hook() ->
    riak_core_bucket:set_bucket("testbucket", [{search, true}]),
    riak_core_bucket:set_bucket("testbucket", [{precommit,
                [riak_search_kv_hook:precommit_def(),
                    my_precommit_def(), riak_search_kv_hook:precommit_def()]}]),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_precommit_def(), riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{precommit,
                [riak_search_kv_hook:precommit_def(),
                    riak_search_kv_hook:precommit_def(),
                    riak_search_kv_hook:precommit_def(),
                    my_precommit_def(),
                    riak_search_kv_hook:precommit_def(),
                    riak_search_kv_hook:precommit_def(),
                    riak_search_kv_hook:precommit_def()]
            }]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_precommit_def(), riak_search_kv_hook:precommit_def()],
        proplists:get_value(precommit, Props2)),
    ok.


my_precommit_def() ->
    {struct, [{<<"mod">>,atom_to_binary(?MODULE, latin1)},
              {<<"fun">>,<<"precommit">>}]}.
