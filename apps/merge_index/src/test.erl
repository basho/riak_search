%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(test).
-compile(export_all).
-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).

ensure_pid() ->
    Pid = get(pid),
    case erlang:is_pid(Pid) andalso erlang:is_process_alive(Pid) of
        true -> 
            Pid;
        false ->
            {ok, Pid1} = merge_index:start_link("test", []),
            put(pid, Pid1),
            Pid1
    end.

merge() ->
    Pid = ensure_pid(),
    merge_index:compact(Pid).

write(N) ->
    Pid = ensure_pid(),
    spawn_link(fun() -> write_inner(Pid, N) end).

write_inner(_, 0) -> 
    io:format("finished write.~n");
write_inner(Pid, N) ->
    NB = list_to_binary(integer_to_list(N)),
    TS = mi_utils:now_to_timestamp(erlang:now()),
    merge_index:index(Pid, <<"a">>, <<"b">>, <<"c", NB/binary>>, N, [], TS),
    write_inner(Pid, N - 1).

info(N) ->
    Pid = ensure_pid(),
    NB = list_to_binary(integer_to_list(N)),
    merge_index:info(Pid, <<"a">>, <<"b">>, <<"c", NB/binary>>).

info(N1, N2) ->
    Pid = ensure_pid(),
    NB1 = list_to_binary(integer_to_list(N1)),
    NB2 = list_to_binary(integer_to_list(N2)),
    merge_index:info(Pid, <<"a">>, <<"b">>, <<"c", NB1/binary>>, <<"c", NB2/binary>>).
