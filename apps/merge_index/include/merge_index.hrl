-define(DEFAULT_MERGE_INTERVAL, timer:seconds(10)).
-define(DEFAULT_ROLLOVER_SIZE, (5 * 1024 * 1024)).

-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).
-define(SERVER, ?MODULE).
-define(SEGFILE(StateOrRoot, N), join(StateOrRoot, "seg." ++ integer_to_list(N))).
-define(MERGEINTERVAL(State), proplists:get_value(merge_interval, State#state.config, ?DEFAULT_MERGE_INTERVAL)).
-define(ROLLOVERSIZE(State), proplists:get_value(rollover_size, State#state.config, ?DEFAULT_ROLLOVER_SIZE)).
-define(MINSUBTERM, 0).
-define(MAXSUBTERM, 18446744073709551615).

-record(state,  { 
    root,
    indexes,
    fields,
    terms,
    segments,
    buffer,
    last_merge,
    merge_pid,
    config
}).

-record(offset, {
    seg_num,
    offset,
    count
}).

