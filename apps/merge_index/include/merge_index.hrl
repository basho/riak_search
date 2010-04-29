
-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).
-define(DEFAULT_ROLLOVER_SIZE, (50 * 1024 * 1024)).
-define(ROLLOVERSIZE(State), proplists:get_value(rollover_size, State#state.config, ?DEFAULT_ROLLOVER_SIZE)).
-define(MINSUBTERM, 0).
-define(MAXSUBTERM, 18446744073709551615).

-record(state,  { 
    root,
    indexes,
    fields,
    terms,
    segments,
    buffers,
    last_merge,
    merge_pid,
    config
}).

-record(offset, {
    seg_num,
    offset,
    count
}).

