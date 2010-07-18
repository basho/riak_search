-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).

-define(DEFAULT_ROLLOVER_SIZE, (512 * 1024)).
-define(ROLLOVER_SIZE(State), proplists:get_value(merge_index_rollover_size, State#state.config, ?DEFAULT_ROLLOVER_SIZE)).

-define(DEFAULT_SYNC_INTERVAL, (2 * 1000)).
-define(SYNC_INTERVAL(State), proplists:get_value(merge_index_sync_interval, State#state.config, ?DEFAULT_SYNC_INTERVAL)).
