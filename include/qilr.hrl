-define(WHITESPACE_ANALYZER, {erlang, text_analyzers, whitespace_analyzer_factory}).
-define(INTEGER_ANALYZER,    {erlang, text_analyzers, integer_analyzer_factory}).
-define(NOOP_ANALYZER,       {erlang, text_analyzers, noop_analyzer_factory}).

-define(CONN_POOL, qilr_conn_pool).

-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.
