-define(PRINT(Var), error_logger:info_msg("DEBUG: ~p:~p~n~p~n  ~p~n", [?MODULE, ?LINE, ??Var, Var])).
-define(TIMEON, erlang:put(debug_timer, [now()|case erlang:get(debug_timer) == undefined of true -> []; false -> erlang:get(debug_timer) end])).
-define(TIMEOFF(Var, Count), io:format("~s :: ~p @ ~10.2fms (~10.2f total) : ~p : ~p~n", [string:copies(" ", length(erlang:get(debug_timer))), Count, (timer:now_diff(now(), hd(erlang:get(debug_timer)))/1000/(Count+1)),(timer:now_diff(now(), hd(erlang:get(debug_timer)))/1000), ??Var, Var]), erlang:put(debug_timer, tl(erlang:get(debug_timer)))).

