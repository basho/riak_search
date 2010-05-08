-module(qilr_suite).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    code:add_patha("../ebin"),
    application:start(sasl),
    application:start(qilr),
    [{setup, fun() -> ok end,
      fun(_) -> ok end,
      [{module, testing_parser}]}].
