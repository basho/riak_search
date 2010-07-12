-module(qilr_suite).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    code:add_patha("../ebin"),
    code:add_patha("../../riak_search/ebin"),
    code:add_patha("../../riak_search_core/ebin"),
    application:start(sasl),
    [{setup, fun() -> ok end,
      fun(_) -> ok end,
      [{module, testing_parser}]}].
