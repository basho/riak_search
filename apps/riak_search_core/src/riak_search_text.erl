-module(riak_search_text).

-export([left_pad/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

left_pad([F|T]=Value, Count) ->
    [F|T] = Value,
    case F of
        $- ->
            [$-|left_pad1(T, Count)];
        _ ->
            left_pad1(Value, Count)
    end.

left_pad1(V, Count) when length(V) >= Count ->
    V;
left_pad1(V, Count) ->
    left_pad1([$0|V], Count).

-ifdef(TEST).
padding_test() ->
    ?assertEqual("0000000001", left_pad("1", 10)),
    ?assertEqual("-0000000001", left_pad("-1", 10)),
    ?assertEqual("0000001.89", left_pad("1.89", 10)),
    ?assertEqual("-0000001.89", left_pad("-1.89", 10)).
-endif.
