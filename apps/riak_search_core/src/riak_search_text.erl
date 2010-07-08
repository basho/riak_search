-module(riak_search_text).

-export([left_pad/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

left_pad(Value, Count, Char) when is_binary(Value) ->
    list_to_binary(left_pad(binary_to_list(Value), Count, Char));
left_pad([F|T]=Value, Count, Char) ->
    [F|T] = Value,
    case F of
        $- ->
            [$-|left_pad1(T, Count, Char)];
        _ ->
            left_pad1(Value, Count, Char)
    end.

left_pad1(V, Count, _Char) when length(V) >= Count ->
    V;
left_pad1(V, Count, Char) ->
    left_pad1([Char|V], Count, Char).

-ifdef(TEST).
padding_test() ->
    ?assertEqual(<<"0000000500">>, left_pad(<<"500">>, 10, $0)),
    ?assertEqual("0000000001", left_pad("1", 10, $0)),
    ?assertEqual("-0000000001", left_pad("-1", 10, $0)),
    ?assertEqual("0000001.89", left_pad("1.89", 10, $0)),
    ?assertEqual("-0000001.89", left_pad("-1.89", 10, $0)).
-endif.
