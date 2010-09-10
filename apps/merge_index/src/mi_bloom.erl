%% @doc Implementation of the Bloom filter data structure.
%% @reference [http://en.wikipedia.org/wiki/Bloom_filter]

%% Adapted from http://code.google.com/p/bloomerl for use in
%% merge_index, where we are worried about speed of creating the bloom
%% filter and testing membership as well as size of the bloom
%% filter. By hard coding some parameters, we reduce the size. Also,
%% by calculating the bloom filter in batches, we improve the
%% performance.

-module(mi_bloom).
-export([new/1, is_element/2]).
-include("merge_index.hrl").

%% These settings give us a max 256 keys with 0.05 error rate.
-define(M, 1600).
-define(K, 4).

%% @doc Generate a new bloom filter containing the specified keys.
new(Keys) ->
    OnBits = lists:usort(lists:flatten([calc_idxs(X) || X <- Keys])),
    list_to_bitstring(generate_bits(0, OnBits)).

generate_bits(Pos, [NextOnPos|T]) ->
    Gap = NextOnPos - Pos - 1,
    case Gap > 0 of
        true ->
            Bits = <<0:Gap/integer, 1:1/integer>>;
        false ->
            Bits = <<1:1/integer>>
    end,
    [Bits|generate_bits(Pos + Gap + 1, T)];
generate_bits(Pos, []) ->
    Gap = ?M - Pos,
    [<<0:Gap/integer>>].

%% @spec is_element(string(), bloom()) -> bool()
%% @doc Determines if the key is (probably) an element of the filter.
is_element(Key, Bitmap) -> 
    is_element(Key, Bitmap, calc_idxs(Key)).
is_element(Key, Bitmap, [Idx | T]) ->
    %% If we are looking for the first bit, do slightly different math
    %% than if we are looking for later bits.
    case Idx > 0 of
        true ->
            PreSize = Idx - 1,
            <<_:PreSize/bits, Bit:1/bits, _/bits>> = Bitmap;
        false ->
            <<Bit:1/bits, _/bits>> = Bitmap
    end,

    %% Check if the bit is on.
    case Bit of
        <<1:1>> -> is_element(Key, Bitmap, T);
        <<0:1>> -> false
    end;
is_element(_, _, []) -> 
    true.

% This uses the "enhanced double hashing" algorithm.
% Todo: handle case of m > 2^32.
calc_idxs(Key) ->
    X = erlang:phash2(Key, ?M),
    Y = erlang:phash2({"salt", Key}, ?M),
    calc_idxs(?K - 1, X, Y, [X]).
calc_idxs(0, _, _, Acc) -> 
    Acc;
calc_idxs(I, X, Y, Acc) ->
    Xi = (X+Y) rem ?M,
    Yi = (Y+I) rem ?M,
    calc_idxs(I-1, Xi, Yi, [Xi | Acc]).
