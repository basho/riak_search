%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_utils).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
         ets_keys/1,
         longest_prefix/2,
         edit_signature/2,
         hash_signature/1
]).

ets_keys(Table) ->
    Key = ets:first(Table),
    ets_keys_1(Table, Key).
ets_keys_1(_Table, '$end_of_table') ->
    [];
ets_keys_1(Table, Key) ->
    [Key|ets_keys_1(Table, ets:next(Table, Key))].

%% longest_prefix/2 - Given two terms, calculate the longest common
%% prefix of the terms.
longest_prefix(A, B) ->
    list_to_binary(longest_prefix_1(A, B)).
longest_prefix_1(<<C, A/binary>>, <<C, B/binary>>) ->
    [C|longest_prefix_1(A, B)];
longest_prefix_1(undefined, B) ->
    [B];
longest_prefix_1(_, _) ->
    [].

%% edit_signature/2 - Given an A term and a B term, return a bitstring
%% consisting of a 0 bit for each matching char and a 1 bit for each
%% non-matching char.
edit_signature(A, B) when is_binary(A) andalso is_binary(B) ->
    list_to_bitstring(edit_signature_binary(A, B));
edit_signature(A, B) when is_integer(A) ->
    edit_signature(<<A:32/integer>>, B);
edit_signature(A, B) when is_integer(B) ->
    edit_signature(A, <<B:32/integer>>).

edit_signature_binary(<<C, A/binary>>, <<C, B/binary>>) ->
    [<<0:1/integer>>|edit_signature_binary(A, B)];
edit_signature_binary(<<_, A/binary>>, <<_, B/binary>>) ->
    [<<1:1/integer>>|edit_signature_binary(A, B)];
edit_signature_binary(<<>>, <<_, B/binary>>) ->
    [<<1:1/integer>>|edit_signature_binary(<<>>, B)];
edit_signature_binary(_, <<>>) ->
    [].

%% hash_signature/1 - Given a term, repeatedly xor and rotate the bits
%% of the field to calculate a unique 1-byte signature. This is used
%% for speedier matches.
hash_signature(Term) when is_binary(Term)->
    hash_signature_binary(Term, 0);
hash_signature(Term) when is_integer(Term) ->
    hash_signature(<<Term:64/integer>>).
hash_signature_binary(<<C, Rest/binary>>, Acc) ->
    case Acc rem 2 of
        0 -> 
            RotatedAcc = ((Acc bsl 1) band 255),
            hash_signature_binary(Rest, RotatedAcc bxor C);
        1 -> 
            RotatedAcc = (((Acc bsl 1) + 1) band 255),
            hash_signature_binary(Rest, RotatedAcc bxor C)
    end;
hash_signature_binary(<<>>, Acc) ->
    Acc.
