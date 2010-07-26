%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_nif).

-export([segidx_new/1,
         segidx_lookup/2,
         segidx_lookup_nearest/2,
         segidx_entry_count/1,
         segidx_ift_count/2, segidx_ift_count/3]).

-on_load(init/0).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

init() ->
    case code:priv_dir(merge_index) of
        {error, bad_name} ->
            SoName = filename:join("priv", mi_nif);
        Dir ->
            SoName = filename:join(Dir, mi_nif)
    end,
    erlang:load_nif(SoName, 0).

segidx_new(_Bin) ->
    "NIF library not loaded".

segidx_lookup(Ref, Ift) ->
    case segidx_lookup_bin(Ref, <<Ift:64/native-unsigned>>) of
        {ok, <<Ift:64/native-unsigned, Offset:64/native-unsigned, Count:32/native-unsigned>>} ->
            {ok, Offset, Count};
        not_found ->
            not_found
    end.

segidx_lookup_nearest(Ref, TargetIft) ->
    case segidx_lookup_nearest_bin(Ref, <<TargetIft:64/native-unsigned>>) of
        {ok, <<Ift:64/native-unsigned, Offset:64/native-unsigned, Count:32/native-unsigned>>} ->
            {ok, Ift, Offset, Count};
        not_found ->
            not_found
    end.


segidx_ift_count(Ref, Ift) ->
    segidx_ift_count_bin(Ref, <<Ift:64/native-unsigned>>).

segidx_ift_count(Ref, StartIft, EndIft) ->
    <<Count:64/native-unsigned>> = segidx_ift_count_bin(Ref, <<StartIft:64/native-unsigned>>,
                                                        <<EndIft:64/native-unsigned>>),
    Count.

segidx_entry_count(_Ref) ->
    "NIF library not loaded".

segidx_ift_count_bin(_Ref, _IftBin) ->
    "NIF library not loaded".

segidx_ift_count_bin(_Ref, _StartIftBin, _EndIftBin) ->
    "NIF library not loaded".

segidx_lookup_bin(_Ref, _IftBin) ->
    "NIF library not loaded".

segidx_lookup_nearest_bin(_Ref, _IftBin) ->
    "NIF library not loaded".

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

g_uint64() ->
    choose(0, ?POW_2(62)).

g_uint32() ->
    choose(0, ?POW_2(31)).

g_uint64_range() ->
    ?SUCHTHAT({Int0, Int1}, {g_uint64(), g_uint64()},
              Int0 =< Int1).

offsets_to_bin([], Acc) ->
    Acc;
offsets_to_bin([{Ift, Offset, Count} | Rest], Acc) ->
    offsets_to_bin(Rest, <<Acc/binary, Ift:64/native-unsigned, Offset:64/native-unsigned,
                           Count:32/native-unsigned>>).

prop_segidx_basic() ->
    ?FORALL(List, list({g_uint64(), g_uint64(), g_uint32()}),
            begin
                %% Pack the list into a binary (sorting and removing any dups first!)
                Offsets = lists:ukeysort(1, List),
                OffsetsBin = offsets_to_bin(Offsets, <<>>),

                %% Setup NIF and inject the binary
                {ok, Ref} = segidx_new(OffsetsBin),

                %% Verify that all the segments are present and match
                [{ok, Offset, Count} = segidx_lookup(Ref, Ift) || {Ift, Offset, Count} <- Offsets],

                %% Verify that the # of entries in the segidx match expected
                ?assertEqual(length(Offsets), segidx_entry_count(Ref)),
                true
            end).

prop_segidx_basic_test_() ->
    {timeout, 60, fun() -> ?assert(eqc:quickcheck(?QC_OUT(prop_segidx_basic()))) end}.

prop_segidx_counts() ->
    ?FORALL({Range, List}, {g_uint64_range(), list({g_uint64(), g_uint64(), g_uint32()})},
             begin
                 %% Unpack range
                 {StartIft, StopIft} = Range,

                 %% Pack the list into a binary (sorting and removing any dups first!)
                 Offsets = lists:ukeysort(1, List),
                 OffsetsBin = offsets_to_bin(Offsets, <<>>),

                 %% Setup NIF and inject the binary
                 {ok, Ref} = segidx_new(OffsetsBin),

                 %% Calculate the expected counts for the provided range
                 ExpectedCount = lists:sum([Count || {Ift, _Offset, Count} <- Offsets,
                                                     Ift >= StartIft, Ift =< StopIft]),

                 ?assertEqual(ExpectedCount, segidx_ift_count(Ref, StartIft, StopIft)),
                 true
             end).

prop_segidx_counts_test_() ->
    {timeout, 60, fun() -> ?assert(eqc:quickcheck(?QC_OUT(prop_segidx_counts()))) end}.

prop_segidx_nearest() ->
    ?FORALL({TargetIft, List}, {g_uint64(), list({g_uint64(), g_uint64(), g_uint32()})},
             begin
                 %% Pack the list into a binary (sorting and removing any dups first!)
                 Offsets = lists:ukeysort(1, List),
                 OffsetsBin = offsets_to_bin(Offsets, <<>>),

                 %% Setup NIF and inject the binary
                 {ok, Ref} = segidx_new(OffsetsBin),

                 %% Find all IFTs in the list >= TargetIft
                 Tail = [{Ift, Offset, Count} || {Ift, Offset, Count} <- Offsets,
                                                 Ift >= TargetIft],
                 case Tail of
                     [] ->
                         Expected = not_found;
                     [{Ift, Offset, Count} | _] ->
                         Expected = {ok, Ift, Offset, Count}
                 end,

                 ?assertEqual(Expected, segidx_lookup_nearest(Ref, TargetIft)),
                 true
             end).

prop_segidx_nearest_test_() ->
    {timeout, 60, fun() -> ?assert(eqc:quickcheck(?QC_OUT(prop_segidx_nearest()))) end}.


-endif. % EQC

-endif.
