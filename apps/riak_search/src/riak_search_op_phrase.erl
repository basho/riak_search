%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_phrase).

-export([preplan_op/2, chain_op/4]).

-include_lib("qilr/include/qilr.hrl").
-include("riak_search.hrl").

preplan_op(#phrase{phrase=Phrase0, props=Props}=Op, F) ->
    case proplists:get_value(op_mod, Props) of
        undefined ->
            Phrase = list_to_binary([C || C <- binary_to_list(Phrase0),
                                          C /= 34]), %% double quotes
            BQ = proplists:get_value(base_query, Props),
            {Mod, BQ1} = case is_tuple(BQ) of
                             true ->
                                 {riak_search_op_term, BQ};
                             false ->
                                 case hd(BQ) of
                                     {land, [Term]} ->
                                         {riak_search_op_term, Term};
                                     {land, Terms} ->
                                         {riak_search_op_land, {land, Terms}}
                                 end
                         end,
            BQ2 = Mod:preplan_op(BQ1, F),
            Props1 = proplists:delete(base_query, Props),
            Props2 = [{base_query, BQ2},
                      {op_mod, Mod}] ++ Props1,
            Op#phrase{phrase=Phrase, props=Props2};
        _ ->
            Op
    end.

chain_op(#phrase{phrase=Phrase, props=Props}, OutputPid, OutputRef, QueryProps) ->
    BaseQuery = proplists:get_value(base_query, Props),
    OpMod = proplists:get_value(op_mod, Props),
    {ok, Client} = riak:local_client(),
    FieldName = proplists:get_value(default_field, QueryProps),
    F = fun({IndexName, DocId, _}) ->
                {ok, Analyzer} = qilr:new_analyzer(),
                try
                    Bucket = riak_search_utils:to_binary(IndexName),
                    Key = riak_search_utils:to_binary(DocId),
                    case Client:get(Bucket, Key, 1) of
                        {ok, Obj} ->
                            IdxDoc = riak_object:get_value(Obj),
                            #riak_idx_doc{fields=Fields}=IdxDoc,
                            Value = proplists:get_value(FieldName, Fields, ""),
                            case proplists:get_value(proximity, Props, undefined) of
                                undefined ->
                                    Evaluator = case proplists:get_value(prohibited,
                                                                         Props,
                                                                         false) of
                                                    true ->
                                                        fun erlang:'=='/2;
                                                    false ->
                                                        fun erlang:'>'/2
                                                end,
                                    Evaluator(string:str(Value, binary_to_list(Phrase)), 0);
                                Distance ->
                                    ProxTerms = proplists:get_value(proximity_terms, Props, []),
                                    {ok, AValue} = qilr_analyzer:analyze(Analyzer,
                                                                         Value, ?WHITESPACE_ANALYZER),
                                    evaluate_proximity(AValue, Distance, ProxTerms)
                            end;
                        _ ->
                            false
                    end
                after
                    qilr:close_analyzer(Analyzer)
                end
        end,
    OpMod:chain_op(BaseQuery, OutputPid, OutputRef, [{term_filter, F}|QueryProps]).

evaluate_proximity(_Value, _Distance, []) ->
    false;
evaluate_proximity(Value, Distance, ProxTerms) ->
    case find_term_indices(ProxTerms, 1, Value, []) of
        false ->
            false;
        [Index] ->
            Index =< Distance;
        Indices ->
            evaluate_indices(Indices, Distance)
    end.

find_term_indices([], _Counter, _Val, Accum) ->
    lists:reverse(Accum);
find_term_indices(_Terms, _Counter, [], _Accum) ->
    false;
find_term_indices(Terms, Counter, [H|T], Accum) ->
    case lists:member(H, Terms) of
        true ->
            find_term_indices(lists:delete(H, Terms), Counter + 1, T, [Counter|Accum]);
        false ->
            find_term_indices(Terms, Counter + 1, T, Accum)
    end.

evaluate_indices([F, S], Distance) ->
    erlang:abs(F - S) =< Distance;
evaluate_indices([F, S|T], Distance) ->
    case erlang:abs(F - S) =< Distance of
        true ->
            evaluate_indices([S|T], Distance);
        false ->
            false
    end.
