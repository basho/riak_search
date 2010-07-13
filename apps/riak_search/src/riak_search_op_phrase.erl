%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_phrase).

-export([preplan_op/2, chain_op/4]).

-include_lib("qilr/include/qilr.hrl").
-include("riak_search.hrl").

preplan_op(#phrase{phrase=Phrase0, base_query=BQ}=Op, F) ->
    Phrase = [C || C <- Phrase0,
                   C /= 34], %% double quotes

    %% This is a temporary fix for Bugzilla #336
    %%
    %% Woven into the parser and preplanner is the assumption that when there is a single
    %%  query component (e.g. "{term, ..}") it will not be enclosed in a list, therefore
    %%  when a phrase query such as "the problem" comes through, "the" gets filtered out
    %%  by the analyzer (which it should), but then becomes a single term not enclosed in
    %%  a list so then never gets converted to a land operation; this would normally be fine
    %%  except the way the query execution mechanism works, all phrases decompose to lands
    %%  and expect as much as per the last line in this function.
    %% The "fix" below detects the case where the phrase has, through the analyze process,
    %%  decomposed to a single term, and therefore a malformed land operation, and
    %%  "fixes" it.  This poses no real performance problem and doesn't have any further
    %%  side effects, so if everyone is comfortable with it, we'll keep it.
    %% If you are not comfortable with this fix, your answer lies in the guts of qilr's
    %%  leex/yecc grammar and code and somewhere in the preplanner.
    %%
    %% e.g.,
    %%   [{base_query,{term,<<"problem">>,[]}}]
    %%  to:
    %%   {land, [{term, "problem", []}]}
    %%
    %% Update: added another similar case for single-term decomps with proximity searches.
    %%
    BQ1 = case BQ of
              BQ when is_tuple(BQ),
                      size(BQ) == 3 orelse size(BQ) == 4 ->
                  {land, [BQ]};
              _ ->
                  BQ
          end,
    Op#phrase{phrase=Phrase, base_query=riak_search_op_land:preplan_op(BQ1, F)}.

chain_op(#phrase{phrase=Phrase, base_query=BaseQuery, props=Props}, OutputPid, OutputRef, QueryProps) ->
    {ok, Client} = riak:local_client(),
    IndexName = proplists:get_value(index_name, QueryProps),
    DefaultField = proplists:get_value(default_field, QueryProps),
    FieldName = get_query_field(DefaultField, BaseQuery),
    F = fun({DocId, _}) ->
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
                                    string:str(Value, Phrase) > 0;
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
    riak_search_op_land:chain_op(BaseQuery, OutputPid, OutputRef, [{term_filter, F}|QueryProps]).

get_query_field(DefaultField, {land, [Op|_]}) ->
    case Op of
        {field, FieldName, _, _} ->
            FieldName;
        _ ->
            DefaultField
    end.

evaluate_proximity(_Value, _Distance, []) ->
    false;
evaluate_proximity(Value, Distance, ProxTerms) ->
    case find_term_indices(ProxTerms, 1, Value, []) of
        false ->
            false;
        [Index] ->
            Index =< Distance;
        Indices ->
            io:format("Indices: ~p~n", [Indices]),
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
