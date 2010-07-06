-module(riak_search_op_phrase).

-export([preplan_op/2, chain_op/4]).

-include("riak_search.hrl").

preplan_op(#phrase{phrase=Phrase0, base_query=BQ}=Op, F) ->
    Phrase = [C || C <- Phrase0,
                   C /= $\"],
    
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
    
    case BQ of
        [{base_query, {term, Term, Props}}] ->
            BQ1 = {land, [{term, Term, Props}]};
        [{base_query, {term, Term, Props}}, {proximity, _}] ->
            BQ1 = {land, [{term, Term, Props}]};
        _ -> BQ1 = BQ
    end,    
    Op#phrase{phrase=Phrase, base_query=riak_search_op_land:preplan_op(BQ1, F)}.

chain_op(#phrase{phrase=Phrase, base_query=BaseQuery}, OutputPid, OutputRef, QueryProps) ->
    {ok, Client} = riak:local_client(),
    IndexName = proplists:get_value(index_name, QueryProps),
    DefaultField = proplists:get_value(default_field, QueryProps),
    FieldName = get_query_field(DefaultField, BaseQuery),
    F = fun({DocId, _}) ->
                Bucket = riak_search_utils:to_binary(IndexName),
                Key = riak_search_utils:to_binary(DocId),
                case Client:get(Bucket, Key, 1) of
                    {ok, Obj} ->
                        IdxDoc = riak_object:get_value(Obj),
                        #riak_idx_doc{fields=Fields}=IdxDoc,
                        Value = proplists:get_value(FieldName, Fields, ""),
                        string:str(Value, Phrase) > 0;
                    _ ->
                        false
                end end,
    riak_search_op_land:chain_op(BaseQuery, OutputPid, OutputRef, [{term_filter, F}|QueryProps]).

get_query_field(DefaultField, {land, [Op|_]}) ->
    case Op of
        {field, FieldName, _, _} ->
            FieldName;
        _ ->
            DefaultField
    end.
