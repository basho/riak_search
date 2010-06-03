-module(riak_search_op_phrase).

-export([preplan_op/2, chain_op/4]).

-include("riak_search.hrl").

preplan_op(#phrase{phrase=Phrase0, base_query=BQ}=Op, F) ->
    Phrase = [C || C <- Phrase0,
                   C /= $\"],
    Op#phrase{phrase=Phrase, base_query=riak_search_op_land:preplan_op(BQ, F)}.

chain_op(#phrase{phrase=Phrase, base_query=BaseQuery}, OutputPid, OutputRef, QueryProps) ->
    {ok, Client} = riak:local_client(),
    IndexName = proplists:get_value(index_name, QueryProps),
    DefaultField = proplists:get_value(default_field, QueryProps),
    FieldName = get_query_field(DefaultField, BaseQuery),
    F = fun({DocId, _}) ->
                Bucket = riak_search_utils:to_binary(IndexName ++ "_docs"),
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
