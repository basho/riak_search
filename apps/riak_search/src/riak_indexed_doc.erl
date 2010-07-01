-module(riak_indexed_doc).

-include("riak_search.hrl").

-export([new/2, id/1, fields/1, add_field/3, set_fields/2, clear_fields/1,
         props/1, add_prop/3, set_props/2, clear_props/1, to_json/1,
         from_json/1, to_mochijson2/1, to_mochijson2/2]).

new(Id, Index) ->
    #riak_idx_doc{id=Id, index=Index}.

fields(#riak_idx_doc{fields=Fields}) ->
    Fields.

id(#riak_idx_doc{id=Id}) ->
    Id.

add_field(Name, Value, #riak_idx_doc{fields=Fields}=Doc) ->
    Doc#riak_idx_doc{fields=[{Name, Value}|Fields]}.

set_fields(Fields, Doc) ->
    Doc#riak_idx_doc{fields=Fields}.

clear_fields(Doc) ->
    Doc#riak_idx_doc{fields=[]}.

props(#riak_idx_doc{props=Props}) ->
    Props.

add_prop(Name, Value, #riak_idx_doc{props=Props}=Doc) ->
    Doc#riak_idx_doc{props=[{Name, Value}|Props]}.

set_props(Props, Doc) ->
    Doc#riak_idx_doc{props=Props}.

clear_props(Doc) ->
    Doc#riak_idx_doc{props=[]}.

to_json(Doc) ->
    mochijson2:encode(to_mochijson2(Doc)).

to_mochijson2(Doc) ->
    F = fun({_Name, Value}) -> Value end,
    to_mochijson2(F, Doc).

to_mochijson2(XForm, #riak_idx_doc{id=Id, index=Index, fields=Fields, props=Props}) ->
    {struct, [{id, riak_search_utils:to_binary(Id)},
              {index, riak_search_utils:to_binary(Index)},
              {fields, {struct, [{riak_search_utils:to_binary(Name),
                                  XForm({Name, Value})} || {Name, Value} <- lists:keysort(1, Fields)]}},
              {props, {struct, [{riak_search_utils:to_binary(Name),
                                 riak_search_utils:to_binary(Value)} || {Name, Value} <- Props]}}]}.

from_json(Json) ->
    case mochijson2:decode(Json) of
        {struct, Data} ->
            Id = proplists:get_value(<<"id">>, Data),
            Index = proplists:get_value(<<"index">>, Data),
            build_doc(Id, Index, Data);
        {error, _} = Error ->
            Error;
        _NonsenseJson ->
            {error, bad_json_format}
    end.

build_doc(Id, Index, _Data) when Id =:= undefined orelse
                                 Index =:= undefined ->
    {error, missing_id_or_index};
build_doc(Id, Index, Data) ->
    #riak_idx_doc{id=riak_search_utils:from_binary(Id), index=binary_to_list(Index),
                  fields=read_json_fields(<<"fields">>, Data),
                  props=read_json_fields(<<"props">>, Data)}.

read_json_fields(Key, Data) ->
    case proplists:get_value(Key, Data) of
        {struct, Fields} ->
            [{riak_search_utils:from_binary(Name),
              riak_search_utils:from_binary(Value)} || {Name, Value} <- Fields];
        _ ->
            []
    end.
