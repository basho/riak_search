%% @doc Support for 2i interface atop Riak Search.
-module(riak_search_2i).
-export([does_contain_idx/1]).
-include("riak_search.hrl").
-define(MD_INDEX, <<"index">>).

%%%===================================================================
%%% Types
%%%===================================================================
-type field_type() :: int | bin.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Predicate to determine if the given metadata `MD' contains 2i
%%      indexes.
-spec does_contain_idx(dict()) -> boolean().
does_contain_idx(MD) ->
    dict:is_key(?MD_INDEX, MD).

%% @doc Extract field/data pairs `SearchFields' from the given Riak
%%      Object `Obj'.
-spec extract(riak_object:riak_object()) -> SearchFields::search_fields().
extract(Obj) ->
    MD = riak_object:get_metadata(Obj),
    Tags = dict:fetch(?MD_INDEX, MD),
    [{field_pair(Field), riak_search_utils:to_utf8(Value)}
     || {Field, Value} <- Tags].

%% @doc Given a field `Field' return a pair describing it's type
%%      `Type' and it's name `FieldName'.
%%
%% TODO: Rather than throwing errors allow them to propogate up and
%%       give something human friendly.
-spec field_pair(binary()) -> {Type::field_type(), FieldName::binary()}
                                  | {error, any()}.
field_pair(Field) ->
    Matches = binary:matches(Field, <<"_">>),
    case Matches of
        [] ->
            throw({error, {no_field_type, Field}});
        _ ->
            {Pos, _} = lists:last(Matches),
            {FieldName, Suffix} = split_binary(Field, Pos),
            case Suffix of
                <<"_int">> -> {int, FieldName};
                <<"_bin">> -> {bin, FieldName};
                _ -> throw({error, {unknown_field_type, Suffix}})
            end
    end.
