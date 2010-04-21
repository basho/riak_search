-module(riak_search_indexer).

-include("riak_search.hrl").

-export([index_doc/1, index/7, index/5]).

index_doc(#riak_indexed_doc{id=DocId, index=Index, fields=Fields}=Doc) ->
    [index(Index, Name, Value, DocId, []) || {Name, Value} <- Fields].

index(Index, Field, Term, Value, Props) ->
    index_internal(Index, Field, Term, {index, Index, Field, Term, Value, Props}).

index(Index, Field, Term, SubType, SubTerm, Value, Props) ->
    index_internal(Index, Field, Term, {index, Index, Field, Term, SubType, SubTerm, Value, Props}).

%% Internal functions
index_internal(Index, Field, Term, Payload) ->
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    Client:put(Obj, 0).
