-module(riak_search_indexer).

-include("riak_search.hrl").

-export([index_doc/1, index_store/2, index_store/3, index/7, index/5, test/0]).

index_doc(#riak_indexed_doc{id=DocId, index=Index, fields=Fields}=Doc) ->
    [index(Index, Name, Value, DocId, []) || {Name, Value} <- analyze_fields(Fields, [])],
    DocBucket = Index ++ "_docs",
    DocObj = riak_object:new(riak_search_utils:to_binary(DocBucket),
                             riak_search_utils:to_binary(DocId),
                             Doc),
    {ok, Client} = riak:local_client(),
    Client:put(DocObj, 1).

index_store(Doc, Obj) ->
    index_store(Doc, Obj, 1).

index_store(#riak_indexed_doc{id=DocId}=Doc, Obj0, W) ->
    Md = dict:store("X-Riak-Search-Id", DocId, dict:new()),
    Obj = riak_object:update_metadata(Obj0, Md),
    ok = index_doc(Doc),
    {ok, Client} = riak:local_client(),
    Client:put(Obj, W).

index(Index, Field, Term, Value, Props) ->
    index_internal(Index, Field, Term, {index, Index, Field, Term, Value, Props}).

index(Index, Field, Term, SubType, SubTerm, Value, Props) ->
    index_internal(Index, Field, Term, {index, Index, Field, Term, SubType, SubTerm, Value, Props}).

test() ->
    Doc = #riak_indexed_doc{id="123",
                            index="search",
                            fields=[{"content", "This is a test of the emergency broadcast system."}]},
    index_doc(Doc).

%% Internal functions
index_internal(Index, Field, Term, Payload) ->
    IndexBin = riak_search_utils:to_binary(Index),
    FieldTermBin = riak_search_utils:to_binary([Field, ".", Term]),
    %% Run the operation...
    {ok, Client} = riak:local_client(),
    Obj = riak_object:new(IndexBin, FieldTermBin, Payload),
    Client:put(Obj, 0).

analyze_fields([], Accum) ->
    Accum;
analyze_fields([{Name, Value}|T], Accum) ->
    {ok, Tokens} = basho_analyzer:analyze(Value),
    analyze_fields(T, [{Name, binary_to_list(Token)} || Token <- Tokens] ++ Accum).
