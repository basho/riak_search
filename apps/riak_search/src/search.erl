%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%%% Convenience module for interacting with Search from the Erlang
%%% command line. Allows the user to search for keys and docs, explain
%%% queries, index documents in a directory, index a set of fields,
%%% de-index (delete) documents in a directory, and delete a specific
%%% document.
%%%
%%% It provides convenience in three main ways:
%%%
%%% 1. Takes care of instantiating the riak_search_client
%%%    parameterized module.
%%%
%%% 2. Wraps arguments into structures such as the #riak_idx_doc.
%%%
%%% 3. Supplies reasonable defaults when calling functions in
%%%    riak_search_client.

-module(search).
-export([
    %% Querying.
    search/1, search/2,
    search_doc/1, search_doc/2,

    %% Inspection.
    explain/1, explain/2,
    graph/1, graph/2,

    %% Indexing.
    index_doc/2, index_doc/3, index_doc/4, index_docs/1,
    index_dir/1, index_dir/2,

    %% Deletion.
    delete_doc/2, delete_docs/1,
    delete_dir/1, delete_dir/2
]).

-include("riak_search.hrl").

search(Q) ->
    search(?DEFAULT_INDEX, Q).

search(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Q) of
        {ok, Ops} ->
            Client:search(Index, Ops, 0, 10000, 60000);
        {error, Error} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [Q, Error]),
            {error, Error}
    end.

search_doc(Q) ->
    search_doc(?DEFAULT_INDEX, Q).

search_doc(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Q) of
        {ok, Ops} ->
            Client:search_doc(Index, Ops, 0, 10000, 60000);
        {error, Error} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [Q, Error]),
            {error, Error}
    end.

explain(Q) ->
    explain(?DEFAULT_INDEX, Q).

explain(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Q) of
        {ok, Ops} ->
            Client:explain(Index, Ops);
        {error, Error} ->
            M = "Error running query '~s': ~p~n",
            error_logger:error_msg(M, [Q, Error]),
            {error, Error}
    end.

graph(Q) ->
    graph(?DEFAULT_INDEX, Q).

graph(Index, Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:query_as_graph(explain(Index, Q)).

%% See index_doc/4.
index_doc(ID, Fields) ->
    index_doc(?DEFAULT_INDEX, ID, Fields, []).

%% See index_doc/4.
index_doc(Index, ID, Fields) ->
    index_doc(Index, ID, Fields, []).

%% Index a document.
%% @param Index - The index.
%% @param Fields - A list of {Key, Value} fields.
%% @param Props - A list of {Key, Value} props.
index_doc(Index, ID, Fields, Props) ->
    IdxDoc = riak_indexed_doc:new(Index, ID, Fields, Props),
    index_docs([IdxDoc]).

index_docs(Docs) ->
    %% Convert to IdxDocs...
    F = fun(IdxDoc) when is_record(IdxDoc, riak_idx_doc) ->
                IdxDoc;
           ({ID, Fields}) ->
                riak_indexed_doc:new(?DEFAULT_INDEX, ID, Fields, []);
           ({Index, ID, Fields}) ->
                riak_indexed_doc:new(Index, ID, Fields, []);
           ({ID, Fields, Props, Index}) ->
                riak_indexed_doc:new(Index, ID, Fields, Props)
        end,
    IdxDocs = [F(X) || X <- Docs],
    
    %% Index the IdxDocs...
    {ok, Client} = riak_search:local_client(),
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    try
        Client:index_docs(IdxDocs, AnalyzerPid)
    after
        qilr:close_analyzer(AnalyzerPid)
    end,
    ok.

%% Full text index the specified directory of plain text files.
index_dir(Directory) ->
    index_dir(?DEFAULT_INDEX, Directory).

%% Full text index the specified directory of plain text files.
index_dir(Index, Directory) ->
    riak_search_dir_indexer:start_index(Index, Directory).

delete_dir(Directory) ->
    delete_dir(?DEFAULT_INDEX, Directory).

delete_dir(Index, Directory) ->
    F = fun(DocId, _Body) ->
        delete_doc(Index, DocId)
    end,
    riak_search_utils:index_recursive(F, Directory),
    ok.

delete_doc(DocIndex, DocID) ->
    delete_docs([{DocIndex, DocID}]).

delete_docs(Docs) ->
    %% Convert to {DocIndex,DocID}...
    F = fun(IdxDoc) when is_record(IdxDoc, riak_idx_doc) ->
                {riak_indexed_doc:index(IdxDoc), riak_indexed_doc:id(IdxDoc)};
           ({DocIndex, DocID}) ->
                {DocIndex, DocID}
        end,
    DocIndexIds = [F(X) || X <- Docs],

    %% Delete the DocIndexIds...
    {ok, Client} = riak_search:local_client(),
    Client:delete_docs(DocIndexIds).
