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
    search/1, search/2, search/3,
    search_doc/1, search_doc/2, search_doc/3,

    %% Inspection.
    explain/1, explain/2, explain/3,
    graph/1, graph/2,

    %% Indexing.
    index_doc/2, index_doc/3, index_doc/4, index_docs/1,
    index_dir/1, index_dir/2,

    %% Deletion.
    delete_doc/2, delete_docs/1,
    delete_dir/1, delete_dir/2
]).

-include("riak_search.hrl").

search(Query) ->
    search(?DEFAULT_INDEX, Query, "").

search(Index, Query) ->
    search(Index, Query, "").

search(Index, Query, Filter) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Query) of
        {ok, QueryOps} ->
            case Client:parse_filter(Index, Filter) of
                {ok, FilterOps} ->
                    Client:search(Index, QueryOps, FilterOps, 0, 10000, 60000);
                {error, Error} ->
                    lager:error("Error parsing filter '~s': ~p",
                                [Filter, Error]),
                    {error, Error}
            end;
        {error, Error} ->
            lager:error("Error parsing query '~s': ~p",
                        [Query, Error]),
            {error, Error}
    end.

search_doc(Query) ->
    search_doc(?DEFAULT_INDEX, Query, "").

search_doc(Index, Query) ->
    search_doc(Index, Query, "").

search_doc(Index, Query, Filter) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Query) of
        {ok, QueryOps} ->
            case Client:parse_filter(Index, Filter) of
                {ok, FilterOps} ->
                    Client:search_doc(Index, QueryOps, FilterOps, 0, 10000, 60000);
                {error, Error} ->
                    lager:error("Error parsing filter '~s': ~p",
                                [Filter, Error]),
                    {error, Error}
            end;
        {error, Error} ->
            lager:error("Error running query '~s': ~p", [Query, Error]),
            {error, Error}
    end.

explain(Query) ->
    explain(?DEFAULT_INDEX, Query).

explain(Index, Query) ->
    explain(Index, Query, "").


explain(Index, Query, Filter) ->
    {ok, Client} = riak_search:local_client(),
    case Client:parse_query(Index, Query) of
        {ok, QueryOps} ->
            case Client:parse_filter(Index, Filter) of
                {ok, FilterOps} ->
                    [{'query', QueryOps}, {'filter', FilterOps}];
                {error, Error} ->
                    lager:error("Error parsing filter '~s': ~p",
                                [Filter, Error]),
                    {error, Error}
            end;
        {error, Error} ->
            lager:error("Error parsing query '~s': ~p", [Query, Error]),
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
    Client:index_docs(IdxDocs),
    ok.

%% Full text index the specified directory of plain text files.
index_dir(Directory) ->
    index_dir(?DEFAULT_INDEX, Directory).

%% Full text index the specified directory of plain text files.
index_dir(Index, Directory) ->
    Fun = fun(Schema, Files) ->
                  F = fun(File) ->
                              {ok, Data} = file:read_file(File),
                              DocID = riak_search_utils:to_binary(filename:basename(File)),
                              Fields = [{Schema:default_field(), Data}],
                              riak_indexed_doc:new(Schema:name(), DocID, Fields, [])
                      end,
                  IdxDocs = [F(X) || X <- Files],
                  {ok, Client} = riak_search:local_client(),
                  Client:index_docs(IdxDocs)
          end,
    riak_search_dir_indexer:index(Index, Directory, Fun).

delete_dir(Directory) ->
    delete_dir(?DEFAULT_INDEX, Directory).

delete_dir(Index, Directory) ->
    Fun = fun(Schema, Files) ->
                  F = fun(File) ->
                              DocID = riak_search_utils:to_binary(filename:basename(File)),
                              {Schema:name(), DocID}
                      end,
                  IdxDocs = [F(X) || X <- Files],
                  {ok, Client} = riak_search:local_client(),
                  Client:delete_docs(IdxDocs)
          end,
    riak_search_dir_indexer:index(Index, Directory, Fun).

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
