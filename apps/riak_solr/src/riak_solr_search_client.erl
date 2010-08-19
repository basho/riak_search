%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_search_client, [RiakClient, SearchClient]).
-export([parse_solr_xml/2,
         run_solr_command/3
]).

-define(DEFAULT_INDEX, "search").
-define(DEFAULT_TIMEOUT, 60000).

-include_lib("riak_search/include/riak_search.hrl").

%% Parse a solr XML formatted file.
parse_solr_xml(IndexOrSchema, Body) when is_binary(Body) ->
    %% Get the schema...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),

    %% Parse the xml...
    {ok, Command, Entries} = riak_solr_xml_xform:xform(Body),

    %% DEBUG - Testing new parser.
    %% {ok, Command, Entries} = riak_solr_xml:parse(Body),

    ParsedDocs = [parse_solr_entry(Schema, Command, X) || X <- Entries],
    {ok, Command, ParsedDocs}.

%% @private
%% Parse a document to add...
parse_solr_entry(Schema, add, {"doc", Entry}) ->
    IdxDoc0 = to_riak_idx_doc(Schema, Entry),
    {ok, IdxDoc} = riak_indexed_doc:analyze(IdxDoc0),
    IdxDoc;

%% Deletion by ID or Query. If query, then parse...
parse_solr_entry(Schema, delete, {"id", ID}) ->
    case string:tokens(binary_to_list(ID), ":") of
        [] ->
            throw({?MODULE, empty_id_on_solr_delete});
        [H] -> 
            {'id', Schema:name(), H};
        [H|T] -> 
            {'id', H, string:join(T, ":")}
    end;
parse_solr_entry(Schema, delete, {"query", Query}) ->
    Index = Schema:name(),
    case SearchClient:parse_query(Index, binary_to_list(Query)) of
        {ok, QueryOps} ->
            {'query', QueryOps};
        {error, Error} ->
            M = "Error parsing query '~s': ~p~n",
            error_logger:error_msg(M, [Query, Error]),
            throw({?MODULE, could_not_parse_query, Query})
    end;

%% Some unknown command...
parse_solr_entry(_, Command, Entry) ->
    throw({?MODULE, unknown_command, Command, Entry}).


%% @private
to_riak_idx_doc(Schema, Doc) ->
    UniqueKey = Schema:unique_key(),
    case lists:keyfind(UniqueKey, 1, Doc) of
        {UniqueKey, Id} ->
            Id;
        false ->
            Id = undefined, % Prevent compiler warnings.
            throw({?MODULE, required_field_not_found, UniqueKey, Doc})
    end,
    Fields = lists:keydelete(UniqueKey, 1, Doc),
    riak_indexed_doc:new(Id, Fields, [], Schema:name()).


run_solr_command(Schema, Command, Entries) ->
    {ok, IndexPid} = SearchClient:get_index_fsm(),
    {ok, DeletePid} = SearchClient:get_delete_fsm(),
    try 
        run_solr_command(Schema, Command, Entries, IndexPid, DeletePid)
    after
        SearchClient:stop_index_fsm(IndexPid),
        SearchClient:stop_delete_fsm(DeletePid)
    end.

%% Run the provided solr command on the provided docs...
run_solr_command(_, _, [], _, _) ->
    ok;

%% Add a list of documents to the index...
run_solr_command(Schema, add, [IdxDoc|Docs], IndexPid, DeletePid) ->
    %% Delete the terms out of the old document, the idxdoc stored 
    %% under k/v will be updated with the new postings.
    delete_doc_terms(DeletePid, Schema:name(), IdxDoc#riak_idx_doc.id),
    
    %% Store the terms...
    Postings = riak_indexed_doc:postings(IdxDoc),
    SearchClient:index_terms(IndexPid, Postings),

    %% Store the document.
    riak_indexed_doc:put(RiakClient, IdxDoc),
    run_solr_command(Schema, add, Docs, IndexPid, DeletePid);

%% Delete a document by ID...
run_solr_command(Schema, delete, [{'id', Index, ID}|IDs], IndexPid, DeletePid) ->
    delete_doc(DeletePid, Index, ID),
    run_solr_command(Schema, delete, IDs, IndexPid, DeletePid);

%% Delete documents by query...
run_solr_command(Schema, delete, [{'query', QueryOps}|Queries], IndexPid, DeletePid) ->
    Index = Schema:name(),
    {_NumFound, _MaxScore, Docs} = SearchClient:search_doc(Schema:name(), QueryOps, 0, infinity, ?DEFAULT_TIMEOUT),
    [delete_doc(DeletePid, Index, X#riak_idx_doc.id) || X <- Docs, X /= {error, notfound}],
    run_solr_command(Schema, delete, Queries, IndexPid, DeletePid);

%% Unknown command, so error...
run_solr_command(_Schema, Command, _Docs, _IndexPid, _DeletePid) ->
    error_logger:error_msg("Unknown solr command: ~p~n", [Command]),
    throw({unknown_solr_command, Command}).

delete_doc(DeletePid, Index, DocId) ->
    case riak_indexed_doc:get(RiakClient, Index, DocId) of
        {error, notfound} ->
            {error, notfound};
        IdxDoc ->
            SearchClient:delete_doc(DeletePid, IdxDoc),
            ok
    end.

delete_doc_terms(DeletePid, Index, DocId) ->
    case riak_indexed_doc:get(RiakClient, Index, DocId) of
        {error, notfound} ->
            {error, notfound};
        IdxDoc ->
            SearchClient:delete_doc_terms(DeletePid, IdxDoc),
            ok
    end.
