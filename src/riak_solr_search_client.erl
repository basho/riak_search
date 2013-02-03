%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_search_client).
-export([new/2,
         parse_solr_xml/3,
         run_solr_command/4
]).

-import(riak_search_utils, [to_list/1, to_binary/1]).


-include("riak_search.hrl").

new(RiakClient, SearchClient) ->
    {?MODULE, [RiakClient, SearchClient]}.

%% Parse a solr XML formatted file.
parse_solr_xml(IndexOrSchema, Body, {?MODULE, [_RiakClient, _SearchClient]}=THIS) when is_binary(Body) ->
    %% Get the schema...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),

    %% Parse the xml...
    {ok, Command, Entries} = riak_solr_xml_xform:xform(Body),

    %% TESTING - This is a faster parser, but doesn't handle deletes
    %% yet, and doesn't do any validation.  {ok, Command, Entries} =
    %% riak_solr_xml:parse(Body),

    ParsedDocs = [parse_solr_entry(Schema, Command, X, THIS) || X <- Entries],
    {ok, Command, ParsedDocs}.

%% @private
%% Parse a document to add...
parse_solr_entry(Schema, add, {<<"doc">>, Entry}, {?MODULE, [_RiakClient, _SearchClient]}) ->
    IdxDoc0 = to_riak_idx_doc(Schema, Entry),
    IdxDoc = riak_indexed_doc:analyze(IdxDoc0),
    IdxDoc;

%% Deletion by ID or Query. If query, then parse...
parse_solr_entry(Schema, delete, {<<"id">>, ID}, {?MODULE, [_RiakClient, _SearchClient]}) ->
    case string:tokens(to_list(ID), ":") of
        [] ->
            throw({?MODULE, empty_id_on_solr_delete});
        [H] -> 
            {'id', Schema:name(), to_binary(H)};
        [H|T] -> 
            {'id', to_binary(H), to_binary(string:join(T, ":"))}
    end;
parse_solr_entry(Schema, delete, {<<"query">>, Query}, {?MODULE, [_RiakClient, SearchClient]}) ->
    Index = Schema:name(),
    case SearchClient:parse_query(Index, binary_to_list(Query)) of
        {ok, QueryOps} ->
            {'query', QueryOps};
        {error, Error} ->
            lager:error("Error parsing query '~s': ~p", [Query, Error]),
            throw({?MODULE, could_not_parse_query, Query})
    end;

%% Some unknown command...
parse_solr_entry(_, Command, Entry, _) ->
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
    riak_indexed_doc:new(Schema:name(), Id, Fields, []).


%% Run the provided solr command on the provided docs...
run_solr_command(_, _, [], _) ->
    ok;

%% Add a list of documents to the index...
run_solr_command(_Schema, add, Docs, {?MODULE, [_RiakClient, SearchClient]}) ->
    %% Delete the terms out of the old document, the idxdoc stored 
    %% under k/v will be updated with the new postings.
    SearchClient:index_docs(Docs),
    ok;

%% Delete a document by ID...
run_solr_command(Schema, delete, Docs, {?MODULE, [_RiakClient, SearchClient]}) ->
    F = fun({'id', Index, ID}, Acc) ->
                [{Index, ID}|Acc];
           ({'query', QueryOps}, Acc) ->
                {_, Results} = SearchClient:search(Schema, QueryOps, [], 0, infinity, ?DEFAULT_TIMEOUT),
                L = [{DocIndex, DocID} || {DocIndex, DocID, _} <- Results],
                L ++ Acc
        end,
    Docs1 = lists:foldl(F, [], Docs),
    SearchClient:delete_docs(Docs1),
    ok;

%% Unknown command, so error...
run_solr_command(_Schema, Command, _Docs, _THIS) ->
    lager:error("Unknown solr command: ~p", [Command]),
    throw({unknown_solr_command, Command}).
