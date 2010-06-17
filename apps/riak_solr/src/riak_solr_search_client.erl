-module(riak_solr_search_client, [Client]).
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
    Index = Schema:name(),

    %% Parse the xml...
    {ok, Command, Entries} = riak_solr_xml_xform:xform(Body),

    ParsedDocs = [parse_solr_entry(Index, Command, X) || X <- Entries],
    {ok, Command, ParsedDocs}.


%% @private
%% Parse a document to add...
parse_solr_entry(Index, add, {"doc", Entry}) ->
    IdxDoc = to_riak_idx_doc(Index, Entry),
    {IdxDoc, Client:parse_idx_doc(IdxDoc)};

%% Deletion by ID or Query. If query, then parse...
parse_solr_entry(_Index, delete, {"id", ID}) ->
    {'id', ID};
parse_solr_entry(_Index, delete, {"query", Query}) ->
    case Client:parse_query(Query) of
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
to_riak_idx_doc(Index, Doc) ->
    case lists:keyfind("id", 1, Doc) of
        {"id", Id} ->
            Id;
        false ->
            Id = undefined, % Prevent compiler warnings.
            throw({?MODULE, required_field_not_found, "id", Doc})
    end,
    Fields = lists:keydelete("id", 1, Doc),
    #riak_idx_doc{id=Id, index=Index, fields=Fields, props=[]}.


%% Run the provided solr command on the provided docs...
run_solr_command(_, _, []) ->
    ok;

%% Add a list of documents to the index...
run_solr_command(Schema, add, [{IdxDoc, Terms}|Docs]) ->
    %% Store the terms...
    F = fun(X) ->
        {Index, Field, Term, DocID, Props} = X,
        Client:index_term(Index, Field, Term, DocID, Props)
    end,
    plists:map(F, Terms, {processes, 4}),

    %% Store the document.
    Client:store_idx_doc(IdxDoc),
    run_solr_command(Schema, add, Docs);

%% Delete a document by ID...
run_solr_command(Schema, delete, [{'id', ID}|IDs]) ->
    Index = Schema:name(),
    Client:delete_document(Index, ID),
    run_solr_command(Schema, delete, IDs);

%% Delete documents by query...
run_solr_command(Schema, delete, [{'query', QueryOps}|Queries]) ->
    Index = Schema:name(),
    {_NumFound, Docs} = Client:search_doc(Schema, QueryOps, 0, infinity, ?DEFAULT_TIMEOUT),
    [Client:delete_document(Index, X#riak_idx_doc.id) || X <- Docs],
    run_solr_command(Schema, delete, Queries);

%% Unknown command, so error...
run_solr_command(_Schema, Command, _Docs) ->
    error_logger:error_msg("Unknown solr command: ~p~n", [Command]),
    throw({unknown_solr_command, Command}).
