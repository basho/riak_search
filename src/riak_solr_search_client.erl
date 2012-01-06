%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_search_client, [RiakClient, SearchClient]).
-export([
         run_solr_command/3
]).

-define(DEFAULT_TIMEOUT, 60000).
-include("riak_search.hrl").

%% Run the provided solr command on the provided docs...
run_solr_command(_, _, []) ->
    ok;

%% Add a list of documents to the index...
run_solr_command(_Schema, add, Docs) ->
    %% Delete the terms out of the old document, the idxdoc stored 
    %% under k/v will be updated with the new postings.
    SearchClient:index_docs(Docs),
    ok;

%% Delete a document by ID...
run_solr_command(Schema, delete, Docs) ->
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
run_solr_command(_Schema, Command, _Docs) ->
    lager:error("Unknown solr command: ~p", [Command]),
    throw({unknown_solr_command, Command}).
