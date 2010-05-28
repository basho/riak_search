-module(riak_solr_search_client, [Client]).
-export([
    parse_solr_xml/2,
    parse_solr_xml/3,
    run_solr_command/2
]).

-define(DEFAULT_INDEX, "search").
-include_lib("riak_search/include/riak_search.hrl").

%% Parse a solr XML formatted file.
parse_solr_xml(IndexOrSchema, Body) when is_binary(Body) ->
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    Result = parse_solr_xml(AnalyzerPid, IndexOrSchema, Body),
    qilr_analyzer:close(AnalyzerPid),
    Result.

%% Index a solr XML formatted file using the provided analyzer pid.
parse_solr_xml(AnalyzerPid, IndexOrSchema, Body) ->
    %% Get the schema...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    Index = Schema:name(),

    %% Parse the xml...
    Commands = riak_solr_xml_xform:xform(Body),
    ok = Schema:validate_commands(Commands),
    {cmd, SolrCmd} = lists:keyfind(cmd, 1, Commands),

    %% Index the docs...
    F = fun(SolrDoc) ->
        IdxDoc = to_riak_idx_doc(Index, SolrDoc),
        {IdxDoc, Client:parse_idx_doc(AnalyzerPid, IdxDoc)}
    end,
    {docs, SolrDocs} = lists:keyfind(docs, 1, Commands),
    ParsedDocs = [F(X) || X <- SolrDocs],
    {ok, SolrCmd, ParsedDocs}.

%% @private
to_riak_idx_doc(Index, Doc0) ->
    Id = dict:fetch("id", Doc0),
    Doc = dict:erase(Id, Doc0),
    Fields = dict:to_list(dict:erase(Id, Doc)),
    #riak_idx_doc{id=Id, index=Index, fields=Fields, props=[]}.


%% Run the provided solr command on the provided docs...
run_solr_command(Command, Docs) when Command == 'add' ->
    %% Store the terms and the document...
    F = fun({IdxDoc, Terms}) ->
        %% Store the terms...
        F1 = fun(X) ->
            {Index, Field, Term, DocID, Props} = X,
            Client:index_term(Index, Field, Term, DocID, Props)
        end,
        plists:map(F1, Terms, {processes, 4}),
        
        %% Store the document.
        Client:store_idx_doc(IdxDoc)
    end,
    [F(X) || X <- Docs];
run_solr_command(Command, _Docs) ->
    error_logger:error_msg("Unknown solr command: ~p~n", [Command]),
    throw({unknown_solr_command, Command}).
