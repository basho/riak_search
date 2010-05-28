-module(solr_search).
-export([
    index_dir/1, 
    index_dir/2
]).

-define(DEFAULT_INDEX, "search").
-include_lib("riak_search/include/riak_search.hrl").

%% Full text index the specified file or directory, which is expected
%% to contain a solr formatted file.
index_dir(Directory) ->
    index_dir(?DEFAULT_INDEX, Directory).

%% Full text index the specified file or directory, which is expected
%% to contain a solr formatted file.
index_dir(IndexOrSchema, Directory) ->
    {ok, SolrClient} = riak_solr_app:local_client(),
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    F = fun(_BaseName, Body) ->
        try
            {ok, Command, Docs} = SolrClient:parse_solr_xml(Schema, Body),
            SolrClient:run_solr_command(Command, Docs)
        catch _ : Error ->
            M = "Could not parse docs '~s'.~n~p~n~p~n",
            error_logger:error_msg(M, [Schema:name(), Error, erlang:get_stacktrace()])
        end
    end,
    riak_search_utils:index_recursive(F, Directory),
    qilr_analyzer:close(AnalyzerPid),
    ok.


