%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(solr_search).
-export([
    index_dir/1,
    index_dir/2
]).

-include("riak_search.hrl").

%% Full text index the specified file or directory, which is expected
%% to contain a solr formatted file.
index_dir(Directory) ->
    index_dir(?DEFAULT_INDEX, Directory).

%% Full text index the specified file or directory, which is expected
%% to contain a solr formatted file.
index_dir(Index, Directory) ->
    {ok, SolrClient} = riak_search:local_solr_client(),
    Fun = fun(Schema, Files) ->
                  F = fun(File) ->
                              {ok, Body} = file:read_file(File),
                              {ok, Command, Docs} = SolrClient:parse_solr_xml(Schema, Body),
                              SolrClient:run_solr_command(Schema, Command, Docs)
                  end,
                  [F(X) || X <- Files]
          end,
    riak_search_dir_indexer:index(Index, Directory, Fun).
