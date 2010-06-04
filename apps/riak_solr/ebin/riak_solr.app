{application, riak_solr,
 [
  {description, "Riak Search Solr API"},
  {vsn, "0.1"},
  {modules, [
             riak_solr_app,
             riak_solr_config,
             riak_solr_sup,
             riak_solr_indexer_wm,
             riak_solr_searcher_wm,
             riak_solr_xml_xform,
             riak_solr_search_client,
             riak_solr_schema,
             riak_solr_schema_parser,
             solr_search
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  riak_core,
                  webmachine
                 ]},
  {mod, {riak_solr_app, []}},
  {env, []}
 ]}.
