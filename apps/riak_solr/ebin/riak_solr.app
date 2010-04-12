{application, riak_solr,
 [
  {description, "Riak Search Solr API"},
  {vsn, "0.1"},
  {modules, [
             riak_solr_app,
             riak_solr_sup,
             riak_solr_wm,
             riak_solr_config,
             riak_solr_schema
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
