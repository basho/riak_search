{application, riak_search,
 [
  {description, "Riak Search"},
  {vsn, "0.9"},
  {modules, [
             riak_search_app,
             riak_search_sup,
             riak_search_preplan,
             riak_search_query,
             riak_search_op,
             riak_search_op_land,
             riak_search_op_lor,
             riak_search_op_lnot,
             riak_search_op_term,
             test
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  riak_core
                 ]},
  {mod, { riak_search_app, []}},
  {env, []}
 ]}.
