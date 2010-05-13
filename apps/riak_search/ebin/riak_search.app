{application, riak_search,
 [
  {description, "Riak Search"},
  {vsn, "0.9"},
  {modules, [
             search,
             plists,
             raptor_index_backend,
             riak_indexed_doc,
             riak_search,
             riak_search_app,
             riak_search_client,
             riak_search_sup,
             riak_search_preplan,
             riak_search_facets,
             riak_search_op,
             riak_search_op_land,
             riak_search_op_lor,
             riak_search_op_lnot,
             riak_search_op_term,
             riak_search_op_field,
             riak_search_op_inclusive_range,
             riak_search_op_exclusive_range,
             riak_search_op_proximity,
             riak_search_op_node,
             riak_search_phase,
             riak_search_test,
             riak_search_utils,
             merge_index_backend
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
