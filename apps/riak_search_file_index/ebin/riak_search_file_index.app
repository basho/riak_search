{application, riak_search_file_index,
 [
  {description, "Riak Search File Index"},
  {vsn, "0.1"},
  {modules, [
             riak_search_file_index_app,
             riak_search_file_index_sup,
             riak_search_file_index,
             riak_search_file_index_merge,
             riak_search_file_index_stream,
             riak_search_file_index_test, 
             sync
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { riak_search_file_index_app, []}},
  {env, []}
 ]}.
