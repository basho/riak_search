{application, riak_search,
 [
  {description, "Riak Search"},
  {vsn, "0.9"},
  {modules, [
             riak_search_app,
             riak_search_sup
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
