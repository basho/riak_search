-record(riak_solr_schemadef, {name,
                              version,
                              fields=dict:new(),
                              dynamic_fields=dict:new()}).

-record(riak_solr_field, {name,
                          type,
                          required}).
