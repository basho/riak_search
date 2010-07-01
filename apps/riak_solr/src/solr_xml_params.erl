-module(solr_xml_params).

-include("riak_solr.hrl").

-compile([export_all]).

q(Ctx) ->
    SQuery = dict:fetch(squery, Ctx),
    SQuery#squery.q.

wt() ->
    "standard".
