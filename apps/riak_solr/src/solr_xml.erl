-module(solr_xml).

-compile([export_all]).

status() ->
    "0".

qtime(Ctx) ->
    integer_to_list(dict:fetch(qtime, Ctx)).

params(Ctx) ->
    TFun = mustache:compile(solr_xml_params),
    mustache:render(solr_xml_params, TFun, Ctx).

result(Ctx) ->
    TFun = mustache:compile(solr_xml_result),
    mustache:render(solr_xml_result, TFun, Ctx).
