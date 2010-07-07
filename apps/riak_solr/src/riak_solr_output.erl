-module(riak_solr_output).

-include_lib("xmerl/include/xmerl.hrl").
-include_lib("riak_search/include/riak_search.hrl").
-include("riak_solr.hrl").

-export([xml_response/7, json_response/7]).

-import(riak_search_utils, [to_atom/1,
                            to_integer/1,
                            to_binary/1,
                            to_boolean/1,
                            to_float/1]).

-define(XML_PROLOG, {prolog, ["<?xml version=\"1.0\" encoding=\"UTF-8\">"]}).


xml_response(Schema, _SortBy, ElapsedTime, SQuery, NumFound, MaxScore, Docs) ->
    RenderedParams = render_xml_params(NumFound, SQuery),
    RenderedDocs = lists:flatten([render_xml_doc(Schema, Doc) || Doc <- Docs]),
    XML = [xml_nl(),
           {response, [],
            [xml_nl(),
             xml_indent(2), {lst, [{name, "responseHeader"}],
              [xml_nl(),
               xml_indent(4), {int, [{name, "status"}], [#xmlText{value="0"}]},
               xml_nl(),
               xml_indent(4), {int, [{name, "QTime"}], [#xmlText{value=ElapsedTime}]}] ++
                             RenderedParams ++ [xml_nl(), xml_indent(2)]},
             xml_nl(),
             xml_indent(2), {result, [{name, "response"},
                                      {numFound, integer_to_list(NumFound)},
                                      {start, integer_to_list(SQuery#squery.query_start)},
                                      {maxScore, MaxScore}],
                             RenderedDocs ++ [xml_nl(), xml_indent(2)]},
            xml_nl()]}],
    xmerl:export_simple(lists:flatten(XML), xmerl_xml, [?XML_PROLOG]).

json_response(Schema, _SortBy, ElapsedTime, SQuery, NumFound, MaxScore, []) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, to_binary(Schema:default_op())},
                                       {<<"wt">>, <<"json">>}]}}]}},
                 {<<"response">>,
                  {struct, [
                            {<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.query_start},
                            {<<"maxScore">>, list_to_binary(MaxScore)},
                            {<<"docs">>, []}]}}],
    mochijson2:encode({struct, Response});
json_response(Schema, SortBy, ElapsedTime, SQuery, NumFound, MaxScore, Docs0) ->
    F = fun({Name, Value}) ->
        case Schema:find_field(Name) of
            Field when is_record(Field, riak_search_field) ->
                Type = Schema:field_type(Field);
            undefined ->
                Type = unknown
        end,
        convert_type(Value, Type)
    end,
    Docs = riak_solr_sort:sort(Docs0, SortBy),
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, to_binary(Schema:default_op())},
                                       {<<"wt">>, <<"json">>}]}}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.query_start},
                            {<<"maxScore">>, list_to_binary(MaxScore)},
                            {<<"docs">>, [riak_indexed_doc:to_mochijson2(F, Doc) || Doc <- Docs]}]}}],
    mochijson2:encode({struct, Response}).

%% Internal functions
convert_type(FieldValue, unknown) ->
    convert_type(FieldValue, string);
convert_type(FieldValue, string) ->
    to_binary(FieldValue);
convert_type(FieldValue, integer) ->
    to_integer(FieldValue);
convert_type(FieldValue, float) ->
    to_float(FieldValue);
convert_type(FieldValue, boolean) ->
    to_boolean(FieldValue);
convert_type(_FieldValue, Other) ->
    throw({unhandled_type, Other}).

xml_indent(N) ->
    xml_indent(N, false).

xml_nl() ->
    xml_indent(0, true).

xml_indent(0, true) ->
    #xmlText{value="\n"};
xml_indent(0, false) ->
    #xmlText{value=""};
xml_indent(Size, EmitNewLine) ->
    Indent = [32 || _ <- lists:seq(1, Size)],
    Value = if
                EmitNewLine =:= true ->
                    Indent ++ "\n";
                true ->
                    Indent
            end,
    #xmlText{value=Value}.

render_xml_doc(Schema, Doc) ->
    Fields0 = lists:keysort(1, riak_indexed_doc:fields(Doc)),
    Fields = [{"id", riak_indexed_doc:id(Doc)}|Fields0],
    [xml_nl(),
     xml_indent(4), {doc, [],
                     render_xml_fields(Schema, Fields, []) ++
                     [xml_nl(), xml_indent(4)]}].

render_xml_params(NumFound, SQuery) ->
    [xml_nl(),
     xml_indent(4), {lst, [{name, "params"}],
     [xml_nl(),
      xml_indent(6), {str, [{name, "indent"}], [#xmlText{value="on"}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "start"}], [#xmlText{value=integer_to_list(SQuery#squery.query_start)}]},
      xml_nl(),
      xml_indent(6),{str, [{name, "q"}], [#xmlText{value=SQuery#squery.q}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "wt"}], [#xmlText{value="standard"}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "version"}], [#xmlText{value="1.1"}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "rows"}], [#xmlText{value=integer_to_list(NumFound)}]},
      xml_nl(),
      xml_indent(4)]}].

render_xml_fields(_Schema, [], Accum) ->
    lists:flatten(lists:reverse(Accum));
render_xml_fields(Schema, [{"id", Value}|T], Accum) ->
    render_xml_fields(Schema, T, [[xml_nl(),
                                  xml_indent(6), {str, [{name, "id"}], [#xmlText{value=Value}]}]|Accum]);
render_xml_fields(Schema, [{Name, Value}|T], Accum) ->
    Field = Schema:find_field(Name),
    Tag = case Schema:field_type(Field) of
               string ->
                   str;
               integer ->
                   int;
               date ->
                   date;
               _ ->
                   str
           end,
    render_xml_fields(Schema, T, [[xml_nl(),
                                  xml_indent(6), {Tag, [{name, Name}], [#xmlText{value=Value},
                                                                        xml_nl(), xml_indent(6)]}]|Accum]).
