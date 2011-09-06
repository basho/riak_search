%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_output).

-include_lib("xmerl/include/xmerl.hrl").
-include("riak_search.hrl").
-include("riak_solr.hrl").

-export([xml_response/8, xml_response_ids_only/6,
         json_response/8, json_response_ids_only/6]).

-import(riak_search_utils, [to_atom/1,
                            to_integer/1,
                            to_binary/1,
                            to_boolean/1,
                            to_float/1]).

-define(XML_PROLOG, {prolog, ["<?xml version=\"1.0\" encoding=\"UTF-8\"?>"]}).


xml_response(Schema, SortBy, ElapsedTime, SQuery, NumFound, MaxScore, Docs0, FL) ->
    Docs = riak_solr_sort:sort(Docs0, SortBy, Schema),

    RenderedParams = render_xml_params(NumFound, Schema, SQuery),
    RenderedDocs = lists:flatten([render_xml_doc(Schema, Doc, FL) || Doc <- Docs]),
    XML = [xml_nl(),
           {response, [],
            [xml_nl(),
             xml_indent(2), {lst, [{name, "responseHeader"}],
              [xml_nl(),
               xml_indent(4), {int, [{name, "status"}], [#xmlText{value="0"}]},
               xml_nl(),
               xml_indent(4), {int, [{name, "QTime"}], [#xmlText{value=integer_to_list(ElapsedTime)}]}] ++
                             RenderedParams ++ [xml_nl(), xml_indent(2)]},
             xml_nl(),
             xml_indent(2), {result, [{name, "response"},
                                      {numFound, integer_to_list(NumFound)},
                                      {start, integer_to_list(SQuery#squery.query_start)},
                                      {maxScore, MaxScore}],
                             RenderedDocs ++ [xml_nl(), xml_indent(2)]},
            xml_nl()]}],
    xmerl:export_simple(lists:flatten(XML), xmerl_xml, [?XML_PROLOG]).

xml_response_ids_only(Schema, ElapsedTime, SQuery, NumFound, MaxScore, DocIDs) ->
    RenderedParams = render_xml_params(NumFound, Schema, SQuery),
    RenderedDocs = lists:flatten([
                    [xml_nl(),
                     xml_indent(4), 
                     {doc, [],               
                      [xml_nl(), 
                       xml_indent(6), 
                       {str, [{name, "id"}], 
                        [#xmlText{value=DocID}, xml_nl(), xml_indent(6)]},
                       xml_nl(),
                       xml_indent(4)]
                     }]
                    || DocID <- DocIDs ]),
    
    XML = [xml_nl(),
           {response, [],
            [xml_nl(),
             xml_indent(2), {lst, [{name, "responseHeader"}],
              [xml_nl(),
               xml_indent(4), {int, [{name, "status"}], [#xmlText{value="0"}]},
               xml_nl(),
               xml_indent(4), {int, [{name, "QTime"}], [#xmlText{value=integer_to_list(ElapsedTime)}]}] ++
                             RenderedParams ++ [xml_nl(), xml_indent(2)]},
             xml_nl(),
             xml_indent(2), {result, [{name, "response"},
                                      {numFound, integer_to_list(NumFound)},
                                      {start, integer_to_list(SQuery#squery.query_start)},
                                      {maxScore, MaxScore}],
                             RenderedDocs ++ [xml_nl(), xml_indent(2)]},
            xml_nl()]}],
    xmerl:export_simple(lists:flatten(XML), xmerl_xml, [?XML_PROLOG]).


json_response(Schema, _SortBy, ElapsedTime, SQuery, NumFound, MaxScore, [], _Fields) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, to_binary(Schema:default_op())},
                                       {<<"filter">>, to_binary(SQuery#squery.filter)},
                                       {<<"wt">>, <<"json">>}]}}]}},
                 {<<"response">>,
                  {struct, [
                            {<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.query_start},
                            {<<"maxScore">>, list_to_binary(MaxScore)},
                            {<<"docs">>, []}]}}],
    mochijson2:encode({struct, Response});
json_response(Schema, SortBy, ElapsedTime, SQuery, NumFound, MaxScore, Docs0, Fields) ->
    F = fun({Name, Value}) ->
        case Schema:find_field(Name) of
            Field when is_record(Field, riak_search_field) ->
                Type = Schema:field_type(Field);
            undefined ->
                Type = unknown
        end,
        convert_type(Value, Type)
    end,
    Docs = riak_solr_sort:sort(Docs0, SortBy, Schema),
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, to_binary(Schema:default_op())},
                                       {<<"filter">>, to_binary(SQuery#squery.filter)},
                                       {<<"df">>, to_binary(Schema:default_field())},
                                       {<<"wt">>, <<"json">>},
                                       {<<"version">>, <<"1.1">>},
                                       {<<"rows">>, NumFound}]}}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.query_start},
                            {<<"maxScore">>, list_to_binary(MaxScore)},
                            {<<"docs">>, [riak_indexed_doc:to_mochijson2(F, Doc, Fields) || Doc <- Docs]}]}}],
    mochijson2:encode({struct, Response}).

json_response_ids_only(Schema, ElapsedTime, SQuery, NumFound, MaxScore, DocIDs) ->
    Response = [{<<"responseHeader">>,
                 {struct, [{<<"status">>, 0},
                           {<<"QTime">>, ElapsedTime},
                           {<<"params">>,
                             {struct, [{<<"q">>, to_binary(SQuery#squery.q)},
                                       {<<"q.op">>, to_binary(Schema:default_op())},
                                       {<<"filter">>, to_binary(SQuery#squery.filter)},
                                       {<<"df">>, to_binary(Schema:default_field())},
                                       {<<"wt">>, <<"json">>},
                                       {<<"version">>, <<"1.1">>},
                                       {<<"rows">>, NumFound}]}}]}},
                 {<<"response">>,
                  {struct, [{<<"numFound">>, NumFound},
                            {<<"start">>, SQuery#squery.query_start},
                            {<<"maxScore">>, list_to_binary(MaxScore)},
                            {<<"docs">>, [{struct, [{id, Id}]} || Id <- DocIDs]}]}}],
    mochijson2:encode({struct, Response}).

%% Internal functions
convert_type(FieldValue, unknown) ->
    convert_type(FieldValue, string);
convert_type(FieldValue, date) ->
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

render_xml_doc(Schema, Doc, FL) ->
    Fields0 = lists:keysort(1, riak_indexed_doc:fields(Doc)),
    UniqueKey = Schema:unique_key(),
    Fields = [{UniqueKey, riak_indexed_doc:id(Doc)}|Fields0],
    [xml_nl(),
     xml_indent(4), {doc, [],
                     render_xml_fields(Schema, Fields, FL, []) ++
                     [xml_nl(), xml_indent(4)]}].

render_xml_params(NumFound, Schema, SQuery) ->
    [xml_nl(),
     xml_indent(4), {lst, [{name, "params"}],
     [xml_nl(),
      xml_indent(6), {str, [{name, "indent"}], [#xmlText{value="on"}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "start"}], [#xmlText{value=integer_to_list(SQuery#squery.query_start)}]},
      xml_nl(),
      xml_indent(6),{str, [{name, "q"}], [#xmlText{value=SQuery#squery.q}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "q.op"}], [#xmlText{value=atom_to_list(Schema:default_op())}]},
      xml_nl(),
      xml_indent(6),{str, [{name, "filter"}], [#xmlText{value=SQuery#squery.filter}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "df"}], [#xmlText{value=Schema:default_field()}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "wt"}], [#xmlText{value="standard"}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "version"}], [#xmlText{value="1.1"}]},
      xml_nl(),
      xml_indent(6), {str, [{name, "rows"}], [#xmlText{value=integer_to_list(NumFound)}]},
      xml_nl(),
      xml_indent(4)]}].


render_xml_fields(_Schema, [], FL, Accum) ->
    lists:flatten(lists:reverse(Accum));    
render_xml_fields(Schema, [{Name, Value}|T], FL, Accum) when is_list(FL) ->
    Show = lists:member(Name, FL),
    if
        Show ->
            Field = Schema:find_field(Name),
            render_xml_fields(Schema, T, FL, [render_xml_field(Schema, Field, Name, Value)|Accum]);
        true ->
            render_xml_fields(Schema, T, FL, Accum)
    end;
render_xml_fields(Schema, [{Name, Value}|T], FL, Accum) -> 
    Field = Schema:find_field(Name),
    render_xml_fields(Schema, T, FL, [render_xml_field(Schema, Field, Name, Value)|Accum]).

render_xml_field(Schema, Field, Name, Value) ->
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
    [xml_nl(),
     xml_indent(6), {Tag, [{name, Name}], [#xmlText{value=Value},
                                           xml_nl(), xml_indent(6)]}].
