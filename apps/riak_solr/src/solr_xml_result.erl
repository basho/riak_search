-module(solr_xml_result).

-compile([export_all]).

num_found(Ctx) ->
    integer_to_list(dict:fetch(num_found, Ctx)).

start_row(Ctx) ->
    integer_to_list(dict:fetch(start_row, Ctx)).

docs(Ctx) ->
    Docs = dict:fetch(results, Ctx),
    Schema = dict:fetch(schema, Ctx),
    TFun = mustache:compile(solr_xml_doc),
    lists:flatten([render_doc(Doc, Schema, TFun) || Doc <- Docs]).

render_doc(Doc, Schema, TFun) ->
    Fields0 = lists:keysort(1, riak_indexed_doc:fields(Doc)),
    Ctx0 = dict:store(id, riak_indexed_doc:id(Doc), dict:new()),
    Fields1 = [dict:from_list(prep_field(Field, Schema)) || Field <- Fields0],
    Fields2 = [dict:from_list([{name, "id"}, {tag, "str"}, {value, riak_indexed_doc:id(Doc)}])|Fields1],
    Ctx1 = dict:store(fields, Fields2, Ctx0),
    mustache:render(solr_xml_doc, TFun, Ctx1).

prep_field({Name, Value0}, Schema) ->
    Field = Schema:find_field(Name),
    Tag = case Schema:field_type(Field) of
               string ->
                   "str";
               integer ->
                   "int";
               date ->
                   "date";
               _ ->
                   "string"
           end,
    Value = case Tag of
                "str" ->
                    case string:chr(Value0, $\n) > 0 orelse
                        string:chr(Value0, $\r) > 0 of
                        true ->
                            case string:str(Value0, "![CDATA[") == 0 of
                                true ->
                                    lists:flatten(["![CDATA[",
                                                   Value0,
                                                   "]]"]);
                                false ->
                                    Value0
                            end;
                        false ->
                            Value0
                    end;
                _ ->
                    Value0
            end,
    [{name, Name}, {tag, Tag}, {value, Value}].
