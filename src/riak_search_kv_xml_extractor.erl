%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_kv_xml_extractor).
-export([extract/3,
        extract_value/3]).
-include("riak_search.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {name_stack=[], fields=[]}).
-import(riak_search_utils, [to_utf8/1]).

extract(RiakObject, DefaultField, _Args) ->
    try
        Values = riak_object:get_values(RiakObject),
        lists:flatten([extract_value(V, DefaultField, _Args) || V <- Values])
    catch
        _:Err ->
            {fail, {cannot_parse_xml, Err}}
    end.

extract_value(Data, _DefaultField, _Args) ->
    Options = [{event_fun, fun sax_cb/3}, {event_state, #state{}}],
    {ok, State, _Rest} = xmerl_sax_parser:stream(Data, Options),
    Fields = lists:reverse(lists:flatten(State#state.fields)),
    [{to_utf8(FieldName), to_utf8(FieldValue)} || {FieldName, FieldValue} <- Fields].

 
%% @private
%% xmerl_sax_parser callback to parse an XML document into search
%% fields.
sax_cb({startElement, _Uri, Name, _QualName, Attrs}, _Location, State) ->
    NewNameStack = [Name | State#state.name_stack],
    AttrFields = case Attrs of
                     [] ->
                         [];
                     _ ->
                         make_attr_fields(make_name(NewNameStack), Attrs, [])
                 end,
    State#state{name_stack=NewNameStack, 
                fields=AttrFields++State#state.fields};
%% End of an element, collapse it into the previous item on the stack...
sax_cb({endElement, _Uri, _Name, _QualName}, _Location, State) ->
    State#state{name_stack=tl(State#state.name_stack)};

%% Got a value, set it to the value of the topmost element in the stack...
sax_cb({characters, Value}, _Location, 
       #state{name_stack=NameStack, fields=Fields}=State) ->
    Field = {make_name(NameStack), Value},
    State#state{fields = [Field|Fields]};

sax_cb(_Event, _Location, State) ->
    State.

make_attr_fields(_BaseName, [], Fields) ->
    Fields;
make_attr_fields(BaseName, [{_Uri, _Prefix, AttrName, Value}|Attrs], Fields) ->
    FieldName = BaseName++[$@ | riak_search_kv_extractor:clean_name(AttrName)],
    Field = {FieldName, Value},
    make_attr_fields(BaseName, Attrs, [Field | Fields]).

%% Make a name from a stack of visted tags (innermost tag at head of list)
make_name(NameStack) ->
    make_name(NameStack, "").

make_name([], Name) ->
    riak_search_kv_extractor:clean_name(Name);
make_name([Tag|Rest],"") ->
    make_name(Rest, Tag);
make_name([Tag|Rest],Name) ->
    make_name(Rest, Tag ++ [$_ | Name]).
    
-ifdef(TEST).

not_xml_test() ->
    Data = <<"{\"surprise\":\"this is not XML\"}">>, 
    Object = riak_object:new(<<"b">>, <<"k">>, Data, "application/xml"),
    ?assertMatch({fail, {cannot_parse_xml,_}}, extract(Object, <<"value">>, undefined)).
    

xml_test() ->
    Tests = [{<<"<tag>hi world</tag>">>, [{<<"tag">>, <<"hi world">>}]},
             {<<"<t1><t2>two</t2>one</t1>">>, [{<<"t1_t2">>, <<"two">>},
                                               {<<"t1">>, <<"one">>}]},
             {<<"<t1><t2a>a</t2a><t2b>b</t2b></t1>">>,
              [{<<"t1_t2a">>, <<"a">>},
               {<<"t1_t2b">>, <<"b">>}]},
             {<<"<t1>abc<t2>two</t2>def</t1>">>, [{<<"t1">>, <<"abc">>},
                                                  {<<"t1_t2">>, <<"two">>},
                                                  {<<"t1">>, <<"def">>}]},
             {<<"<tag attr1=\"abc\" attr2='def'/>">>,
              [{<<"tag@attr1">>,<<"abc">>},
               {<<"tag@attr2">>,<<"def">>}]},

             %% Check namespaces are stripped
             {<<"<root>
<h:table xmlns:h=\"http://www.w3.org/TR/html4/\">
  <h:tr>
    <h:td>Apples</h:td>
    <h:td>Bananas</h:td>
  </h:tr>
</h:table>

<f:table xmlns:f=\"http://www.w3schools.com/furniture\">
  <f:name>African Coffee Table</f:name>
  <f:width>80</f:width>
  <f:length>120</f:length>
</f:table>

</root>">>,
              [{<<"root_table_tr_td">>, <<"Apples">>},
               {<<"root_table_tr_td">>, <<"Bananas">>},
               {<<"root_table_name">>, <<"African Coffee Table">>},
               {<<"root_table_width">>, <<"80">>},
               {<<"root_table_length">>, <<"120">>}]},

             %% Check any dots/colons are removed from fields
             {<<"<a.b>cde</a.b>">>, [{<<"a_b">>, <<"cde">>}]}
            ],
    check_expected(Tests).

check_expected([]) ->
    ok;
check_expected([{Xml, Fields}|Rest]) ->
    Object = riak_object:new(<<"b">>, <<"k">>, 
                             <<"<?xml version=\"1.0\"?>", Xml/binary>>,
                             "application/xml"),
    ?assertEqual(Fields, extract(Object, <<"value">>, undefined)),
    check_expected(Rest).

-endif. % TEST
