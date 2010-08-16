%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_kv_json_extractor).
-export([extract/2,
         extract_value/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_FIELD, "value").

extract(RiakObject, _Args) ->
    try
        Values = riak_object:get_values(RiakObject),
        lists:flatten([extract_value(V, _Args) || V <- Values])
    catch
        _:Err ->
            {fail, {cannot_parse_json,Err}}
    end.

extract_value(Data, _Args) ->
    Json = mochijson2:decode(Data),    
    lists:reverse(lists:flatten(make_search_fields(undefined, Json, []))).


make_search_fields(NamePrefix, {struct, Fields}, Output) ->
    F = fun({Name, Val}, Acc) ->
                Name2 = append_fieldname(NamePrefix, Name),
                [make_search_fields(Name2, Val, []) | Acc]
        end,
    lists:foldl(F, Output, Fields);
make_search_fields(Name, List, Output) when is_list(List) ->
    F = fun(El, Acc) ->
                [make_search_fields(Name, El, []) | Acc]
        end,
    lists:foldl(F, Output, List);
make_search_fields(_Name, null, Output) ->
    Output;
make_search_fields(Name, Term, Output) ->
    [{search_fieldname(Name), to_data(Term)} | Output].

to_data(String) when is_binary(String) ->
    String;
to_data(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_data(Term) ->
    iolist_to_binary(io_lib:format("~p", [Term])).
    

append_fieldname(undefined, Name) ->
    binary_to_list(Name);
append_fieldname(FieldPrefix, Name) ->
    FieldPrefix ++ [$_ | binary_to_list(Name)].
    
%% Make a search field name - if no names encountered yet use the
%% default field, otherwise make sure the field name does not
%% contain . or : by substituting with _
search_fieldname(undefined) ->
    ?DEFAULT_FIELD;
search_fieldname(Name) ->
    riak_search_kv_extractor:clean_name(Name).



-ifdef(TEST).

bad_json_test() ->
    Data = <<"<?xml><suprise>this is not json</suprise>">>, 
    Object = riak_object:new(<<"b">>, <<"k">>, Data, "application/json"),
    ?assertMatch({fail, {cannot_parse_json,_}}, extract(Object, undefined)).
             
json_test() ->
    Tests = [{<<"{\"myfield\":\"myvalue\"}">>,
              [{"myfield",<<"myvalue">>}]},

             {<<"{\"myfield\":123}">>,
              [{"myfield",<<"123">>}]},

             {<<"{\"myfield\":123.456}">>,
              [{"myfield",<<"123.456">>}]},

             {<<"{\"myfield\":true}">>,
              [{"myfield",<<"true">>}]},

             {<<"{\"myfield\":null}">>, []},

             {<<"{\"one\":{\"two\":{\"three\":\"go\"}}}">>,
              [{"one_two_three", <<"go">>}]},

             {<<"[\"abc\"]">>,
              [{"value", <<"abc">>}]},

             {<<"
{\"menu\": {
  \"id\": \"file\",
  \"value\": \"File\",
  \"popup\": {
    \"menuitem\": [
      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},
      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},
      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}
    ]
  }
}}">>,
              [{"menu_id", <<"file">>},
               {"menu_value", <<"File">>},
               {"menu_popup_menuitem_value", <<"New">>},
               {"menu_popup_menuitem_onclick", <<"CreateNewDoc()">>},
               {"menu_popup_menuitem_value", <<"Open">>},
               {"menu_popup_menuitem_onclick", <<"OpenDoc()">>},
               {"menu_popup_menuitem_value", <<"Close">>},
               {"menu_popup_menuitem_onclick", <<"CloseDoc()">>}]},
             %% From http://www.ibm.com/developerworks/library/x-atom2json.html
             {<<"{ 
 \"lang\":\"en-US\", 
 \"dir\":\"ltr\", 
 \"id\":\"tag:example.org,2007:/foo\", 
 \"title\":\"Example Feed\", 
 \"subtitle\":{ 
  \"attributes\":{ 
   \"type\":\"html\" 
  }, 
  \"children\":[{ 
    \"name\":\"p\", 
    \"attributes\":{ }, 
    \"children\":[\"This is an example feed\" ] 
   } ] }, 
 \"rights\":{ 
  \"attributes\":{ \"type\":\"xhtml\" }, 
  \"children\":[{ 
    \"name\":\"p\", 
    \"attributes\":{ }, 
    \"children\":[\"Copyright \\u00a9 James M Snell\" ] 
   } ]}, 
 \"updated\":\"2007-10-14T12:12:12.000Z\", 
 \"authors\":[{ 
   \"name\":\"James M Snell\", 
   \"email\":\"jasnell@example.org\", 
   \"uri\":\"http://example.org/~jasnell\" 
  } ], 
 \"links\":[
   { 
     \"href\":\"http://example.org/foo\", 
     \"rel\":\"self\" 
   },
   { 
     \"href\":\"http://example.org/blog\" 
   },
   { 
     \"href\":\"http://example.org/blog;json\", 
     \"rel\":\"alternate\", 
     \"type\":\"application/json\" 
   } ], 
 \"entries\":[],
 \"attributes\":{ 
  \"xml:lang\":\"en-US\", 
  \"xml:base\":\"http://example.org/foo\" 
 }
}">>,
              [{"lang",<<"en-US">>}, 
               {"dir",<<"ltr">>}, 
               {"id",<<"tag:example.org,2007:/foo">>}, 
               {"title",<<"Example Feed">>}, 
               {"subtitle_attributes_type", <<"html">>},
               {"subtitle_children_name", <<"p">>}, 
               {"subtitle_children_children", <<"This is an example feed">>},
               {"rights_attributes_type", <<"xhtml">> }, 
               {"rights_children_name",<<"p">>}, 
               {"rights_children_children",<<"Copyright © James M Snell" >>},
               {"updated",<<"2007-10-14T12:12:12.000Z">>}, 
               {"authors_name",<<"James M Snell">>}, 
               {"authors_email",<<"jasnell@example.org">>}, 
               {"authors_uri",<<"http://example.org/~jasnell">>},
               {"links_href",<<"http://example.org/foo">>}, 
               {"links_rel",<<"self">>},
               {"links_href",<<"http://example.org/blog">>}, 
               {"links_href",<<"http://example.org/blog;json">>}, 
               {"links_rel",<<"alternate">>},
               {"links_type",<<"application/json">>},
               {"attributes_xml_lang", <<"en-US">>},
               {"attributes_xml_base", <<"http://example.org/foo">>}]}
            ],
    check_expected(Tests).

check_expected([]) ->
    ok;
check_expected([{Json, Fields}|Rest]) ->
    Object = riak_object:new(<<"b">>, <<"k">>, Json, "application/json"),
    ?assertEqual(Fields, extract(Object, undefined)),
    check_expected(Rest).

-endif. % TEST
