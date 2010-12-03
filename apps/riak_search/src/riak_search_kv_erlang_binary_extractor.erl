%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_search_kv_erlang_binary_extractor).
-export([extract/3,
         extract_value/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

extract(RiakObject, DefaultField, _Args) ->
    try
        Values = riak_object:get_values(RiakObject),
        lists:flatten([extract_value(V, DefaultField, _Args) || V <- Values])
    catch
        _:Err ->
            {fail, {bad_erlang,Err}}
    end.

extract_value(Data, DefaultField, _Args) ->
    riak_search_kv_erlang_extractor:extract_value(
      binary_to_term(Data), DefaultField, _Args).

-ifdef(TEST).

bad_binary_test() ->
    Data = <<"this is not a binaried term">>,
    Object = riak_object:new(<<"b">>, <<"k">>, Data,
                             "application/x-erlang-binary"),
    ?assertMatch({fail, {bad_erlang,_}}, extract(Object, <<"value">>, undefined)).
             
term_test() ->
    Tests = [{[{<<"myfield">>,<<"myvalue">>}],
              [{<<"myfield">>,<<"myvalue">>}]},

             {[{<<"myfield">>,123}],
              [{<<"myfield">>,<<"123">>}]},

             {[{<<"myfield">>,123.456}],
              [{<<"myfield">>,<<"123.456">>}]},

             {[{<<"myfield">>,true}],
              [{<<"myfield">>,<<"true">>}]},

             {[{<<"myfield">>,undefined}],
              []},

             {[{<<"one">>,[{<<"two">>,[{<<"three">>,<<"go">>}]}]}],
              [{<<"one_two_three">>, <<"go">>}]},

             {<<"abc">>,
              [{<<"value">>, <<"abc">>}]},

             {[{<<"menu">>,
                [{<<"id">>,<<"file">>},
                 {<<"value">>,<<"File">>},
                 {<<"popup">>,
                  [{<<"menuitem">>,
                    [[{<<"value">>,<<"New">>},
                      {<<"onclick">>,<<"CreateNewDoc()">>}],
                     [{<<"value">>,<<"Open">>},
                      {<<"onclick">>,<<"OpenDoc()">>}],
                     [{<<"value">>,<<"Close">>},
                      {<<"onclick">>, <<"CloseDoc()">>}]]
                   }]
                 }]
               }],
              [{<<"menu_id">>, <<"file">>},
               {<<"menu_value">>, <<"File">>},
               {<<"menu_popup_menuitem_value">>, <<"New">>},
               {<<"menu_popup_menuitem_onclick">>, <<"CreateNewDoc()">>},
               {<<"menu_popup_menuitem_value">>, <<"Open">>},
               {<<"menu_popup_menuitem_onclick">>, <<"OpenDoc()">>},
               {<<"menu_popup_menuitem_value">>, <<"Close">>},
               {<<"menu_popup_menuitem_onclick">>, <<"CloseDoc()">>}]},
             %% From http://www.ibm.com/developerworks/library/x-atom2json.html
              %% via riak_search_kv_json_extractor.erl
             {[{<<"lang">>,<<"en-US">>},
               {<<"dir">>,<<"ltr">>},
               {<<"id">>,<<"tag:example.org,2007:/foo">>},
               {<<"title">>,<<"Example Feed">>},
               {<<"subtitle">>,
                [{<<"attributes">>,
                  [{<<"type">>,<<"html">>}]},
                 {<<"children">>,
                  [{<<"name">>,<<"p">>},
                   {<<"attributes">>,[]},
                   {<<"children">>,[<<"This is an example feed">>] }]}]},
               {<<"rights">>,
                [{<<"attributes">>,
                  [{<<"type">>,<<"xhtml">>}]},
                 {<<"children">>,
                  [{<<"name">>,<<"p">>},
                   {<<"attributes">>,[]},
                   {<<"children">>,[<<"Copyright © James M Snell">>]}]}]},
               {<<"updated">>,<<"2007-10-14T12:12:12.000Z">>},
               {<<"authors">>,
                [{<<"name">>,<<"James M Snell">>},
                 {<<"email">>,<<"jasnell@example.org">>},
                 {<<"uri">>,<<"http://example.org/~jasnell">>}]},
               {<<"links">>,
                [[{<<"href">>,<<"http://example.org/foo">>},
                  {<<"rel">>,<<"self">>}],
                 [{<<"href">>,<<"http://example.org/blog">>}],
                 [{<<"href">>,<<"http://example.org/blog;json">>},
                  {<<"rel">>,<<"alternate">>},
                  {<<"type">>,<<"application/json">>}]]},
               {<<"entries">>,[]},
               {<<"attributes">>,
                [{<<"xml:lang">>,<<"en-US">>},
                 {<<"xml:base">>,<<"http://example.org/foo">>}]}],
              [{<<"lang">>,<<"en-US">>}, 
               {<<"dir">>,<<"ltr">>}, 
               {<<"id">>,<<"tag:example.org,2007:/foo">>}, 
               {<<"title">>,<<"Example Feed">>}, 
               {<<"subtitle_attributes_type">>, <<"html">>},
               {<<"subtitle_children_name">>, <<"p">>}, 
               {<<"subtitle_children_children">>, <<"This is an example feed">>},
               {<<"rights_attributes_type">>, <<"xhtml">> }, 
               {<<"rights_children_name">>,<<"p">>}, 
               {<<"rights_children_children">>,<<"Copyright © James M Snell" >>},
               {<<"updated">>,<<"2007-10-14T12:12:12.000Z">>}, 
               {<<"authors_name">>,<<"James M Snell">>}, 
               {<<"authors_email">>,<<"jasnell@example.org">>}, 
               {<<"authors_uri">>,<<"http://example.org/~jasnell">>},
               {<<"links_href">>,<<"http://example.org/foo">>}, 
               {<<"links_rel">>,<<"self">>},
               {<<"links_href">>,<<"http://example.org/blog">>}, 
               {<<"links_href">>,<<"http://example.org/blog;json">>}, 
               {<<"links_rel">>,<<"alternate">>},
               {<<"links_type">>,<<"application/json">>},
               {<<"attributes_xml_lang">>, <<"en-US">>},
               {<<"attributes_xml_base">>, <<"http://example.org/foo">>}]}
            ],
    check_expected(Tests).

check_expected([]) ->
    ok;
check_expected([{Terms, Fields}|Rest]) ->
    Object = riak_object:new(<<"b">>, <<"k">>,
                             term_to_binary(Terms),
                             "application/x-erlang-binary"),
    ?assertEqual(Fields, extract(Object, <<"value">>, undefined)),
    check_expected(Rest).

-endif. % TEST
