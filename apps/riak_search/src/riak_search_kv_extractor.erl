%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_kv_extractor).
-export([extract/2, clean_name/1]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Extract search data from the riak_object.  Switch between the
%% built-in extractors based on Content-Type.
extract(RiakObject, Args) ->
    ContentType =  dict:fetch(<<"content-type">>, 
                              riak_object:get_metadata(RiakObject)),
    Extractor = get_extractor(ContentType, encodings()),
    Extractor:extract(RiakObject, Args).

%% Get the encoding from the content type
get_extractor(_, []) ->
    riak_search_kv_raw_extractor;
get_extractor(CT, [{Encoding, Types} | Rest]) ->
    case lists:member(CT, Types) of
        true ->
            Encoding;
        false ->
            get_extractor(CT, Rest)
    end.
    
encodings() ->
    [{riak_search_kv_xml_extractor,  ["application/xml",
                                      "text/xml"]},
     {riak_search_kv_json_extractor, ["application/json",
                                      "application/x-javascript",
                                      "text/javascript",
                                      "text/x-javascript",
                                      "text/x-json"]}].

%% Substitute : and . for _
clean_name(Name) ->
    clean_name(Name, "").
    
clean_name([], RevName) ->
    lists:reverse(RevName);
clean_name([C | Rest], RevName) when C =:= $.; C =:= $: ->
    clean_name(Rest, [$_ | RevName]);
clean_name([C | Rest], RevName) ->
    clean_name(Rest, [C | RevName]).

-ifdef(TEST).

extractor_test() ->
    JsonData = <<"{\"one\":{\"two\":{\"three\":\"go\"}}}">>,
    JsonFields = [{"one_two_three", <<"go">>}],
    XmlData = <<"<?xml version=\"1.0\"?><t1>abc<t2>two</t2>def</t1>">>, 
    XmlFields = [{"t1", <<"abc">>},
                 {"t1_t2", <<"two">>},
                 {"t1", <<"def">>}],
    PlainData = <<"the quick brown fox">>,
    PlainFields = [{"value", <<"the quick brown fox">>}],

    Tests = [{JsonData, "application/json", JsonFields},
             {JsonData, "application/x-javascript", JsonFields},
             {JsonData, "text/javascript", JsonFields},
             {JsonData, "text/x-javascript", JsonFields},
             {JsonData, "text/x-json", JsonFields},
             {XmlData,  "application/xml", XmlFields},
             {XmlData,  "text/xml", XmlFields},
             {PlainData,"text/plain", PlainFields}],
    check_expected(Tests).

check_expected([]) ->
    ok;
check_expected([{Data, CT, Fields}|Rest]) ->
    Object = riak_object:new(<<"b">>, <<"k">>, Data, CT),
    ?assertEqual(Fields, extract(Object, undefined)),
    check_expected(Rest).

-endif. % TEST
