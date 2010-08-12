%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_kv_extractor).
-export([extract/2]).

%% Initial implementation - get the whole object as the default field
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
