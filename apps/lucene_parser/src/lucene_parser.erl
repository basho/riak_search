%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(lucene_parser).
-export([parse/3]).
-include("lucene_parser.hrl").

%% @doc Parse a given query string into a query graph. The query graph
%% consists of a nested set of Erlang records found in lucene_parser.hrl.
parse(DefaultIndex, DefaultField, QueryString) when is_list(DefaultIndex) andalso is_list(DefaultField) andalso is_list(QueryString) ->
    case lucene_scan:string(QueryString) of
        {ok, Tokens, _Line} ->
            case lucene_parse:parse(Tokens) of
                {ok, QueryGraph} -> 
                    %% Success case.
                    QueryGraph1 = #scope { index=DefaultIndex, field=DefaultField, ops=QueryGraph },
                    {ok, QueryGraph1};
                {error, {_, lucene_parse, String}} ->
                    %% Syntax error.
                    {error, {lucene_parse, lists:flatten(String)}};
                {error, Error} -> 
                    %% Other parsing error.
                    {error, Error}
            end;
        {error, Error, _Line} -> {error, Error}
    end.


