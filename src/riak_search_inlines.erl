%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_inlines).
%% -export([passes_inlines/2]).
%% -include("riak_search.hrl").

%% %% Convert all fields to list.
%% -import(riak_search_utils, [to_list/1]).

%% passes_inlines(Props, InlineFields) when is_list(InlineFields) ->
%%     F = fun(X) -> passes_inlines(Props, X) end,
%%     lists:all(F, InlineFields);

%% passes_inlines(Props, Inline) when is_record(Inline, land) ->
%%     F = fun(X) -> passes_inlines(Props, X) end,
%%     lists:all(F, Inline#land.ops);

%% passes_inlines(Props, Inline) when is_record(Inline, lor) ->
%%     F = fun(X) -> passes_inlines(Props, X) end,
%%     lists:any(F, Inline#lor.ops);

%% passes_inlines(Props, Inline) when is_record(Inline, lnot) ->
%%     not passes_inlines(Props, Inline#lnot.ops);

%% passes_inlines(Props, Inline) when is_record(Inline, inclusive_range) ->
%%     Start = hd(Inline#inclusive_range.start_op),
%%     End = hd(Inline#inclusive_range.end_op),
%%     {_Index, StartField, StartValue} = Start#term.q,
%%     {_Index, StartField, EndValue} = End#term.q,
%%     Value = proplists:get_value(StartField, Props),
%%     StartValue =< Value andalso Value =< EndValue;

%% passes_inlines(Props, Inline) when is_record(Inline, exclusive_range) ->
%%     Start = hd(Inline#exclusive_range.start_op),
%%     End = hd(Inline#exclusive_range.end_op),
%%     {_Index, StartField, StartValue} = Start#term.q,
%%     {_Index, StartField, EndValue} = End#term.q,
%%     Value = proplists:get_value(StartField, Props),
%%     StartValue < Value andalso Value < EndValue;

%% passes_inlines(Props, Inline) when is_record(Inline, term) ->
%%     {_Index, Field, SearchTerm} = Inline#term.q,
%%     InlineTerm = proplists:get_value(Field, Props),
%%     case proplists:keyfind(1, 'wildcard', Inline#term.options) of
%%         {wildcard, Wildcard} when Wildcard == 'one'; Wildcard == 'all' ->
%%             wildcard_match(SearchTerm, InlineTerm, Wildcard);
%%         _ ->
%%             %% Exact match.
%%             SearchTerm == InlineTerm
%%     end.
%% wildcard_match(SearchTerm, InlineTerm, Wildcard) when is_binary(SearchTerm), is_binary(InlineTerm) -> 
%%     Size = size(SearchTerm),
%%     case InlineTerm of
%%         <<SearchTerm:Size/binary, _>> when Wildcard=='one' -> 
%%             true;
%%         <<SearchTerm:Size/binary, _/binary>> when Wildcard=='all' -> 
%%             true;
%%         _ -> 
%%             false
%%     end;
%% wildcard_match(_, _, _) ->
%%     false.
