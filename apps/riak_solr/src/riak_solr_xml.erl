%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% XML parser that only handles Solr-formatted XML files. For large
%% XML files, using this instead of xmerl improves parsing speed by
%% 10x.

-module(riak_solr_xml).
-export([parse/1, file/1]).
-define(IS_WHITESPACE(C), (C == $\s orelse C == $\t orelse C == $\r orelse C == $\n)).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).



%% Parse Solr XML as quickly as possible. Does minimal checking for correctness.
%% add -> doc / field name
%% delete -> id, query
%% Return {ok, Command, Entries}.

file(File) ->
    {ok, B} = file:read_file(File),
    parse(B).

parse(XML) ->
    is_binary(XML) orelse 
        throw({not_a_binary, XML}),
    parse_operation(XML).

%% TODO - Skip attributes.
parse_operation(<<>>) ->
    {ok, add, []};
parse_operation(<<"<add>", T/binary>>) ->
    {ok, add, parse_add(T)};
parse_operation(<<"<delete>", _/binary>>) -> 	
    {ok, delete, []};
parse_operation(<<_, T/binary>>) ->
    parse_operation(T).


%%% PARSE ADD %%%

%% Parse the list of documents.    
parse_add(<<>>) ->
    [];
parse_add(<<"<doc>", T/binary>>) ->
    {ok, Fields, NewT} = parse_doc(T, []),
    [{"doc", Fields}|parse_add(NewT)];
parse_add(<<_, T/binary>>) ->
    parse_add(T).
    
%% Parse the fields of a document.
parse_doc(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
parse_doc(<<"</doc>", T/binary>>, Acc) ->
    {ok, Acc, T};
parse_doc(<<"<field ", T/binary>>, Acc) ->
    {ok, FieldName, T1} = parse_field_name(T),
    {ok, T2} = peel_to($>, T1),
    {ok, FieldValue, T3} = parse_field_value(T2, []),
    parse_doc(T3, [{FieldName, FieldValue}|Acc]);
parse_doc(<<_, T/binary>>, Acc) ->
    parse_doc(T, Acc).

%% Parse the field name attribute.
parse_field_name(<<"name", T/binary>>) ->
    {ok, T1} = peel_to($", T),
    parse_field_name_inner(T1, []);
parse_field_name(<<_, T/binary>>) ->
    parse_field_name(T).
    
%% Parse the value of the  field name attribute. (Between quotes.)
parse_field_name_inner(<<$", T/binary>>, Acc) ->
    FieldName = lists:reverse(Acc),
    {ok, FieldName, T};
parse_field_name_inner(<<H, T/binary>>, Acc) ->
    parse_field_name_inner(T, [H|Acc]).

%% Extract a plain field value.
parse_field_value(<<"<![CDATA[", T/binary>>, []) ->
    parse_field_value_cdata(T, []);
parse_field_value(<<$<, T/binary>>, Acc) ->
    FieldValue = lists:reverse(Acc),
    {ok, FieldValue, T};
parse_field_value(<<H, T/binary>>, Acc) ->
    parse_field_value(T, [H|Acc]).

%% Extract the value from a CDATA section.
parse_field_value_cdata(<<"]]>", T/binary>>, Acc) ->
    FieldValue = lists:reverse(Acc),
    {ok, FieldValue, T};
parse_field_value_cdata(<<H, T/binary>>, Acc) ->
    parse_field_value_cdata(T, [H|Acc]).


%%% PARSE DELETE %%%








peel_to(C, <<C, T/binary>>) ->
    {ok, T};
peel_to(C, <<_, T/binary>>) ->
    peel_to(C, T);
peel_to(_, <<>>) ->
    {ok, <<>>}.
