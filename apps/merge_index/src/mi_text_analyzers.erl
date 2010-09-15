%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_text_analyzers).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").

-export([
         integer_analyzer_factory/2
]).

-define(UPPERCHAR(C), (C >= $A andalso C =< $Z)).
-define(LOWERCHAR(C), (C >= $a andalso C =< $z)).
-define(NUMBER(C), (C >= $0 andalso C =< $9)).

%% integer_analyzer_factory/2 - Encode any integers in the field, toss
%% everything else. 
integer_analyzer_factory(Text, _) ->
    {ok, integer(Text, [])}.
integer(<<H, T/binary>>, Acc) when ?NUMBER(H) ->
    integer(T, [H|Acc]);
integer(<<$-, T/binary>>, Acc) ->
    integer(T, [$-|Acc]);
integer(<<_, T/binary>>, Acc) when length(Acc) > 0->
    integer_termify(T, Acc);
integer(<<>>, Acc) when length(Acc) > 0->
    integer_termify(<<>>, Acc);
integer(<<>>, []) ->
    [].

integer_termify(T, Acc) ->
    Term = lists:reverse(Acc),
    Integer = list_to_integer(Term),
    [Integer|integer(T, [])].
