%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(text_analyzers).
-export([default_analyzer_factory/2]).
-define(UPPERCHAR(C), (C >= $A andalso C =< $Z)).
-define(LOWERCHAR(C), (C >= $a andalso C =< $z)).
-define(NUMBER(C), (C >= $0 andalso C =< $9)).

%% Mimics the DefaultAnalyzerFactory.
default_analyzer_factory(Text, [MinLength]) ->
    {ok, default(Text, MinLength, [])};
default_analyzer_factory(Text, _Other) ->
    default_analyzer_factory(Text, [3]).

default(<<H, T/binary>>, MinLength, Acc) when ?UPPERCHAR(H) ->
    H1 = H + ($a - $A),
    default(T, MinLength, [H1|Acc]);
default(<<H, T/binary>>, MinLength, Acc) when ?LOWERCHAR(H) orelse ?NUMBER(H) ->
    default(T, MinLength, [H|Acc]);
default(<<$.,H,T/binary>>, MinLength, Acc) when ?UPPERCHAR(H) ->
    H1 = H + ($a - $A),
    default(T, MinLength, [H1,$.|Acc]);
default(<<$.,H,T/binary>>, MinLength, Acc) when ?LOWERCHAR(H) orelse ?NUMBER(H) ->
    default(T, MinLength, [H,$.|Acc]);
default(<<_,T/binary>>, MinLength, Acc) ->
    default_termify(T, MinLength, Acc);
default(<<>>, MinLength, Acc) ->
    default_termify(<<>>, MinLength, Acc).

%% Determine if this term is valid, if so, add it to the list we are
%% generating.
default_termify(<<>>, _MinLength, []) ->
    [];
default_termify(T, MinLength, []) ->
    default(T, MinLength, []);
default_termify(T, MinLength, Acc) when length(Acc) < MinLength ->
    %% mimic org.apache.lucene.analysis.LengthFilter,
    %% which does not incement position index
    default(T, MinLength, []);
default_termify(T, MinLength, Acc) ->
    Term = lists:reverse(Acc),
    case is_stopword(Term) of
        false ->
            TermBinary = list_to_binary(Term),
            [TermBinary|default(T, MinLength, [])];
        true -> 
            [skip|default(T, MinLength, [])]
    end.

is_stopword(Term) when length(Term) == 2 -> 
    ordsets:is_element(Term, ["an", "as", "at", "be", "by", "if", "in", "is", "it", "no", "of", "on", "or", "to"]);
is_stopword(Term) when length(Term) == 3 -> 
    ordsets:is_element(Term, ["and", "are", "but", "for", "not", "the", "was"]);
is_stopword(Term) when length(Term) == 4 -> 
    ordsets:is_element(Term, ["into", "such", "that", "then", "they", "this", "will"]);
is_stopword(Term) when length(Term) == 5 -> 
    ordsets:is_element(Term, ["their", "there", "these"]);
is_stopword(_Term) -> 
    false.
