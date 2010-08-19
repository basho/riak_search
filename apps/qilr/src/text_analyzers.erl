%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(text_analyzers).
-export([default_analyzer_factory/2]).
-define(CHAR(C), ((C >= $a andalso C =< $z) orelse (C >= $A andalso C =< $z) orelse (C >= $0 andalso C =< $9))).

%% Mimics the DefaultAnalyzerFactory.
default_analyzer_factory(Text, Options) ->
    Tokens1 = standard_tokenizer(Text, Options),
    Tokens2 = length_filter(Tokens1, Options),
    Tokens3 = lower_case_filter(Tokens2, Options),
    Tokens4 = stop_filter(Tokens3, Options),
    {ok, [list_to_binary(X) || X <- Tokens4]}.

%% Text is binary. Return [Tokens] as a list of strings.
standard_tokenizer(Text, Options) ->
    Tokens = standard_tokenizer(Text, Options, []),
    lists:reverse(Tokens).
standard_tokenizer(<<>>, _Options, []) ->
    [];
standard_tokenizer(<<>>, _Options, Acc) ->
    [lists:reverse(Acc)];
standard_tokenizer(<<H,T/binary>>, Options, Acc) when ?CHAR(H) ->
    standard_tokenizer(T, Options, [H|Acc]);
standard_tokenizer(<<$.,H,T/binary>>, Options, Acc) when ?CHAR(H) ->
    standard_tokenizer(T, Options, [H,$.|Acc]);
standard_tokenizer(<<_,T/binary>>, Options, []) ->
    standard_tokenizer(T, Options, []);
standard_tokenizer(<<_,T/binary>>, Options, Acc) ->
    [lists:reverse(Acc)|standard_tokenizer(T, Options, [])].

%% Greater than 3 characters.
%% Tokens is a list of strings.
length_filter(Tokens, [MinLength]) ->
    [X || X <- Tokens, length(X) >= MinLength];
length_filter(Tokens, _Options) ->
    [X || X <- Tokens, length(X) >= 3].

%% Return lower case token.
%% Tokens is a list of strings.
lower_case_filter(Tokens, _Options) ->
    [string:to_lower(X) || X <- Tokens].

%% Filter out stopwords.
%% Tokens is a list of strings.
stop_filter(Tokens, Options) ->
    F = fun(Token) -> 
                not is_stopword(Token, Options)
        end,
    lists:filter(F, Tokens).

is_stopword(Token, _Options) -> 
    Stopwords = ["a", "an", "and", "are", "as", "at", "be", "but", "by", "for", 
                 "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", 
                 "s", "such", "t", "that", "the", "their", "then", "there", 
                 "these", "they", "this", "to", "was", "will", "with"],
    lists:member(Token, Stopwords).
