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
-define(STOPWORD(X), (X /= "a" andalso X /= "an" andalso X /= "and" andalso X/= "are" andalso X/= "as" andalso X/= "at" andalso X/= "be" andalso X/= "but" andalso X/= "by" andalso X/= "for" andalso X/= "if" andalso X/= "in" andalso X/= "into" andalso X/= "is" andalso X/= "it" andalso X/= "no" andalso X/= "not" andalso X/= "of" andalso X/= "on" andalso X/= "or" andalso X/= "s" andalso X/= "such" andalso X/= "t" andalso X/= "that" andalso X/= "the" andalso X/= "their" andalso X/= "then" andalso X/= "there" andalso X/= "these" andalso X/= "they" andalso X/= "this" andalso X/= "to" andalso X/= "was" andalso X/= "will" andalso X/= "with")).


%% Mimics the DefaultAnalyzerFactory.
default_analyzer_factory(Text, []) ->
    default_analyzer_factory(Text, [3]);
default_analyzer_factory(Text, [MinLength]) ->
    {ok, default(Text, MinLength, [])}.

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
default(<<_,T/binary>>, MinLength, Acc) when length(Acc) >= MinLength->
    default_termify(T, MinLength, Acc);
default(<<_,T/binary>>, MinLength, _) ->
    default(T, MinLength, []);
default(<<>>, MinLength, Acc) when length(Acc) >= MinLength ->
    default_termify(<<>>, MinLength, Acc);
default(<<>>, _MinLength, _Acc) ->
    [].

%% Determine if this term is valid, if so, add it to the list we are
%% generating.
default_termify(T, MinLength, Acc) ->
    Term = lists:reverse(Acc),
    case is_stopword(Term) of
        false ->
            TermBinary = list_to_binary(Term),
            [TermBinary|default(T, MinLength, [])];
        true -> 
            default(T, MinLength, [])
    end.

is_stopword(Term) when length(Term) == 2 -> 
    ordsets:is_element(Term, ["an", "as", "at", "be", "by", "if", "in", "is", "it", "no", "of", "on", "or", "to"]);
is_stopword(Term) when length(Term) == 3 -> 
    ordsets:is_element(Term, ["and", "are", "but", "for", "not", "the", "was"]);
is_stopword(Term) when length(Term) == 4 -> 
    ordsets:is_element(Term, ["into", "such", "that" "then", "they", "this", "will"]);
is_stopword(Term) when length(Term) == 5 -> 
    ordsets:is_element(Term, ["their", "there", "these"]);
is_stopword(_Term) -> 
    false.

%% %% Mimics the DefaultAnalyzerFactory.
%% default_analyzer_factory(Text, Options) ->
%%     Tokens1 = standard_tokenizer(Text, Options),
%%     Tokens2 = length_filter(Tokens1, Options),
%%     Tokens3 = lower_case_filter(Tokens2, Options),
%%     Tokens4 = stop_filter(Tokens3, Options),
%%     {ok, [list_to_binary(X) || X <- Tokens4]}.

%% %% Text is binary. Return [Tokens] as a list of strings.
%% standard_tokenizer(Text, Options) ->
%%     Tokens = standard_tokenizer(Text, Options, []),
%%     lists:reverse(Tokens).
%% standard_tokenizer(<<>>, _Options, []) ->
%%     [];
%% standard_tokenizer(<<>>, _Options, Acc) ->
%%     [lists:reverse(Acc)];
%% standard_tokenizer(<<H,T/binary>>, Options, Acc) when ?UPPERCHAR(H) ->
%%     standard_tokenizer(T, Options, [H|Acc]);
%% standard_tokenizer(<<$.,H,T/binary>>, Options, Acc) when ?UPPERCHAR(H) ->
%%     standard_tokenizer(T, Options, [H,$.|Acc]);
%% standard_tokenizer(<<H,T/binary>>, Options, Acc) when ?CHAR(H) ->
%%     standard_tokenizer(T, Options, [H|Acc]);
%% standard_tokenizer(<<$.,H,T/binary>>, Options, Acc) when ?CHAR(H) ->
%%     standard_tokenizer(T, Options, [H,$.|Acc]);
%% standard_tokenizer(<<_,T/binary>>, Options, []) ->
%%     standard_tokenizer(T, Options, []);
%% standard_tokenizer(<<_,T/binary>>, Options, Acc) ->
%%     [lists:reverse(Acc)|standard_tokenizer(T, Options, [])].

%% %% Greater than 3 characters.
%% %% Tokens is a list of strings.
%% length_filter(Tokens, [MinLength]) ->
%%     [X || X <- Tokens, length(X) >= MinLength];
%% length_filter(Tokens, _Options) ->
%%     [X || X <- Tokens, length(X) >= 3].

%% %% Return lower case token.
%% %% Tokens is a list of strings.
%% lower_case_filter(Tokens, _Options) ->
%%     [string:to_lower(X) || X <- Tokens].

%% %% Filter out stopwords.
%% %% Tokens is a list of strings.
%% stop_filter(Tokens, Options) ->
%%     F = fun(Token) -> 
%%                 not is_stopword(Token, Options)
%%         end,
%%     lists:filter(F, Tokens).

%% is_stopword(Token, _Options) -> 
%%     Stopwords = ["a", "an", "and", "are", "as", "at", "be", "but", "by", "for", 
%%                  "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", 
%%                  "s", "such", "t", "that", "the", "their", "then", "there", 
%%                  "these", "they", "this", "to", "was", "will", "with"],
%%     lists:member(Token, Stopwords).
