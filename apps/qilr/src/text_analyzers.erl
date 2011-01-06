%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(text_analyzers).
-export([
         standard_analyzer_factory/2,
         whitespace_analyzer_factory/2,
         noop_analyzer_factory/2,
         integer_analyzer_factory/2
        ]).

-define(UPPERCHAR(C),  (C >= $A andalso C =< $Z)).
-define(LOWERCHAR(C),  (C >= $a andalso C =< $z)).
-define(NUMBER(C),     (C >= $0 andalso C =< $9)).
-define(WHITESPACE(C), ((C == $\s) orelse (C == $\n) orelse (C == $\t) orelse (C == $\f) orelse (C == $\r) orelse (C == $\v))).

%% @doc Tokenize incoming text using roughly the same rules as the
%% StandardAnalyzerFactory in Lucene/Java.
standard_analyzer_factory(Text, [MinLengthArg]) ->
    MinLength = list_to_integer(MinLengthArg),
    {ok, standard(Text, MinLength, [], [])};
standard_analyzer_factory(Text, _Other) ->
    {ok, standard(Text, 3, [], [])}.

standard(<<H, T/binary>>, MinLength, Acc, ResultAcc) when ?UPPERCHAR(H) ->
    H1 = H + ($a - $A),
    standard(T, MinLength, [H1|Acc], ResultAcc);
standard(<<H, T/binary>>, MinLength, Acc, ResultAcc) when ?LOWERCHAR(H) orelse ?NUMBER(H) ->
    standard(T, MinLength, [H|Acc], ResultAcc);
standard(<<$.,H,T/binary>>, MinLength, Acc, ResultAcc) when ?UPPERCHAR(H) ->
    H1 = H + ($a - $A),
    standard(T, MinLength, [H1,$.|Acc], ResultAcc);
standard(<<$.,H,T/binary>>, MinLength, Acc, ResultAcc) when ?LOWERCHAR(H) orelse ?NUMBER(H) ->
    standard(T, MinLength, [H,$.|Acc], ResultAcc);
standard(<<_,T/binary>>, MinLength, Acc, ResultAcc) ->
    standard_termify(T, MinLength, Acc, ResultAcc);
standard(<<>>, MinLength, Acc, ResultAcc) ->
    standard_termify(<<>>, MinLength, Acc, ResultAcc).

%% Determine if this term is valid, if so, add it to the list we are
%% generating.
standard_termify(<<>>, _MinLength, [], ResultAcc) ->
    lists:reverse(ResultAcc);
standard_termify(T, MinLength, [], ResultAcc) ->
    standard(T, MinLength, [], ResultAcc);
standard_termify(T, MinLength, Acc, ResultAcc) when length(Acc) < MinLength ->
    %% mimic org.apache.lucene.analysis.LengthFilter,
    %% which does not incement position index
    standard(T, MinLength, [], ResultAcc);
standard_termify(T, MinLength, Acc, ResultAcc) ->
    Term = lists:reverse(Acc),
    case is_stopword(Term) of
        false ->
            TermBinary = list_to_binary(Term),
            NewResultAcc = [TermBinary|ResultAcc];
        true -> 
            NewResultAcc = [skip|ResultAcc]
    end,
    standard(T, MinLength, [], NewResultAcc).


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

%% @doc Tokenize incoming text using whitespace, return the list of
%% tokens.
whitespace_analyzer_factory(Text, _) ->
    {ok, whitespace(Text, [], [])}.
whitespace(<<H, T/binary>>, Acc, ResultAcc) when ?WHITESPACE(H) ->
    whitespace_termify(T, Acc, ResultAcc);
whitespace(<<H, T/binary>>, Acc, ResultAcc) ->
    whitespace(T, [H|Acc], ResultAcc);
whitespace(<<>>, Acc, ResultAcc) when length(Acc) > 0 ->
    whitespace_termify(<<>>, Acc, ResultAcc);
whitespace(<<>>, [], ResultAcc) ->
    lists:reverse(ResultAcc).

whitespace_termify(T, [], ResultAcc) ->
    whitespace(T, [], ResultAcc);
whitespace_termify(T, Acc, ResultAcc) ->
    Term = list_to_binary(lists:reverse(Acc)),
    whitespace(T, [], [Term|ResultAcc]).

%% @doc Treat the incoming text as a single token.
noop_analyzer_factory(Text, _) ->
    {ok, [Text]}.

%% @doc Tokenize incoming text as integers.
integer_analyzer_factory(Text, [PadSizeArg]) ->
    PadSize = list_to_integer(PadSizeArg),
    {ok, integer(Text, PadSize, [], [])}.
integer(<<H, T/binary>>, PadSize, Acc, ResultAcc) when ?NUMBER(H) orelse (H == $-) ->
    integer(T, PadSize, [H|Acc], ResultAcc);
integer(<<_, T/binary>>, PadSize, Acc, ResultAcc) ->
    integer_termify(T, PadSize, Acc, ResultAcc);
integer(<<>>, PadSize, Acc, ResultAcc) when length(Acc) > 0 ->
    integer_termify(<<>>, PadSize, Acc, ResultAcc);
integer(<<>>, _PadSize, [], ResultAcc) ->
    lists:reverse(ResultAcc).

integer_termify(T, PadSize, [], ResultAcc) ->
    integer(T, PadSize, [], ResultAcc);
integer_termify(T, PadSize, Acc, ResultAcc) ->
    try 
        %% Ensure that we have a valid integer.
        Term = lists:reverse(Acc),
        _ValidInteger = list_to_integer(Term),
        BTerm = list_to_binary(Term),
        PaddedTerm = integer_pad(BTerm, PadSize),
        integer(T, PadSize, [], [PaddedTerm|ResultAcc])
    catch _ : _ ->
        integer(T, PadSize, [], ResultAcc)
    end.

%% @private Given an integer in binary form, pad it with zeros until
%% it is at least PadSize characters.
integer_pad(<<$-, T/binary>>, PadSize) ->
    T1 = integer_pad(T, PadSize - 1),
    <<$-, T1/binary>>;
integer_pad(T, PadSize) when size(T) < PadSize ->
    integer_pad(<<$0, T/binary>>, PadSize);
integer_pad(T, _) ->
    T.
