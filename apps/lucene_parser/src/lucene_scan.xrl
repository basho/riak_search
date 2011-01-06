%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

Definitions.

% Whitespace.
WS = [\b\f\n\r\t\s\v]

% A single quoted string. Special characters are allowed in string.
SNGSTRING = '(\\'|[^'])*'

% A double quoted string. Special characters are allowed in string.
DBLSTRING = \"(\\\"|[^\"])*\"

% An unquoted string. 
% Must not start with a special character.  
% Must not contain special characters (the second clause is the same
% as the first, but allows for +/-/! inside the word without escaping.
STRING = (\\.|[^\+\-\!\s\t\n\r\"\'\[\]\\:^~&|(){}])(\\.|[^\s\t\n\r\"\'\[\]\\:^~&|(){}])*
WC_CHAR_STRING = (\\.|[^\+\-\!\s\t\n\r\"\'\[\]\\:^~&|(){}])(\\.|[^\s\t\n\r\"\'\[\]\\:^~&|(){}])*[^\\]\?
WC_GLOB_STRING = (\\.|[^\+\-\!\s\t\n\r\"\'\[\]\\:^~&|(){}])(\\.|[^\s\t\n\r\"\'\[\]\\:^~&|(){}])*[^\\]\*

% Fuzzy search.
FUZZY0 = ~
FUZZY1 = ~[01]\.[0-9]+

% Proximity search.
PROXIMITY = ~[0-9]*

% Proximity search.
BOOST0 = \^
BOOST1 = \^[0-9]+
BOOST2 = \^[0-9]+\.[0-9]+

% From QueryParser.jj listing all special characters.
% | <#_TERM_START_CHAR: ( ~[ " ", "\t", "\n", "\r", "\u3000", "+", "-", "!", "(", ")", ":", "^",
%                            "[", "]", "\"", "{", "}", "~", "*", "?", "\\" ]

Rules.
AND              : {token, {intersection, TokenLine, TokenChars}}.
&*               : {token, {intersection, TokenLine, TokenChars}}.
OR               : {token, {union, TokenLine, TokenChars}}.
\|*              : {token, {union, TokenLine, TokenChars}}.
NOT              : {token, {negation, TokenLine, TokenChars}}.
\!*              : {token, {negation, TokenLine, TokenChars}}.
\:*              : {token, {scope, TokenLine, TokenChars}}.
\+*              : {token, {required, TokenLine, TokenChars}}.
\-*              : {token, {prohibited, TokenLine, TokenChars}}.
\(               : {token, {group_start, TokenLine, TokenChars}}.
\)               : {token, {group_end, TokenLine, TokenChars}}.
\[               : {token, {inclusive_start, TokenLine, TokenChars}}.
\]               : {token, {inclusive_end, TokenLine, TokenChars}}.
\{               : {token, {exclusive_start, TokenLine, TokenChars}}.
\}               : {token, {exclusive_end, TokenLine, TokenChars}}.
TO               : {token, {range_to, TokenLine, TokenChars}}.
{FUZZY0}         : {token, {fuzzy, TokenLine, 0.5}}.
{FUZZY1}         : {token, {fuzzy, TokenLine, fuzzy_to_float(TokenChars)}}.
{PROXIMITY}      : {token, {proximity, TokenLine, proximity_to_integer(TokenChars)}}.
{BOOST0}         : {token, {boost, TokenLine, 1.0}}.
{BOOST1}         : {token, {boost, TokenLine, boost1_to_float(TokenChars)}}.
{BOOST2}         : {token, {boost, TokenLine, boost2_to_float(TokenChars)}}.
{SNGSTRING}      : {token, {string, TokenLine, unescape(strip(TokenChars))}}.
{DBLSTRING}      : {token, {string, TokenLine, unescape(strip(TokenChars))}}.
{WC_CHAR_STRING} : {token, {wildcard_char, TokenLine, unescape(TokenChars)}}.
{WC_GLOB_STRING} : {token, {wildcard_glob, TokenLine, unescape(TokenChars)}}.
{STRING}         : {token, {string, TokenLine, unescape(TokenChars)}}.
{WS}             : skip_token.
.                : {error, lists:flatten(io_lib:format("invalid character \"~s\" at line ~p", [TokenChars, TokenLine]))}.

Erlang code.

%% Strip the first and last char, which will be quotes. Unescape anything else.
strip(S) ->
    lists:sublist(S, 2, length(S) - 2).

%% Unescape any escaped chars *except* for '*' and '?'.
unescape(S) ->
    unescape(S, []).
unescape([$\\,C|Rest], Acc) when C/=$* andalso C/=$? ->
    unescape(Rest, [C|Acc]);
unescape([C|Rest], Acc) ->
    unescape(Rest, [C|Acc]);
unescape([], Acc) ->
    lists:reverse(Acc).

%% Convert a fuzzy match to float. We know first char is ~.
fuzzy_to_float(S) ->
    list_to_float(tl(S)).

%% Convert a proximity search to integer. We know first char is ~.
proximity_to_integer(S) ->
    list_to_integer(tl(S)).

%% Convert a search boost to integer. We know first char is ^.
boost1_to_float(S) ->
    float(list_to_integer(tl(S))).

boost2_to_float(S) ->
    list_to_float(tl(S)).
