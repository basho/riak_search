Definitions.

WHITESPACE  = [\b\f\n\r\t\s\v]*
TERM = (\\\^.|\\.|[^\:^\(^\)^\[^\]^\+^\-^\!^\&^\|^\^^\~^\s])*
STRING = "(\\\^.|\\.|[^\"])*"
Rules.

{WHITESPACE}		: skip_token.
\/\/(.*?\n)		: skip_token.
\/\*(.|\n)*?\*\/	: skip_token.

\:			: {token, {colon, TokenLine, TokenChars}}.
\(			: {token, {lparen, TokenLine, TokenChars}}.
\)			: {token, {rparen, TokenLine, TokenChars}}.
\[			: {token, {lbracket, TokenLine, TokenChars}}.
\]			: {token, {rbracket, TokenLine, TokenChars}}.
\+			: {token, {plus, TokenLine, TokenChars}}.
\-			: {token, {minus, TokenLine, TokenChars}}.
\!			: {token, {lnot, TokenLine, TokenChars}}.
NOT			: {token, {lnot, TokenLine, TokenChars}}.
\&\&			: {token, {land, TokenLine, TokenChars}}.
AND			: {token, {land, TokenLine, TokenChars}}.
OR			: {token, {lor, TokenLine, TokenChars}}.
\|\|			: {token, {lor, TokenLine, TokenChars}}.
TO			: {token, {to, TokenLine, TokenChars}}.
\^			: {token, {caret, TokenLine, TokenChars}}.
\~			: {token, {tilde, TokenLine, TokenChars}}.
{STRING}		: S = lists:sublist(TokenChars, 2, TokenLen - 2),
			  {token, {phrase, TokenLine, S}}.
{TERM}			: {token, {term, TokenLine, TokenChars}}.

%% Without the next line, the scanner would hang on unrecognized tokens...
.			: {error, lists:flatten(io_lib:format("invalid character \"~s\" at line ~p", [TokenChars, TokenLine]))}.

Erlang code.
