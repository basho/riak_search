Nonterminals

%% Terms
term term_core

%% Fields
field_core field field_body

%% Decorators
prefixes suffixes fuzzy_prox_suffix boost_suffix

%% Ranges
exclusive_range inclusive_range

%% Queries
query expr

%% Grouping and Booleans
entity_bool
.

Terminals

word phrase plus minus lnot land lor lparen rparen colon
tilde caret to lstache rstache lbracket rbracket

%%colon tilde plus minus caret lnot land lor lparen rparen
%%lbracket rbracket to lstache rstache
.

Left 100 land.
Left 100 lor.
Right 200 lnot.

Expect 1.

Rootsymbol query.

query -> expr:
      '$1'.

expr -> lparen expr entity_bool rparen:
    set_default_op('$2' ++ '$3').
expr -> expr entity_bool:
    set_default_op('$1' ++ '$2').
expr -> entity_bool:
    '$1'.

field -> field_core:
    '$1'.
field -> prefixes field_core:
    add_options('$2', '$1').

field_core -> word colon lparen field_body rparen:
    make_field('$1', '$4', []).
field_core -> word colon exclusive_range:
    make_field('$1', '$3', []).
field_core -> word colon inclusive_range:
    make_field('$1', '$3', []).
field_core -> word colon term:
    make_field('$1', '$3', []).

field_body -> lparen field_body rparen:
    if
        length('$2') > 1 ->
            [{group, '$2'}];
        true ->
            '$2'
    end.

field_body -> lnot field_body:
    [{lnot, '$2'}].
field_body -> term land field_body:
    [{land, ['$1'] ++ '$3'}].
field_body -> term lor field_body:
    [{lor, ['$1'] ++ '$3'}].
field_body -> term field_body:
    [{default_op(), ['$1'] ++ '$2'}].
field_body -> term:
    ['$1'].

inclusive_range -> lbracket term to term rbracket:
    [{inclusive_range, '$2', '$4'}].
inclusive_range -> lbracket term to term rstache:
    [{inclusive_range, '$2', '$4'}].
exclusive_range -> lstache term to term rstache:
    [{exclusive_range, '$2', '$4'}].
exclusive_range -> lstache term to term rbracket:
    [{exclusive_range, '$2', '$4'}].

prefixes -> prefixes plus:
    '$1' ++ [required].
prefixes -> prefixes minus:
    '$1' ++ [prohibited].
prefixes -> plus:
    [required].
prefixes -> minus:
    [prohibited].

fuzzy_prox_suffix -> tilde:
    [{fuzzy, "0.5"}].
fuzzy_prox_suffix -> tilde word:
    make_fuzzy_prox_suffix('$2').

boost_suffix -> caret word:
    make_boost_suffix('$2').

suffixes -> suffixes fuzzy_prox_suffix:
    '$1' ++ '$2'.
suffixes -> suffixes boost_suffix:
    '$1' ++ '$2'.
suffixes -> fuzzy_prox_suffix:
    '$1'.
suffixes -> boost_suffix:
    '$1'.

entity_bool -> lparen entity_bool rparen:
    if
        length('$2') > 1 ->
            [{group, '$2'}];
        true ->
            '$2'
    end.

entity_bool -> lnot entity_bool:
    [{lnot, '$2'}].
entity_bool -> entity_bool land entity_bool:
    [{land, '$1' ++ '$3'}].
entity_bool -> entity_bool lor entity_bool:
    [{lor, '$1' ++ '$3'}].
entity_bool -> term:
    ['$1'].
entity_bool -> field:
    ['$1'].

term -> term_core:
    '$1'.
term -> prefixes term_core:
    add_options('$2', '$1').
term-> term_core suffixes:
    add_options('$1', '$2').
term -> prefixes term_core suffixes:
    add_options('$2', '$1' ++ '$3').

term_core -> word:
    make_term(term, '$1', []).
term_core -> phrase:
    make_term(phrase, '$1', []).

Erlang code.
-export([string/1, string/2]).

string(Query, _) ->
    string(Query).
string(Query) ->
    case qilr_scan:string(Query) of
        {ok, Tokens, _} ->
            case parse(Tokens) of
	        {ok, AST} ->
                    {ok, qilr_post:process(AST)};
                {error, {_, _, [Message, [91, Err, 93]]}} ->
                    Msg = [list_to_integer(E) || E <- Err,
                                                 is_list(E)],
                    throw({parse_error, lists:flatten([Message, Msg])})
             end;
        Other ->
            Other
    end.

%% Internal functions
make_term(phrase, Phrase, Opts) ->
    {phrase, list_to_binary(extract_text(Phrase)), Opts};
make_term(term, Word, Opts0) ->
    WordText = extract_text(Word),
    [LC|Text0] = lists:reverse(WordText),
    Text = lists:reverse(Text0),
    {T, Opts} = case LC == $* of
               true ->
                   {Text, [{wildcard, all}]};
               false ->
                   {WordText, []}
           end,
    {term, list_to_binary(T), Opts0 ++ Opts}.

make_field(FieldName, FieldBody, Opts) ->
    {field, extract_text(FieldName), FieldBody, Opts}.

make_fuzzy_prox_suffix({word, LineNum, Text}) ->
    %% Does text have a decimal point
    {Fun, Type} = case lists:member($., Text) of
                      true ->
                          {fun erlang:list_to_float/1, fuzzy};
                      false ->
                          {fun erlang:list_to_integer/1, proximity}
                  end,
    case make_fuzzy_prox_suffix(Fun, Type, Text) of
        {ok, S} ->
            S;
        error ->
            throw({parse_error, LineNum, Text})
    end.

make_boost_suffix({word, LineNum, Text}) ->
    case is_numeric(Text) of
        false ->
            throw({parse_error, LineNum, Text});
        true ->
            [{boost, Text}]
    end.

make_fuzzy_prox_suffix(F, Type, Text) ->
    case catch F(Text) of
        {'EXIT', _} ->
            error;
        _ ->
            {ok, [{Type, Text}]}
    end.

extract_text({word, _, Text}) ->
    Text;
extract_text({phrase, _, Text}) ->
    Text.

add_options({TermType, Text, Existing}, Options) when is_list(Options),
                                                      is_list(Existing) ->
    NewOptions = Existing ++ Options,
    case TermType of
        phrase ->
            case proplists:get_value(proximity, NewOptions) /= undefined andalso
                 proplists:get_value(proximity_terms, NewOptions) =:= undefined of
                true ->
                    Terms = string:tokens(string:strip(binary_to_list(Text), both, 34), [$\ ]),
                    PT = {proximity_terms, [list_to_binary(T) || T <- Terms]},
                    {TermType, Text, [PT|NewOptions]};
                false ->
                    {TermType, Text, NewOptions}
            end;
        _ ->
            {TermType, Text, NewOptions}
    end;
add_options({field, Name, Body, Existing}, Options) when is_list(Options),
                                                         is_list(Existing) ->
    {field, Name, Body, Existing ++ Options}.

set_default_op(Q) when length(Q) == 2 ->
    DefaultOp = default_op(),
    [H|T] = Q,
    Type = element(1, H),
    case Type =:= DefaultOp of
        true ->
            {Type, Body} = H,
            [{Type, Body ++ T}];
        false ->
            [{DefaultOp, Q}]
    end;
set_default_op(Q) ->
    Q.

default_op() ->
    case erlang:get(qilr_default_op) of
        undefined ->
            lor;
        Op ->
            Op
    end.

is_numeric(Word) ->
    case catch list_to_integer(Word) of
        {'EXIT', _} ->
            case catch list_to_float(Word) of
                {'EXIT', _} ->
                    false;
                _ ->
                    true
            end;
        _ ->
            true
    end.
