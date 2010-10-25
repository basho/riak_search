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

.

Left 100 land.
Left 100 lor.
Right 200 lnot.

Expect 1.

Rootsymbol query.

query -> expr:
    case prune_empty('$1', raw) of
        ?EMPTY ->
            [];
        ASTNode ->
            ASTNode
    end.

expr -> lparen expr entity_bool rparen:
    case prune_empty('$2' ++ '$3', raw) of
        ?EMPTY ->
            ?EMPTY;
        ASTNode when length(ASTNode) == 1 ->
            ASTNode;
        ASTNode ->
            set_default_op(ASTNode)
    end.

expr -> expr entity_bool:
    case prune_empty('$1' ++ '$2', raw) of
        ?EMPTY ->
            ?EMPTY;
        ASTNode when length(ASTNode) == 1 ->
            ASTNode;
        ASTNode ->
            set_default_op(ASTNode)
    end.

expr -> entity_bool:
     '$1'.

field -> field_core:
    '$1'.
field -> prefixes field_core:
    prohibited_to_not(add_options('$2', '$1')).

field_core -> word colon lparen field_body rparen:
    make_field('$1', '$4', []).
field_core -> word colon exclusive_range:
    make_field('$1', '$3', []).
field_core -> word colon inclusive_range:
    make_field('$1', '$3', []).

field_core -> word colon term:
    case '$3' of
        ?EMPTY ->
            ?EMPTY;
        _ ->
            make_field('$1', '$3', [])
    end.

field_body -> lparen field_body rparen:
    if
        length('$2') > 1 ->
            [{group, '$2'}];
        true ->
            '$2'
    end.

field_body -> lnot field_body:
    case '$1' of
        ?EMPTY ->
            ?EMPTY;
        _ ->
            [{lnot, '$2'}]
    end.
field_body -> term land field_body:
    case '$1' of
        ?EMPTY ->
            ?EMPTY;
        _ ->
            [{land, ['$1'] ++ '$3'}]
    end.
field_body -> term lor field_body:
    case '$1' of
        ?EMPTY ->
            ?EMPTY;
        _ ->
            [{lor, ['$1'] ++ '$3'}]
    end.
field_body -> term field_body:
    case '$1' of
        ?EMPTY ->
            ?EMPTY;
        _ ->
            [{default_op(), ['$1'] ++ '$2'}]
    end.
field_body -> term:
    case '$1' of
        ?EMPTY ->
            ?EMPTY;
        _ ->
            ['$1']
    end.

inclusive_range -> lbracket term_core to term_core rbracket:
    {inclusive_range, analyze_range_word(extract_text('$2')),
                       analyze_range_word(extract_text('$4'))}.
inclusive_range -> lbracket term_core to term_core rstache:
    {inclusive_range, analyze_range_word(extract_text('$2')),
                        analyze_range_word(extract_text('$4'))}.
exclusive_range -> lstache term_core to term_core rstache:
    {exclusive_range, analyze_range_word(extract_text('$2')),
                       analyze_range_word(extract_text('$4'))}.
exclusive_range -> lstache term_core to term_core rbracket:
    {exclusive_range, analyze_range_word(extract_text('$2')),
                       analyze_range_word(extract_text('$4'))}.

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
    prune_empty('$2', lnot).
entity_bool -> entity_bool land entity_bool:
    prune_empty('$1' ++ '$3', land).
entity_bool -> entity_bool lor entity_bool:
    prune_empty('$1' ++ '$3', lor).
entity_bool -> term:
    ['$1'].
entity_bool -> field:
    ['$1'].

term -> term_core:
    make_term('$1').
term -> prefixes term_core:
    prohibited_to_not(make_term(add_options('$2', '$1'))).
term-> term_core suffixes:
    make_term(add_options('$1', '$2')).
term -> prefixes term_core suffixes:
    prohibited_to_not(make_term(add_options('$2', '$1' ++ '$3'))).

term_core -> word:
    make_term_core(term, '$1', []).
term_core -> phrase:
    make_term_core(phrase, '$1', []).

Erlang code.
-define(EMPTY, {empty, '$empty', []}).
-ifdef(TEST).
-export([string/1]).
-endif.
-export([string/3]).

-ifdef(TEST).
string(Query) ->
    string(undefined, Query, undefined).
-endif.

string(Pid, Query, Schema) ->
    erlang:put(qilr_schema, Schema),
    erlang:put(qilr_analyzer, Pid),
    case qilr_scan:string(Query) of
        {ok, Tokens, _} ->
            case parse(Tokens) of
	        {ok, AST} ->
                    {ok, AST};
                {error, {_, _, [Message, [91, Err, 93]]}} ->
                    Msg = [list_to_integer(E) || E <- Err,
                                                 is_list(E)],
                    throw({parse_error, lists:flatten([Message, Msg])})
             end;
        Other ->
            Other
    end.

%% Internal functions
make_term_core(term, Term, Opts) ->
    WordText = extract_text(Term),
    {term, WordText, Opts};
make_term_core(phrase, Phrase, Opts) ->
    PhraseText = extract_text(Phrase),
    {phrase, PhraseText, Opts}.

make_term({phrase, PhraseText, Opts}) ->
    case analyze_word(PhraseText) of
        '$empty' ->
            ?EMPTY;
        AT ->
            Opts1 = proplists:delete(proximity_terms, Opts),
            Opts2 = proplists:delete(proximity, Opts1),
            Terms = [prohibited_to_not({term, T, Opts2}) || T <- AT],
            BaseQuery = [{land, Terms}],
            NewOpts = [{base_query, BaseQuery}] ++ Opts,
            {phrase, list_to_binary(PhraseText), NewOpts}
    end;
make_term({term, WordText, Opts0}) ->
    [LC|Text0] = lists:reverse(WordText),
    Text = lists:reverse(Text0),
    {T, Opts} = case LC == $* orelse LC == $? of
                    true ->
                        case analyze_word(list_to_binary(Text)) of
                            '$empty' ->
                                {'$empty', []};
                            [AT] ->
                                case LC of
                                    $* ->
                                        {AT, [{wildcard, all}]};
                                    $? ->
                                        {AT, [{wildcard, one}]}
                                end
                        end;
                    false ->
                        case analyze_word(WordText) of
                            '$empty' ->
                                {'$empty', []};
                            [AT] ->
                                {AT, []}
                        end
                end,
    if
        T =:= '$empty' ->
            ?EMPTY;
        true ->
            {term, T, Opts0 ++ Opts}
    end.


make_field(FieldName, FieldBody, Opts) ->
    {field, list_to_binary(extract_text(FieldName)), FieldBody, Opts}.

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
extract_text({term, TermText, _}) ->
    TermText;
extract_text({phrase, _, Text}) ->
    Text.

add_options({TermType, Body, Existing}, Options) when is_list(Options),
                                                      is_list(Existing) ->
    NewOptions = Existing ++ Options,
    NewOpts1 = case TermType of
        phrase ->
            case proplists:get_value(proximity, NewOptions) /= undefined andalso
                 proplists:get_value(proximity_terms, NewOptions) =:= undefined of
                true ->
                    Terms = string:tokens(string:strip(Body, both, 34), [$\ ]),
                    PT = {proximity_terms, [list_to_binary(T) || T <- Terms]},
                    [PT|NewOptions];
                false ->
                    NewOptions
            end;
        _ ->
            NewOptions
    end,
    case proplists:get_value(base_query, NewOpts1, undefined) of
        undefined ->
            {TermType, Body, NewOpts1};
        [{land, BQ}] ->
	    NewOpts2 = remove_options([proximity, proximity_terms, base_query], NewOpts1),
	    NewOpts3 = remove_options([base_query], NewOpts1),
	    BQ1 = [{term, TBody, NewOpts2} || {term, TBody, _} <- BQ],
            NewOpts4 = [{base_query, [{land, BQ1}]}|NewOpts3],
	    {TermType, Body, NewOpts4}
    end;

add_options({field, Name, Body, Existing}, Options) when is_list(Options),
                                                         is_list(Existing) ->
    {field, Name, Body, Existing ++ Options}.

prohibited_to_not({term, Body, Opts}=Term) ->
    case proplists:get_value(prohibited, Opts) of
        true ->
            NewOpts = proplists:delete(prohibited, Opts),
            {lnot, [{term, Body, NewOpts}]};
        undefined ->
            Term
    end;
prohibited_to_not({field, Name, Body, Opts}=Term) ->
    case proplists:get_value(prohibited, Opts) of
        true ->
            NewOpts = proplists:delete(prohibited, Opts),
            {lnot, [{field, Name, Body, NewOpts}]};
        undefined ->
            Term
    end;
prohibited_to_not(Term) ->
    Term.

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
    case erlang:get(qilr_schema) of
        undefined ->
            lor;
        Schema ->
            case Schema:default_op() of
	        'or' ->
                    lor;
                'and' ->
                    land;
                'not' ->
                    lnot
            end
    end.

analyze_range_word(Text) ->
    case analyze_word(Text) of
        '$empty' ->
            Text;
        [T] ->
            T;
        T ->
            hd(T)
    end.

analyze_word(Text0) ->
    Pid = erlang:get(qilr_analyzer),
    Schema = erlang:get(qilr_schema),
    Text = case is_binary(Text0) of
               true ->
                   binary_to_list(Text0);
               false ->
                   Text0
           end,
    case Pid =:= undefined orelse Schema =:= undefined of
        true ->
	    Text1 = string:strip(string:strip(Text, left, $\"), right, $\"),
	    Tokens = string:tokens(Text1, " "),
	    case [list_to_binary(T) || T <- Tokens,
	    	   	               length(T) > 2] of
                [] ->
                    '$empty';
                V ->
                    V
            end;
        false ->
	    DefaultField = Schema:find_field(Schema:default_field()),
            AnalyzerFactory = Schema:analyzer_factory(DefaultField),
	    {ok, T0} = qilr_analyzer:analyze(Pid, Text, AnalyzerFactory),
            case [ W || W <- T0, W /= skip ] of
                [] ->
                    '$empty';
                T ->
                    T
            end
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

prune_empty(AST, Type) ->
    AST1 = [Node || Node <- AST,
                    Node /= ?EMPTY],
    case length(AST1) of
        0 ->
            ?EMPTY;
        1 ->
            if
	        Type =:= lnot ->
                    emit_pruned_node(AST1, Type);
                true ->
                    AST1
            end;
        _ ->
            case Type of
                raw ->
                    AST1;
                _ ->
                    emit_pruned_node(AST1, Type)
            end
    end.
emit_pruned_node(AST1, lnot) ->
    [{lnot, AST1}];
emit_pruned_node(AST1, land) ->
    [{land, AST1}];
emit_pruned_node(AST1, lor) ->
    [{lor, AST1}];
emit_pruned_node(AST1, _) ->
    AST1.

remove_options([], Opts) ->
    Opts;
remove_options([H|T], Opts) ->
    remove_options(T, proplists:delete(H, Opts)).