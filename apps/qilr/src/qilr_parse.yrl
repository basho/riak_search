Nonterminals

query query_term plain_term reqd_omit_prefix field_prefix tilde_suffix boost_suffix
bool_expr expr group_expr group_body field_group
.

Terminals

term phrase colon tilde plus minus caret lnot land lor lparen rparen
lbracket rbracket to lstache rstache
.

Rootsymbol query.

Expect 1.

query -> expr:
    '$1'.

expr -> query_term:
    ['$1'].
expr -> bool_expr:
    ['$1'].
expr -> group_expr:
    ['$1'].
expr -> field_group:
    ['$1'].
expr -> expr query_term:
    add_node('$1', '$2').
expr -> expr bool_expr:
    [add_operand('$2', '$1')].
expr -> expr group_expr:
    add_node('$1', '$2').
expr -> expr field_group:
    add_node('$1', '$2').

group_body -> bool_expr:
    '$1'.
group_body -> query_term:
    '$1'.
group_body -> group_body bool_expr:
    [add_operand('$2', '$1')].
group_body -> group_body query_term:
    add_node('$1', '$2').

group_expr -> lparen group_body rparen:
    collapse_group({group, emit_group_expr('$2')}).
group_expr -> lparen group_expr rparen:
    collapse_group({group, emit_group_expr('$2')}).

field_group -> field_prefix group_expr:
    make_field_term('$1', '$2').

bool_expr -> lnot query_term:
    {lnot, '$2'}.
bool_expr -> land query_term:
    {land, '$2'}.
bool_expr -> lor query_term:
    {lor, '$2'}.
bool_expr -> lnot group_expr:
    {lnot, '$2'}.
bool_expr -> land group_expr:
    {land, '$2'}.
bool_expr -> lor group_expr:
    {lor, '$2'}.

query_term -> plain_term:
    '$1'.
query_term -> reqd_omit_prefix plain_term:
    add_attribute('$2', '$1').
query_term -> field_prefix plain_term:
    make_field_term('$1', '$2').
query_term -> reqd_omit_prefix field_prefix plain_term:
    add_attribute(make_field_term('$1', '$2'), '$1').

query_term -> plain_term tilde_suffix:
    make_term('$1', '$2').
query_term -> reqd_omit_prefix plain_term tilde_suffix:
    add_attribute(make_term('$1', '$2'), '$3').
query_term -> field_prefix plain_term tilde_suffix:
    make_field_term('$1', '$2', '$3').
query_term -> reqd_omit_prefix field_prefix plain_term tilde_suffix:
    add_attribute(make_field_term('$1', '$2', '$3'), '$4').

query_term -> plain_term boost_suffix:
    make_term('$1', '$2').
query_term -> plain_term tilde_suffix boost_suffix:
    make_term('$1', '$2' ++ '$3').
query_term -> reqd_omit_prefix plain_term tilde_suffix boost_suffix:
    add_attribute(make_term('$1', '$2' ++ '$4'), '$1').
query_term -> field_prefix plain_term tilde_suffix boost_suffix:
    make_field_term('$1', '$2', '$3' ++ '$4').
query_term -> reqd_omit_prefix field_prefix plain_term tilde_suffix boost_suffix:
    add_attribute(make_field_term('$1', '$2', '$3' ++ '$5'), '$1').

query_term -> field_prefix lbracket plain_term to plain_term rbracket:
    make_field_term('$1', {inclusive_range, '$3', '$5'}).
query_term -> field_prefix lbracket plain_term to plain_term rstache:
    make_field_term('$1', {inclusive_range, '$3', '$5'}).
query_term -> field_prefix lstache plain_term to plain_term rstache:
    make_field_term('$1', {exclusive_range, '$3', '$5'}).
query_term -> field_prefix lstache plain_term to plain_term rbracket:
    make_field_term('$1', {exclusive_range, '$3', '$5'}).


plain_term -> term:
    make_term('$1').
plain_term -> phrase:
    make_term('$1').

field_prefix -> term colon:
    make_field_name('$1').

reqd_omit_prefix -> plus:
    required.
reqd_omit_prefix -> minus:
    prohibited.

tilde_suffix -> tilde:
    {fuzzy, 0.5}.
tilde_suffix -> tilde term:
    make_suffix('$2').
boost_suffix -> caret term:
    make_boost('$2').

Erlang code.
-export([string/1, string/2]).
string(Query) ->
    string(Query, 'or').
string(Query, Bool0) when Bool0 =:= 'and' orelse
                          Bool0 =:= 'or' ->
    Bool = if
               Bool0 =:= 'and' ->
                   land;
               true ->
                   lor
           end,
    {ok, Tokens, _} = qilr_scan:string(Query),
    qilr_optimizer:optimize(parse(Tokens), [{default_bool, Bool}]).

%% Internal functions
add_node(Parent, Child) when is_list(Parent) ->
    Parent ++ [Child];
add_node(Parent, Child) ->
    [Parent] ++ [Child].

emit_group_expr(Expr) when is_list(Expr) ->
    Expr;
emit_group_expr(Expr) ->
    [Expr].

add_operand({lnot, _}=Bool, Term) ->
    [Term] ++ [Bool];
add_operand({BoolType, Op2}, [{BoolType, Op1}]) ->
    {BoolType, Op1 ++ [Op2]};
add_operand({BoolType, Op2}, Op1) when is_list(Op1) ->
    {BoolType, Op1 ++ [Op2]};
add_operand({BoolType, Op2}, Op1) ->
    {BoolType, [Op1, Op2]}.


make_term({Type, _, Term}) when Type =:= phrase orelse Type =:= term->
    QMark = string:chr(Term, $?),
    EscQMark = string:str(Term, "\\?"),
    Star = string:chr(Term, $*),
    EscStar = string:str(Term, "\\*"),
    case QMark > 0 andalso EscQMark /= QMark - 1 of
        true ->
            {term, Term, [{wildcard, one}]};
        false ->
            case Star > 0 andalso EscStar /= Star - 1 of
                true ->
                    {term, Term, [{wildcard, all}]};
                false ->
                    {term, Term, []}
            end
    end.

make_term({term, Term, SL0}, SL1) when is_list(SL1) ->
    {term, Term, SL0 ++ SL1};
make_term({term, Term, SL0}, SL1) ->
    {term, Term, SL0 ++ [SL1]};
make_term(Attr, {term, Term, SL0}) when is_atom(Attr) ->
    {term, Term, SL0 ++ [Attr]}.

make_field_name({term, _, Term}) ->
    {field, Term}.

make_field_term({field, Field}, {term, Term, SL}) ->
    {field, Field, Term, SL};
make_field_term({field, Field}, {group, _}=Group) ->
    {field, Field, Group};
make_field_term({field, Field}, {RangeType, _, _}=Range) when RangeType =:= inclusive_range orelse
                                                              RangeType =:= exclusive_range ->
    {field, Field, [Range]}.

make_field_term({field, Field}, {term, Term, SL0}, SL) ->
    {field, Field, Term, SL0 ++ SL}.

make_suffix({term, Line, Term}) ->
    try
        case lists:member($., Term) of
            true ->
                {fuzzy, list_to_float(Term)};
            false ->
                {proximity, list_to_integer(Term)}
        end
    catch
        error:badarg ->
            throw({parse_error, Line, Term})
    end.

add_attribute({term, Term, Attrs}, Attr) ->
    {term, Term, [Attr|Attrs]};
add_attribute({field, Field, Term, Attrs}, Attr) ->
    {field, Field, Term, [Attr|Attrs]}.

make_boost({term, Line, Term}) ->
    try
        BoostAmt = case lists:member($., Term) of
                       true ->
                           list_to_float(Term);
                       false ->
                           list_to_integer(Term)
                       end,
        {boost, BoostAmt}
    catch
        error:badarg ->
            throw({parse_error, Line, Term})
    end.

collapse_group({group, [{group, Terms}]}) ->
    {group, Terms};
collapse_group(Group) ->
    Group.