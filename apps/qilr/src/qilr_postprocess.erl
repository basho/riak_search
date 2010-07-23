-module(qilr_postprocess).

-export([optimize/3]).

-include("qilr.hrl").

optimize(AnalyzerPid, {ok, Query}, Schema) when not is_list(Query) ->
    optimize(AnalyzerPid, {ok, [Query]}, Schema);
optimize(AnalyzerPid, {ok, Query0}, Schema) when is_list(Query0) ->
    %% Get default boolean op..
    case Schema:default_op() of
        'and' -> DefaultBoolOp = land;
        _     -> DefaultBoolOp = lor
    end,

    %% Process and consolidate the query...
    Query1 = analyze_terms(AnalyzerPid, Query0, Schema),
    Query2 = consolidate_exprs(Query1, []),
    Query3 = default_bool(Query2, DefaultBoolOp),
    Query4 = consolidate_exprs(Query3, []),
    {ok, consolidate_exprs(Query4, [])};
optimize(_, {error, {_, _, [_Message, [91, Error, 93]]}}, _) ->
    Err = [list_to_integer(M) || M <- Error,
                                 is_list(M)],
    throw({syntax_error, Err});
optimize(_, Error, _) ->
    throw(Error).

%% Internal functions
consolidate_exprs([], Acc) ->
    lists:reverse(Acc);
consolidate_exprs([{Op, [Term]}|T], Acc) when Op =:= land;
                                              Op =:= lor;
                                              Op =:= group ->
    case get_type(Term) of
        Type when Type =:= term;
                  Type =:= field ->
            consolidate_exprs(T, [Term|Acc]);
        _ ->
            consolidate_exprs(T, [{Op, [Term]}|Acc])
    end;
consolidate_exprs([{Op, Terms}|T], Acc) when is_atom(Op) ->
    NewTerms = consolidate_exprs(Terms, []),
    consolidate_exprs(T, [{Op, NewTerms}|Acc]);
consolidate_exprs([H|T], Acc) ->
    consolidate_exprs(T, [H|Acc]);
consolidate_exprs(Term, Acc) ->
    lists:reverse([Term|Acc]).

analyze_terms(AnalyzerPid, Query, Schema) ->
    analyze_terms(AnalyzerPid, Schema, Query, []).

analyze_terms(_AnalyzerPid, _Schema, [], Acc) ->
    lists:reverse(Acc);
analyze_terms(AnalyzerPid, Schema, [{Op, Terms}|T], Acc) when Op =:= land;
                                                              Op =:= lor;
                                                              Op =:= lnot;
                                                              Op =:= group ->
    case analyze_terms(AnalyzerPid, Schema, Terms, []) of
        [] ->
            analyze_terms(AnalyzerPid, Schema, T, Acc);
        NewTerms ->
            analyze_terms(AnalyzerPid, Schema, T, [{Op, NewTerms}|Acc])
    end;
analyze_terms(AnalyzerPid, Schema, [{field, FieldName, TermText, TProps}|T], Acc) when is_list(TermText) ->
    TermFun = fun(Text) -> {field, FieldName, Text, TProps} end,
    PhraseTermsFun = fun(Text) -> {field, FieldName, Text, proplists:delete(proximity, TProps)} end,
    PhraseFun = fun(BaseQuery) ->
                            {phrase, TermText,
                             BaseQuery,
                             add_proximity_terms(AnalyzerPid, TermText, TProps)} end,
    Funs = [{term, TermFun},
            {phrase_terms, PhraseTermsFun},
            {phrase, PhraseFun}],
    case analyze_term(AnalyzerPid, FieldName, Schema, Funs, TermText) of
        none ->
            analyze_terms(AnalyzerPid, Schema, T, Acc);
        NewTerm ->
            analyze_terms(AnalyzerPid, Schema, T, [NewTerm|Acc])
    end;
analyze_terms(AnalyzerPid, Schema, [{field, FieldName, {group, Group}}|T], Acc) ->
    NewGroup = analyze_terms(AnalyzerPid, Schema, Group, []),
    NewTerm = {field, FieldName, {group, NewGroup}},
    analyze_terms(AnalyzerPid, Schema, T, [NewTerm|Acc]);
analyze_terms(AnalyzerPid, Schema, [{field, FieldName,
                                     [{Range, {term, StartText, StartProps},
                                       {term, EndText, EndProps}}]}|T], Acc)
                                                         when Range =:= inclusive_range orelse
                                                              Range =:= exclusive_range ->
    StartTermFun = fun(Text) -> {term, Text, StartProps} end,
    StartPhraseT = fun(Text) -> {term, Text, proplists:delete(proximity, StartProps)} end,
    StartPhraseFun = fun(BaseQuery) ->
                             {phrase, StartText,
                              BaseQuery,
                              add_proximity_terms(AnalyzerPid, StartText, StartProps)} end,

    EndTermFun = fun(Text) -> {term, Text, EndProps} end,
    EndPhraseT = fun(Text) -> {term, Text, proplists:delete(proximity, EndProps)} end,
    EndPhraseFun = fun(BaseQuery) ->
                           {phrase, EndText,
                            BaseQuery,
                            add_proximity_terms(AnalyzerPid, EndText, EndProps)} end,

    SFuns = [{term, StartTermFun},
             {phrase_terms, StartPhraseT},
             {phrase, StartPhraseFun}],
    EFuns = [{term, EndTermFun},
             {phrase_terms, EndPhraseT},
             {phrase, EndPhraseFun}],
    NewStartTerm = analyze_term(AnalyzerPid, FieldName, Schema, SFuns, StartText),
    NewEndTerm = analyze_term(AnalyzerPid, FieldName, Schema, EFuns, EndText),
    case NewStartTerm =:= none orelse NewEndTerm =:= none of
        true ->
            analyze_terms(AnalyzerPid, Schema, T, Acc);
        false ->
            NewRange = {field, FieldName,
                        [{Range, NewStartTerm, NewEndTerm}]},
            analyze_terms(AnalyzerPid, Schema, T, [NewRange|Acc])
    end;

analyze_terms(AnalyzerPid, Schema, [{field, FieldName, FieldTerms, TProps}|T], Acc) ->
    [NewFieldTerms] = analyze_terms(AnalyzerPid, Schema, [FieldTerms], []),
    analyze_terms(AnalyzerPid, Schema, T, [{field, FieldName, NewFieldTerms, TProps}|Acc]);

analyze_terms(AnalyzerPid, Schema, [{term, TermText, TProps}|T], Acc) ->
    TermFun = fun(Text) -> {term, Text, TProps} end,
    PhraseTermsFun = fun(Text) -> {term, Text, proplists:delete(proximity, TProps)} end,
    PhraseFun = fun(BaseQuery) ->
                        {phrase, TermText,
                         BaseQuery,
                         add_proximity_terms(AnalyzerPid, TermText, TProps)} end,
    Funs = [{term, TermFun},
            {phrase_terms, PhraseTermsFun},
            {phrase, PhraseFun}],
    case analyze_term(AnalyzerPid, default, Schema, Funs, TermText) of
        none ->
            analyze_terms(AnalyzerPid, Schema, T, Acc);
        NewTerm ->
            analyze_terms(AnalyzerPid, Schema, T, [NewTerm|Acc])
    end;
analyze_terms(AnalyzerPid, Schema, [H|T], Acc) ->
    analyze_terms(AnalyzerPid, Schema, T, [H|Acc]).

analyze_term(AnalyzerPid, FieldName, Schema, TFuns, TermText) ->
    case string:chr(TermText, $\ ) > 0 andalso
        hd(TermText) =:= 34 of %% double quotes
        false ->
            TF = proplists:get_value(term, TFuns),
            case analyze_term_text(FieldName, AnalyzerPid, Schema, TermText) of
                {single, NewText} ->
                    TF(NewText);
                {multi, NewTexts} ->
                    {group, [{land,
                              [TF(NT) || NT <- NewTexts]}]};
                none ->
                    none
            end;
        true ->
            PTF = proplists:get_value(phrase_terms, TFuns),
            PF = proplists:get_value(phrase, TFuns),
            case analyze_term_text(FieldName, AnalyzerPid, Schema, TermText) of
                {single, NewText} ->
                    BaseQuery = PTF(NewText),
                    PF(BaseQuery);
                {multi, NewTexts} ->
                    BaseQuery = {land,
                                 [PTF(NT) || NT <- NewTexts]},
                    PF(BaseQuery);
                none ->
                    none
            end
    end.

default_bool([{term, _, _}=H|T], DefaultBoolOp) ->
    default_bool([{DefaultBoolOp, [H|T]}], DefaultBoolOp);
default_bool([{group, _}=H|T], DefaultBoolOp) when length(T) > 0 ->
    default_bool([{DefaultBoolOp, [H|T]}], DefaultBoolOp);
default_bool([{Op, SubTerms}|T], DefaultBoolOp) when Op =:= lnot;
                                                     Op =:= lor;
                                                     Op =:= land ->
    [{Op, default_bool_children(SubTerms, DefaultBoolOp)}|default_bool(T, DefaultBoolOp)];
default_bool([{field, FieldName, Terms}|T], DefaultBoolOp) ->
    [{field, FieldName, default_bool_children(Terms, DefaultBoolOp)}|
     default_bool(T, DefaultBoolOp)];
default_bool(Query, _DefaultBoolOp) ->
    Query.

default_bool_children([{group, SubTerms}=H|T], DefaultBoolOp) ->
    if
        length(SubTerms) < 2 ->
            [H|default_bool_children(T, DefaultBoolOp)];
        true ->
            [{group, {DefaultBoolOp, SubTerms}}|
             default_bool_children(T, DefaultBoolOp)]
    end;
default_bool_children([H|T], DefaultBoolOp) ->
    [H|default_bool_children(T, DefaultBoolOp)];
default_bool_children({group, [H|_]=SubTerms}, DefaultBoolOp) ->
    [H|_] = SubTerms,
    case get_type(H) of
        land ->
            {group, SubTerms};
        lor ->
            {group, SubTerms};
        _ ->
            {group, [{DefaultBoolOp, SubTerms}]}
    end;
default_bool_children(Query, _DefaultBoolOp) ->
    Query.

add_proximity_terms(AnalyzerPid, TermText, Props) ->
    case proplists:get_value(proximity, Props, undefined) of
        undefined ->
            Props;
        _ ->
            Terms = analyze_proximity_text(AnalyzerPid, TermText),
            [{proximity_terms, Terms}|Props]
    end.
%% All of these ifdefs are ugly, ugly hacks and need to be fixed via proper
%% app deps.
%% For testing only
-ifdef(TEST).
analyze_term_text(FieldName, testing, Schema, Text0) ->
    %% If FieldName == 'default', then pull the default field from the
    %% schema.
    case FieldName of
        default -> FieldName1 = Schema:default_field();
        _ -> FieldName1 = FieldName
    end,

    %% If this is a quoted value, then strip the quotes. Not using
    %% string:strip/N because there might be multiple quotes, and we
    %% only want to strip one set.
    case hd(Text0) == $" andalso lists:last(Text0) == $" of
        true ->
            Text = string:sub_string(Text0, 2, length(Text0) - 1);
        false ->
            Text = Text0
    end,

    %% Get the field....
    Field = Schema:find_field(FieldName1),
    PadSize = Schema:padding_size(Field),
    PadChar = Schema:padding_char(Field),

    %% Analyze the field...
    Tokens = [list_to_binary(lists:delete($*, Tok)) || Tok <- string:tokens(Text, " "),
                                                       length(Tok) > 2 andalso
                                                       Tok /= "the"],
    Tokens1 = [riak_search_text:left_pad(X, PadSize, PadChar) || X <- Tokens, X /= ""],
    case Tokens1 of
        [] ->      % None
            none;
        [Token] -> % One
            {single, Token};
        _ ->       % Many
            {multi, Tokens1}
    end.
analyze_proximity_text(testing, TermText) ->
    [list_to_binary(string:strip(T, both, 34)) || T <- string:tokens(TermText, " ")].
-endif.

-ifndef(TEST).
analyze_term_text(FieldName, AnalyzerPid, Schema, Text0) ->
    %% If FieldName == 'default', then pull the default field from the
    %% schema.
    case FieldName of
        default -> FieldName1 = Schema:default_field();
        _ -> FieldName1 = FieldName
    end,

    %% If this is a quoted value, then strip the quotes. Not using
    %% string:strip/N because there might be multiple quotes, and we
    %% only want to strip one set.
    case hd(Text0) == $" andalso lists:last(Text0) == $" of
        true ->
            Text = string:sub_string(Text0, 2, length(Text0) - 1);
        false ->
            Text = Text0
    end,

    %% Get the field....
    Field = Schema:find_field(FieldName1),
    AnalyzerFactory = Schema:analyzer_factory(Field),
    AnalyzerArgs = Schema:analyzer_args(Field),

    %% Analyze the field...
    {ok, Tokens} = qilr_analyzer:analyze(AnalyzerPid, Text, AnalyzerFactory, AnalyzerArgs),
    case Tokens of
        [] ->      % None
            none;
        [Token] -> % One
            {single, Token};
        _ ->       % Many
            {multi, Tokens}
    end.
analyze_proximity_text(AnalyzerPid, TermText) ->
    {ok, Terms0} = qilr_analyzer:analyze(AnalyzerPid, TermText, ?WHITESPACE_ANALYZER),
    [list_to_binary(string:strip(binary_to_list(T), both, 34)) ||
        T <- Terms0].
-endif.

get_type({land, _}) ->
    land;
get_type({lor, _}) ->
    lor;
get_type({lnot, _}) ->
    lnot;
get_type({group, _}) ->
    group;
get_type({field, _, _}) ->
    field;
get_type({field, _, _, _}) ->
    field;
get_type({phrase, _Phrase, _Ops}) ->
    phrase;
get_type({term, _, _}) ->
    term.
