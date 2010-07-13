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
    case analyze_terms(AnalyzerPid, Query0, Schema) of
        [] ->
            {error, no_terms};
        Query1 ->
            Query2 = consolidate_exprs(Query1, []),
            Query3 = default_bool(Query2, DefaultBoolOp),
            {ok, consolidate_exprs(Query3, [])}
    end;
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
                                                              Op =:= group ->
    case analyze_terms(AnalyzerPid, Schema, Terms, []) of
        [] ->
            analyze_terms(AnalyzerPid, Schema, T, Acc);
        NewTerms ->
            analyze_terms(AnalyzerPid, Schema, T, [{Op, NewTerms}|Acc])
    end;
analyze_terms(AnalyzerPid, Schema, [{field, FieldName, TermText, TProps}|T], Acc) ->
    NewTerm = case string:chr(TermText, $\ ) > 0 andalso
                  hd(TermText) =:= 34 of %% double quotes
                  false ->
                      case analyze_term_text(FieldName, AnalyzerPid, Schema, TermText) of
                          {single, NewText} ->
                              {field, FieldName, NewText, TProps};
                          {multi, NewTexts} ->
                              {group, [{land,
                                        [{field, FieldName, NT, TProps} ||
                                            NT <- NewTexts]}]};
                          none ->
                              none
                      end;
                  true ->
                      case analyze_term_text(FieldName, AnalyzerPid, Schema, TermText) of
                          {single, NewText} ->
                              BaseQuery = {field, FieldName, NewText, []},
                              {phrase, TermText, BaseQuery, add_proximity_terms(AnalyzerPid, TermText, TProps)};
                          {multi, NewTexts} ->
                              BaseQuery = {land,
                                           [{field, FieldName, NT, []} ||
                                               NT <- NewTexts]},
                              {phrase, TermText, BaseQuery, add_proximity_terms(AnalyzerPid, TermText, TProps)};
                          none ->
                              none
                      end
              end,
    case NewTerm of
        none ->
            analyze_terms(AnalyzerPid, Schema, T, Acc);
        _ ->
            analyze_terms(AnalyzerPid, Schema, T, [NewTerm|Acc])
    end;

analyze_terms(AnalyzerPid, Schema, [{term, TermText, TProps}|T], Acc) ->
    NewTerm = case string:chr(TermText, $\ ) > 0 andalso
                  hd(TermText) =:= 34 of %% double quotes
                  false ->
                      case analyze_term_text(default, AnalyzerPid, Schema, TermText) of
                          {single, NewText} ->
                              {term, NewText, TProps};
                          {multi, NewTexts} ->
                               {group, [{land,
                                        [{term, NT, TProps} ||
                                            NT <- NewTexts]}]};
                          none ->
                              none
                      end;
                  true ->
                      case analyze_term_text(default, AnalyzerPid, Schema, TermText) of
                          {single, NewText} ->
                              BaseQuery = {term, NewText, []},
                              {phrase, TermText, BaseQuery, add_proximity_terms(AnalyzerPid, TermText, TProps)};
                          {multi, NewTexts} ->
                              BaseQuery = {land,
                                           [{term, NT, []} ||
                                               NT <- NewTexts]},
                              {phrase, TermText, BaseQuery,
                               add_proximity_terms(AnalyzerPid, TermText, TProps)};
                          none ->
                              none
                      end
              end,
    case NewTerm of
        none ->
            analyze_terms(AnalyzerPid, Schema, T, Acc);
        _ ->
            analyze_terms(AnalyzerPid, Schema, T, [NewTerm|Acc])
    end;
analyze_terms(AnalyzerPid, Schema, [H|T], Acc) ->
    analyze_terms(AnalyzerPid, Schema, T, [H|Acc]).

default_bool([{term, _, _}=H|T], DefaultBoolOp) ->
    default_bool([{DefaultBoolOp, [H|T]}], DefaultBoolOp);
default_bool([{group, _}=H|T], DefaultBoolOp) when length(T) > 0 ->
    default_bool([{DefaultBoolOp, [H|T]}], DefaultBoolOp);
default_bool([{Bool, SubTerms}|T], DefaultBoolOp) when Bool =:= lnot;
                                              Bool =:= lor;
                                              Bool =:= land ->
    [{Bool, default_bool_children(SubTerms, DefaultBoolOp)}|default_bool(T, DefaultBoolOp)];
default_bool([{field, FieldName, Terms}|T], DefaultBoolOp) ->
    [{field, FieldName, default_bool_children(Terms, DefaultBoolOp)}|
     default_bool(T, DefaultBoolOp)];
default_bool(Query, _DefaultBoolOp) ->
    Query.

default_bool_children({group, SubTerms}, DefaultBoolOp) ->
    {group, default_bool(SubTerms, DefaultBoolOp)};
default_bool_children([{group, SubTerms}|T], DefaultBoolOp) ->
    [{group, default_bool(SubTerms, DefaultBoolOp)}|default_bool_children(T, DefaultBoolOp)];
default_bool_children([H|T], DefaultBoolOp) ->
    [H|default_bool_children(T, DefaultBoolOp)];
default_bool_children(Query, _DefaultBoolOp) ->
    Query.

add_proximity_terms(AnalyzerPid, TermText, Props) ->
    case proplists:get_value(proximity, Props, undefined) of
        undefined ->
            Props;
        _ ->
            {ok, Terms0} = qilr_analyzer:analyze(AnalyzerPid, TermText, ?WHITESPACE_ANALYZER),
            Terms = [list_to_binary(string:strip(binary_to_list(T), both, 34)) ||
                        T <- Terms0],
            [{proximity_terms, Terms}|Props]
    end.

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
    PadSize = Schema:padding_size(Field),
    PadChar = Schema:padding_char(Field),

    %% Analyze the field...
    {ok, Tokens} = qilr_analyzer:analyze(AnalyzerPid, Text, AnalyzerFactory),
    Tokens1 = [riak_search_text:left_pad(X, PadSize, PadChar) || X <- Tokens, X /= ""],
    case Tokens1 of
        [] ->      % None
            none;
        [Token] -> % One
            {single, Token};
        _ ->       % Many
            {multi, Tokens1}
    end.
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
get_type({phrase, _Phrase, _Ops}) ->
    phrase;
get_type({term, _, _}) ->
    term.
