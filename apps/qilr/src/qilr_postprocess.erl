-module(qilr_postprocess).

-export([optimize/3]).

-include_lib("eunit/include/eunit.hrl").

-include("qilr.hrl").

optimize(AnalyzerPid, {ok, Query}, Opts) when not is_list(Query) ->
    optimize(AnalyzerPid, {ok, [Query]}, Opts);
optimize(AnalyzerPid, {ok, Query0}, Opts) when is_list(Query0) ->
    case process_terms(AnalyzerPid, Query0, Opts) of
        [] ->
            {error, no_terms};
        Query1 ->
            Query2 = consolidate_exprs(Query1, []),
            Query3 = default_bool(Query2, Opts),
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

process_terms(AnalyzerPid, Query, Opts) ->
    AnalyzerFactory = proplists:get_value(analyzer_factory, Opts),
    SchemaFields = proplists:get_value(schema_fields, Opts),
    analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, Query, []).

analyze_terms(_AnalyzerPid, _AnalyzerFactory, _SchemaFields, [], Acc) ->
    lists:reverse(Acc);
analyze_terms(AnalyzerPid, AnalyzerFactory,
              SchemaFields, [{Op, Terms}|T], Acc) when Op =:= land;
                                                       Op =:= lor;
                                                       Op =:= group ->
    case analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, Terms, []) of
        [] ->
            analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, T, Acc);
        NewTerms ->
            analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, T, [{Op, NewTerms}|Acc])
    end;
analyze_terms(AnalyzerPid, AnalyzerFactory,
              SchemaFields, [{field, FieldName, TermText, TProps}|T], Acc) ->
    NewTerm = case string:chr(TermText, $\ ) > 0 andalso
                  hd(TermText) =:= 34 of %% double quotes
                  false ->
                      case analyze_term_text(FieldName, AnalyzerPid, AnalyzerFactory, SchemaFields, TermText) of
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
                      case analyze_term_text(FieldName, AnalyzerPid, AnalyzerFactory, SchemaFields, TermText) of
                          {single, NewText} ->
                              BaseQuery = {field, FieldName, NewText, TProps},
                              {phrase, TermText, BaseQuery};
                          {multi, NewTexts} ->
                              BaseQuery = {land,
                                           [{field, FieldName, NT, TProps} ||
                                               NT <- NewTexts]},
                              {phrase, TermText, BaseQuery};
                          none ->
                              none
                      end
              end,
    case NewTerm of
        none ->
            analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, T, Acc);
        _ ->
            analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, T, [NewTerm|Acc])
    end;

analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, [{term, TermText, TProps}|T], Acc) ->
    NewTerm = case string:chr(TermText, $\ ) > 0 andalso
                  hd(TermText) =:= 34 of %% double quotes
                  false ->
                      case analyze_term_text(default, AnalyzerPid, AnalyzerFactory, SchemaFields, TermText) of
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
                      case analyze_term_text(default, AnalyzerPid, AnalyzerFactory, SchemaFields, TermText) of
                          {single, NewText} ->
                              BaseQuery = {term, NewText, TProps},
                              {phrase, TermText, [{base_query, BaseQuery}|TProps]};
                          {multi, NewTexts} ->
                              BaseQuery = {land,
                                           [{term, NT, TProps} ||
                                               NT <- NewTexts]},
                              {phrase, TermText, BaseQuery};
                          none ->
                              none
                      end
              end,
    case NewTerm of
        none ->
            analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, T, Acc);
        _ ->
            analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, T, [NewTerm|Acc])
    end;
analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, [H|T], Acc) ->
    analyze_terms(AnalyzerPid, AnalyzerFactory, SchemaFields, T, [H|Acc]).

default_bool([{term, _, _}=H|T], Opts) ->
    DefaultBool = proplists:get_value(default_bool, Opts),
    default_bool([{DefaultBool, [H|T]}], Opts);
default_bool([{group, _}=H|T], Opts) when length(T) > 0 ->
    DefaultBool = proplists:get_value(default_bool, Opts),
    default_bool([{DefaultBool, [H|T]}], Opts);
default_bool([{Bool, SubTerms}|T], Opts) when Bool =:= lnot;
                                              Bool =:= lor;
                                              Bool =:= land ->
    [{Bool, default_bool_children(SubTerms, Opts)}|default_bool(T, Opts)];
default_bool([{field, FieldName, Terms}|T], Opts) ->
    [{field, FieldName, default_bool_children(Terms, Opts)}|
     default_bool(T, Opts)];
default_bool(Query, _Opts) ->
    Query.

default_bool_children({group, SubTerms}, Opts) ->
    {group, default_bool(SubTerms, Opts)};
default_bool_children([{group, SubTerms}|T], Opts) ->
    [{group, default_bool(SubTerms, Opts)}|default_bool_children(T, Opts)];
default_bool_children([H|T], Opts) ->
    [H|default_bool_children(T, Opts)];
default_bool_children(Query, _Opts) ->
    Query.

analyze_term_text(FieldName, AnalyzerPid, AnalyzerFactory, SchemaFields, Text0) ->
    Start = hd(Text0),
    End = hd(lists:reverse(Text0)),
    Text = case Start == $" andalso End == $" of
               true ->
                   string:sub_string(Text0, 2, length(Text0) - 1);
               false ->
                   Text0
           end,
    Type = proplists:get_value(FieldName, SchemaFields),
    case qilr_analyzer:analyze(AnalyzerPid, Text,
                               determine_analyzer(AnalyzerFactory, Type)) of
        {ok, []} ->
            none;
        {ok, [Token]} ->
            {single, left_pad(Type, Token)};
        {ok, Tokens} ->
            case [left_pad(Type, Tok) || Tok <- Tokens,
                         not(Tok =:= "")] of
                [] ->
                    none;
                Toks ->
                    {multi, Toks}
            end
    end.

get_type({land, _}) ->
    land;
get_type({lor, _}) ->
    lor;
get_type({lnot, _}) ->
    lnot;
get_type({group, _}) ->
    group;
get_type({field, _, _, _}) ->
    field;
get_type({phrase, _Phrase, _Ops}) ->
    phrase;
get_type({term, _, _}) ->
    term.

left_pad(integer, Token) ->
    riak_search_text:left_pad(Token, 10);
left_pad(_, Token) ->
    Token.

determine_analyzer(_AnalyzerFactory, integer) ->
    ?WHITESPACE_ANALYZER;
determine_analyzer(_AnalyzerFactory, date) ->
    ?WHITESPACE_ANALYZER;
determine_analyzer(AnalyzerFactory, _) ->
    AnalyzerFactory.
