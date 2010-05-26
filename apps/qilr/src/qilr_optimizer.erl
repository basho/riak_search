-module(qilr_optimizer).

-export([optimize/3]).

-include_lib("eunit/include/eunit.hrl").

optimize(AnalyzerPid, {ok, Query}, Opts) when not is_list(Query) ->
    optimize(AnalyzerPid, {ok, [Query]}, Opts);
optimize(AnalyzerPid, {ok, Query0}, Opts) when is_list(Query0) ->
    %io:format("~n~nPass 0: ~p~n", [Query0]),
    case process_terms(AnalyzerPid, Query0, Opts) of
        [] ->
            {error, no_terms};
        Query1 ->
            %io:format("~n~nPass 1: ~p~n", [Query1]),
            Query2 = consolidate_exprs(Query1, []),
            %io:format("~n~nPass 2: ~p~n", [Query2]),
            Query3 = default_bool(Query2, Opts),
            %io:format("~n~nPass 3: ~p~n", [Query3]),
            Query4 = consolidate_exprs(Query3, []),
            %io:format("~n~nFinal Pass: ~p~n", [Query4]),
            {ok, Query4}
    end;
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

process_terms(AnalyzerPid, Query, _Opts) ->
  analyze_terms(AnalyzerPid, Query, []).

analyze_terms(_AnalyzerPid, [], Acc) ->
  lists:reverse(Acc);
analyze_terms(AnalyzerPid, [{Op, Terms}|T], Acc) when Op =:= land;
                                                      Op =:= lor;
                                                      Op =:= group ->
    case analyze_terms(AnalyzerPid, Terms, []) of
        [] ->
            analyze_terms(AnalyzerPid, T, Acc);
        NewTerms ->
            analyze_terms(AnalyzerPid, T, [{Op, NewTerms}|Acc])
    end;
analyze_terms(AnalyzerPid, [{field, FieldName, TermText, TProps}|T], Acc) ->
    NewTerm = case analyze_term_text(AnalyzerPid, TermText) of
                  {single, NewText} ->
                      {field, FieldName, NewText, TProps};
                  {multi, NewTexts} ->
                      {group, [{land,
                                [{field, FieldName, NT, TProps} ||
                                    NT <- NewTexts]}]};
                  none ->
                      none
              end,
    case NewTerm of
        none ->
            analyze_terms(AnalyzerPid, T, Acc);
        _ ->
            analyze_terms(AnalyzerPid, T, [NewTerm|Acc])
    end;
analyze_terms(AnalyzerPid, [{term, TermText, TProps}|T], Acc) ->
    NewTerm = case analyze_term_text(AnalyzerPid, TermText) of
                  {single, NewText} ->
                      {term, NewText, TProps};
                  {multi, NewTexts} ->
                      {group, [{land,
                                [{term, NT, TProps} || NT <- NewTexts]}]};
                  none ->
                      none
              end,
    case NewTerm of
        none ->
            analyze_terms(AnalyzerPid, T, Acc);
        _ ->
            analyze_terms(AnalyzerPid, T, [NewTerm|Acc])
    end;
analyze_terms(AnalyzerPid, [H|T], Acc) ->
    analyze_terms(AnalyzerPid, T, [H|Acc]).

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
default_bool(Query, _Opts) ->
    Query.

default_bool_children([{group, SubTerms}|T], Opts) ->
    [{group, default_bool(SubTerms, Opts)}|default_bool_children(T, Opts)];
default_bool_children([H|T], Opts) ->
    [H|default_bool_children(T, Opts)];
default_bool_children(Query, _Opts) ->
    Query.

%% needs_implicit_bool(term, T) when length(T) > 0 ->
%%     true;
%% needs_implicit_bool(group, T) when length(T) > 0 ->
%%     true;
%% needs_implicit_bool(lnot, T) when length(T) > 0 ->
%%     true;
%% needs_implicit_bool(lor, T) when length(T) > 0 ->
%%     true;
%% needs_implicit_bool(land, T) when length(T) > 0 ->
%%     true;
%% needs_implicit_bool(_, _) ->
%%     false.

analyze_term_text(AnalyzerPid, Text0) ->
    Start = hd(Text0),
    End = hd(lists:reverse(Text0)),
    Text = case Start == $" andalso End == $" of
               true ->
                   string:sub_string(Text0, 2, length(Text0) - 1);
               false ->
                   Text0
           end,
    case qilr_analyzer:analyze(AnalyzerPid, Text) of
        {ok, []} ->
            none;
        {ok, [Token]} ->
            {single, Token};
        {ok, Tokens} ->
            case [Tok || Tok <- Tokens,
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
get_type({term, _, _}) ->
    term.
