-module(qilr_optimizer).

-export([optimize/2]).

-include_lib("eunit/include/eunit.hrl").

optimize({ok, Query}, Opts) when not is_list(Query) ->
    optimize({ok, [Query]}, Opts);
optimize({ok, Query0}, Opts) ->
    Debug = proplists:get_value(debug, Opts, false),
    log(Debug, "Initial query: ~p~n", [Query0]),
    case process_terms(Query0, Opts) of
        [] ->
            log(Debug, "After analysis: ~p~n", [{error, no_terms}]),
            {error, no_terms};
        Query1 ->
            log(Debug, "After analysis: ~p~n", [Query1]),
            Query2 = default_bool(Query1, Opts),
            log(Debug, "After adding default boolean op: ~p~n", [Query2]),
            {ok, Query2}
    end.

%% Internal functions
process_terms(Query, _Opts) ->
  analyze_terms(Query, []).

analyze_terms([], Acc) ->
  lists:reverse(Acc);
analyze_terms([{field, FieldName, TermText, TProps}|T], Acc) ->
    NewTerm = case analyze_term_text(TermText) of
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
            analyze_terms(T, Acc);
        _ ->
            analyze_terms(T, [NewTerm|Acc])
    end;
analyze_terms([{term, TermText, TProps}|T], Acc) ->
    NewTerm = case analyze_term_text(TermText) of
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
            analyze_terms(T, Acc);
        _ ->
            analyze_terms(T, [NewTerm|Acc])
    end;
analyze_terms([H|T], Acc) ->
    analyze_terms(T, [H|Acc]).

default_bool([H|T], Opts) when not(is_list(T)) ->
    DefaultBool = proplists:get_value(default_bool, Opts, lor),
    [{DefaultBool, H ++ [T]}];
default_bool([{Type, _, _}=H|T], Opts) ->
    DefaultBool = proplists:get_value(default_bool, Opts, lor),
    case needs_implicit_bool(Type, T) of
        true ->
            [{DefaultBool, [H|T]}];
        false ->
            [H|T]
    end;
default_bool(Query, _Opts) ->
    Query.

needs_implicit_bool(term, T) when length(T) > 0 ->
    true;
needs_implicit_bool(group, T) when length(T) > 0 ->
    true;
needs_implicit_bool(lnot, T) when length(T) > 0 ->
    true;
needs_implicit_bool(lor, T) when length(T) > 0 ->
    true;
needs_implicit_bool(land, T) when length(T) > 0 ->
    true;
needs_implicit_bool(_, _) ->
    false.

analyze_term_text(Text0) ->
    Start = hd(Text0),
    End = hd(lists:reverse(Text0)),
    Text = case Start == $" andalso End == $" of
               true ->
                   string:sub_string(Text0, 2, length(Text0) - 1);
               false ->
                   Text0
           end,
    case qilr_analyzer:analyze(list_to_binary(Text)) of
        {ok, <<"">>} ->
            none;
        {ok, Token} when is_binary(Token) ->
            {single, binary_to_list(Token)};
        {ok, Tokens} when is_list(Tokens) ->
            case [Tok || Tok <- Tokens,
                         not(Tok =:= <<"">>)] of
                [] ->
                    none;
                Toks ->
                    {multi, Toks}
            end
    end.

log(false, _, _) ->
    ok;
log(true, Format, Args) ->
    error_log:info_msg(Format, Args).
