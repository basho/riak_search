%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(qilr_analyzer).

%% API
-export([analyze/1, analyze/2, analyze/3]).

%% RPZ Do I need analyze/1 and 2?
analyze(Text) when is_list(Text) ->
    case analyze(list_to_binary(Text)) of
        {ok, Tokens} ->
            {ok, [binary_to_list(Token) || Token <- Tokens]};
        Error ->
            Error
    end;
analyze(Text) when is_binary(Text) ->
    analyze(Text, undefined).

analyze(Text, AnalyzerFactory) when is_list(Text) ->
    analyze(list_to_binary(Text), AnalyzerFactory);
analyze(Text, AnalyzerFactory) ->
    analyze(Text, AnalyzerFactory, undefined).


%% No analyzer is defined. Throw an exception.
analyze(_Text, undefined, _AnalyzerArgs) ->
    error_logger:error_msg("The analyzer_factory setting is not set. Check your schema.~n"),
    throw({schema_setting_required, analyzer_factory});

%% Handle Erlang-based AnalyzerFactory. Can either be of the form
%% {erlang, Mod, Fun} or {erlang, Mod, Fun, Args}. Function should
%% return {ok, Terms} where Terms is a list of Erlang binaries.
analyze(Text, {erlang, Mod, Fun, AnalyzerArgs}, _) ->
    Mod:Fun(Text, AnalyzerArgs);
analyze(Text, {erlang, Mod, Fun}, AnalyzerArgs) ->
    Mod:Fun(Text, AnalyzerArgs).
