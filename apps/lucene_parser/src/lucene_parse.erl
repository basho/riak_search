-module(lucene_parse).
-export([parse/1, parse_and_scan/1, format_error/1]).
-file("src/lucene_parse.yrl", 109).

-include("lucene_parser.hrl").

val({_, _, V}) -> V.

split_scope(S) ->
    case string:chr(S, $.) of
        0 -> {undefined, S};
        N -> 
            {A, B} = lists:split(N-1, S),
            {A, tl(B)}
    end.

unwrap(Type, [Rec]) ->
    unwrap(Type, Rec);
unwrap(intersection, Rec = #intersection {}) ->
    Rec#intersection.ops;
unwrap(union, Rec = #union {}) ->
    Rec#union.ops;
unwrap(_, Rec) ->
    Rec.
    

-file("/usr/local/lib/erlang/lib/parsetools-2.0.2/include/yeccpre.hrl", 0).
%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2010. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% The parser generator will insert appropriate declarations before this line.%

-type yecc_ret() :: {'error', _} | {'ok', _}.

-spec parse(Tokens :: list()) -> yecc_ret().
parse(Tokens) ->
    yeccpars0(Tokens, {no_func, no_line}, 0, [], []).

-spec parse_and_scan({function() | {atom(), atom()}, [_]} | {atom(), atom(), [_]}) ->
            yecc_ret().
parse_and_scan({F, A}) -> % Fun or {M, F}
    yeccpars0([], {{F, A}, no_line}, 0, [], []);
parse_and_scan({M, F, A}) ->
    yeccpars0([], {{{M, F}, A}, no_line}, 0, [], []).

-spec format_error(any()) -> [char() | list()].
format_error(Message) ->
    case io_lib:deep_char_list(Message) of
        true ->
            Message;
        _ ->
            io_lib:write(Message)
    end.

%% To be used in grammar files to throw an error message to the parser
%% toplevel. Doesn't have to be exported!
-compile({nowarn_unused_function,{return_error,2}}).
-spec return_error(integer(), any()) -> no_return().
return_error(Line, Message) ->
    throw({error, {Line, ?MODULE, Message}}).

-define(CODE_VERSION, "1.4").

yeccpars0(Tokens, Tzr, State, States, Vstack) ->
    try yeccpars1(Tokens, Tzr, State, States, Vstack)
    catch 
        error: Error ->
            Stacktrace = erlang:get_stacktrace(),
            try yecc_error_type(Error, Stacktrace) of
                {syntax_error, Token} ->
                    yeccerror(Token);
                {missing_in_goto_table=Tag, Symbol, State} ->
                    Desc = {Symbol, State, Tag},
                    erlang:raise(error, {yecc_bug, ?CODE_VERSION, Desc},
                                 Stacktrace)
            catch _:_ -> erlang:raise(error, Error, Stacktrace)
            end;
        %% Probably thrown from return_error/2:
        throw: {error, {_Line, ?MODULE, _M}} = Error ->
            Error
    end.

yecc_error_type(function_clause, [{?MODULE,F,[State,_,_,_,Token,_,_]} | _]) ->
    case atom_to_list(F) of
        "yeccpars2" ++ _ ->
            {syntax_error, Token};
        "yeccgoto_" ++ SymbolL ->
            {ok,[{atom,_,Symbol}],_} = erl_scan:string(SymbolL),
            {missing_in_goto_table, Symbol, State}
    end.

yeccpars1([Token | Tokens], Tzr, State, States, Vstack) ->
    yeccpars2(State, element(1, Token), States, Vstack, Token, Tokens, Tzr);
yeccpars1([], {{F, A},_Line}, State, States, Vstack) ->
    case apply(F, A) of
        {ok, Tokens, Endline} ->
            yeccpars1(Tokens, {{F, A}, Endline}, State, States, Vstack);
        {eof, Endline} ->
            yeccpars1([], {no_func, Endline}, State, States, Vstack);
        {error, Descriptor, _Endline} ->
            {error, Descriptor}
    end;
yeccpars1([], {no_func, no_line}, State, States, Vstack) ->
    Line = 999999,
    yeccpars2(State, '$end', States, Vstack, yecc_end(Line), [],
              {no_func, Line});
yeccpars1([], {no_func, Endline}, State, States, Vstack) ->
    yeccpars2(State, '$end', States, Vstack, yecc_end(Endline), [],
              {no_func, Endline}).

%% yeccpars1/7 is called from generated code.
%%
%% When using the {includefile, Includefile} option, make sure that
%% yeccpars1/7 can be found by parsing the file without following
%% include directives. yecc will otherwise assume that an old
%% yeccpre.hrl is included (one which defines yeccpars1/5).
yeccpars1(State1, State, States, Vstack, Token0, [Token | Tokens], Tzr) ->
    yeccpars2(State, element(1, Token), [State1 | States],
              [Token0 | Vstack], Token, Tokens, Tzr);
yeccpars1(State1, State, States, Vstack, Token0, [], {{_F,_A}, _Line}=Tzr) ->
    yeccpars1([], Tzr, State, [State1 | States], [Token0 | Vstack]);
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, no_line}) ->
    Line = yecctoken_end_location(Token0),
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line});
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, Line}) ->
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line}).

%% For internal use only.
yecc_end({Line,_Column}) ->
    {'$end', Line};
yecc_end(Line) ->
    {'$end', Line}.

yecctoken_end_location(Token) ->
    try
        {text, Str} = erl_scan:token_info(Token, text),
        {line, Line} = erl_scan:token_info(Token, line),
        Parts = re:split(Str, "\n"),
        Dline = length(Parts) - 1,
        Yline = Line + Dline,
        case erl_scan:token_info(Token, column) of
            {column, Column} ->
                Col = byte_size(lists:last(Parts)),
                {Yline, Col + if Dline =:= 0 -> Column; true -> 1 end};
            undefined ->
                Yline
        end
    catch _:_ ->
        yecctoken_location(Token)
    end.

yeccerror(Token) ->
    Text = yecctoken_to_string(Token),
    Location = yecctoken_location(Token),
    {error, {Location, ?MODULE, ["syntax error before: ", Text]}}.

yecctoken_to_string(Token) ->
    case catch erl_scan:token_info(Token, text) of
        {text, Txt} -> Txt;
        _ -> yecctoken2string(Token)
    end.

yecctoken_location(Token) ->
    case catch erl_scan:token_info(Token, location) of
        {location, Loc} -> Loc;
        _ -> element(2, Token)
    end.

yecctoken2string({atom, _, A}) -> io_lib:write(A);
yecctoken2string({integer,_,N}) -> io_lib:write(N);
yecctoken2string({float,_,F}) -> io_lib:write(F);
yecctoken2string({char,_,C}) -> io_lib:write_char(C);
yecctoken2string({var,_,V}) -> io_lib:format("~s", [V]);
yecctoken2string({string,_,S}) -> io_lib:write_unicode_string(S);
yecctoken2string({reserved_symbol, _, A}) -> io_lib:write(A);
yecctoken2string({_Cat, _, Val}) -> io_lib:write(Val);
yecctoken2string({dot, _}) -> "'.'";
yecctoken2string({'$end', _}) ->
    [];
yecctoken2string({Other, _}) when is_atom(Other) ->
    io_lib:write(Other);
yecctoken2string(Other) ->
    io_lib:write(Other).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



-file("src/lucene_parse.erl", 208).

yeccpars2(0=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(1=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_1(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(2=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_2(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(3=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_3(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(4=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(5=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_5(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(6=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(7=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(8=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_8(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(9=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_9(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(10=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_10(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(11=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_11(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(12=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_12(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(13=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(14=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_14(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(15=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(16=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(17=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_17(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(18=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_18(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(19=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(20=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_20(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(21=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_21(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(22=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_22(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(23=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(24=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_24(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(25=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(26=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_26(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(27=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_27(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(28=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_28(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(29=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_29(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(30=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_30(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(31=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(32=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_32(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(33=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_33(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(34=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_34(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(35=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_35(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(36=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_36(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(Other, _, _, _, _, _, _) ->
 erlang:error({yecc_bug,"1.3",{missing_state_in_action_table, Other}}).

yeccpars2_0(S, exclusive_start, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 3, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, group_start, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 4, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, inclusive_start, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 5, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, negation, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 6, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, prohibited, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 7, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, required, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 8, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 9, Ss, Stack, T, Ts, Tzr).

yeccpars2_1(S, intersection, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_1(S, union, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 16, Ss, Stack, T, Ts, Tzr);
yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_1_(Stack),
 yeccgoto_queries(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_2(_S, '$end', _Ss, Stack,  _T, _Ts, _Tzr) ->
 {ok, hd(Stack)};
yeccpars2_2(S, exclusive_start, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 3, Ss, Stack, T, Ts, Tzr);
yeccpars2_2(S, group_start, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 4, Ss, Stack, T, Ts, Tzr);
yeccpars2_2(S, inclusive_start, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 5, Ss, Stack, T, Ts, Tzr);
yeccpars2_2(S, negation, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 6, Ss, Stack, T, Ts, Tzr);
yeccpars2_2(S, prohibited, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 7, Ss, Stack, T, Ts, Tzr);
yeccpars2_2(S, required, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 8, Ss, Stack, T, Ts, Tzr);
yeccpars2_2(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 9, Ss, Stack, T, Ts, Tzr).

yeccpars2_3(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 32, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_4: see yeccpars2_0

yeccpars2_5(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 24, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_6: see yeccpars2_0

%% yeccpars2_7: see yeccpars2_0

yeccpars2_8(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr).

yeccpars2_9(S, boost, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 10, Ss, Stack, T, Ts, Tzr);
yeccpars2_9(S, fuzzy, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 11, Ss, Stack, T, Ts, Tzr);
yeccpars2_9(S, proximity, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 12, Ss, Stack, T, Ts, Tzr);
yeccpars2_9(S, scope, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 13, Ss, Stack, T, Ts, Tzr);
yeccpars2_9(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_9_(Stack),
 'yeccgoto_\'query\''(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_10(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_10_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_11_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_12_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_13: see yeccpars2_0

yeccpars2_14(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_14_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_15: see yeccpars2_0

%% yeccpars2_16: see yeccpars2_0

yeccpars2_17(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_17_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_18(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_18_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_19(S, scope, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 20, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_19_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_20(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 21, Ss, Stack, T, Ts, Tzr).

yeccpars2_21(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_21_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_22(S, intersection, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_22(S, union, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 16, Ss, Stack, T, Ts, Tzr);
yeccpars2_22(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_22_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_23(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_23_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_24(S, range_to, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 25, Ss, Stack, T, Ts, Tzr).

yeccpars2_25(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 26, Ss, Stack, T, Ts, Tzr).

yeccpars2_26(S, exclusive_end, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 27, Ss, Stack, T, Ts, Tzr);
yeccpars2_26(S, inclusive_end, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 28, Ss, Stack, T, Ts, Tzr).

yeccpars2_27(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_27_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_28(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_28_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_29(S, group_end, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 31, Ss, Stack, T, Ts, Tzr);
yeccpars2_29(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_30(S, intersection, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(S, union, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 16, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_30_(Stack),
 yeccgoto_queries(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_31(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_31_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_32(S, range_to, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 33, Ss, Stack, T, Ts, Tzr).

yeccpars2_33(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 34, Ss, Stack, T, Ts, Tzr).

yeccpars2_34(S, exclusive_end, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 35, Ss, Stack, T, Ts, Tzr);
yeccpars2_34(S, inclusive_end, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 36, Ss, Stack, T, Ts, Tzr).

yeccpars2_35(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_35_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_36(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_36_(Stack),
 'yeccgoto_\'query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccgoto_queries(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_2(2, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_queries(4, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_29(29, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'query\''(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_1(1, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(2, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_30(30, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(4, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_1(1, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(6=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(7, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_22(22, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(13=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_14(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(15=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_18(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(16=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_17(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'query\''(29, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_30(30, Cat, Ss, Stack, T, Ts, Tzr).

-compile({inline,yeccpars2_1_/1}).
-file("src/lucene_parse.yrl", 34).
yeccpars2_1_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_9_/1}).
-file("src/lucene_parse.yrl", 103).
yeccpars2_9_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # string { s = val ( __1 ) }
  end | __Stack].

-compile({inline,yeccpars2_10_/1}).
-file("src/lucene_parse.yrl", 95).
yeccpars2_10_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # string { s = val ( __1 ) , flags = [ { boost , val ( __2 ) } ] }
  end | __Stack].

-compile({inline,yeccpars2_11_/1}).
-file("src/lucene_parse.yrl", 87).
yeccpars2_11_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # string { s = val ( __1 ) , flags = [ { fuzzy , val ( __2 ) } ] }
  end | __Stack].

-compile({inline,yeccpars2_12_/1}).
-file("src/lucene_parse.yrl", 91).
yeccpars2_12_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # string { s = val ( __1 ) , flags = [ { proximity , val ( __2 ) } ] }
  end | __Stack].

-compile({inline,yeccpars2_14_/1}).
-file("src/lucene_parse.yrl", 99).
yeccpars2_14_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { Index , Field } = split_scope ( val ( __1 ) ) ,
    # scope { index = Index , field = Field , ops = [ __3 ] }
  end | __Stack].

-compile({inline,yeccpars2_17_/1}).
-file("src/lucene_parse.yrl", 65).
yeccpars2_17_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   Ops = lists : flatten ( [ unwrap ( union , __1 ) , unwrap ( union , __3 ) ] ) ,
    # union { ops = Ops }
  end | __Stack].

-compile({inline,yeccpars2_18_/1}).
-file("src/lucene_parse.yrl", 61).
yeccpars2_18_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   Ops = lists : flatten ( [ unwrap ( intersection , __1 ) , unwrap ( intersection , __3 ) ] ) ,
    # intersection { ops = Ops }
  end | __Stack].

-compile({inline,yeccpars2_19_/1}).
-file("src/lucene_parse.yrl", 48).
yeccpars2_19_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # string { s = val ( __2 ) , flags = [ required ] }
  end | __Stack].

-compile({inline,yeccpars2_21_/1}).
-file("src/lucene_parse.yrl", 51).
yeccpars2_21_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { Index , Field } = split_scope ( val ( __2 ) ) ,
    TermOp = # string { s = val ( __4 ) , flags = [ required ] } ,
    # scope { index = Index , field = Field , ops = [ TermOp ] }
  end | __Stack].

-compile({inline,yeccpars2_22_/1}).
-file("src/lucene_parse.yrl", 57).
yeccpars2_22_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # negation { op = __2 }
  end | __Stack].

-compile({inline,yeccpars2_23_/1}).
-file("src/lucene_parse.yrl", 83).
yeccpars2_23_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # negation { op = __2 }
  end | __Stack].

-compile({inline,yeccpars2_27_/1}).
-file("src/lucene_parse.yrl", 76).
yeccpars2_27_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # range { from = { inclusive , val ( __2 ) } , to = { exclusive , val ( __4 ) } }
  end | __Stack].

-compile({inline,yeccpars2_28_/1}).
-file("src/lucene_parse.yrl", 70).
yeccpars2_28_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # range { from = { inclusive , val ( __2 ) } , to = { inclusive , val ( __4 ) } }
  end | __Stack].

-compile({inline,yeccpars2_30_/1}).
-file("src/lucene_parse.yrl", 37).
yeccpars2_30_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   __1 ++ [ __2 ]
  end | __Stack].

-compile({inline,yeccpars2_31_/1}).
-file("src/lucene_parse.yrl", 41).
yeccpars2_31_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   case __2 of
    [ Rec ] -> Rec ;
    Recs -> # group { ops = Recs }
    end
  end | __Stack].

-compile({inline,yeccpars2_35_/1}).
-file("src/lucene_parse.yrl", 79).
yeccpars2_35_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # range { from = { exclusive , val ( __2 ) } , to = { exclusive , val ( __4 ) } }
  end | __Stack].

-compile({inline,yeccpars2_36_/1}).
-file("src/lucene_parse.yrl", 73).
yeccpars2_36_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # range { from = { exclusive , val ( __2 ) } , to = { inclusive , val ( __4 ) } }
  end | __Stack].


-file("src/lucene_parse.yrl", 132).
