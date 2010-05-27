-module(qilr_repl).

-export([start/0]).

start() ->
    {ok, P} = qilr_analyzer_sup:new_analyzer(),
    F = fun qilr_parse:string/2,
    read_input(P, F, []),
    exit(P, shutdown).

%% Internal functions
read_input(P, Fun, Accum0) ->
    Accum = Accum0 ++ read_line("qilr> "),
    case string:rstr(Accum, " \\") of
        0 ->
            case Accum of
                "exit" ->
                    io:format("~p exiting~n", [?MODULE]),
                    ok;
                "g()" ->
                    F = fun(_, Text) ->
                                Graph = search:graph(Text),
                                io:format("~p~n", [Graph]) end,
                    io:format("ok~n"),
                    read_input(P, F, []);
                "s()" ->
                    F = fun qilr_parse:string/2,
                    io:format("ok~n"),
                    read_input(P, F, []);
                _ ->
                    io:format("~p~n", [Fun(P, Accum)]),
                    read_input(P, Fun, [])
            end;
        Cont ->
            read_input(P, Fun, string:substr(Accum, 1, Cont))
    end.

read_line(Prompt) ->
    [_|Line] = lists:reverse(io:get_line(Prompt)),
    lists:reverse(Line).
