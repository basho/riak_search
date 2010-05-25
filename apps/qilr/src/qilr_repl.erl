-module(qilr_repl).

-export([start/0]).

start() ->
    {ok, P} = qilr_analyzer_sup:new_analyzer(),
    read_query(P, []),
    exit(P, kill).

%% Internal functions
read_query(P, Accum0) ->
    Accum = Accum0 ++ read_line("qilr> "),
    case string:rstr(Accum, " \\") of
        0 ->
            case Accum =:= "exit" of
                true ->
                    io:format("~p exiting~n", [?MODULE]),
                    ok;
                false ->
                    io:format("~n~p~n", [qilr_parse:string(P, Accum)]),
                    read_query(P, [])
            end;
        Cont ->
            read_query(P, string:substr(Accum, 1, Cont))
    end.

read_line(Prompt) ->
    [_|Line] = lists:reverse(io:get_line(Prompt)),
    lists:reverse(Line).
