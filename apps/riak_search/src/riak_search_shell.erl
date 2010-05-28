-module(riak_search_shell).

-export([start/0, search/2, parse/2, graph/2]).

-record(state, {client,
                index,
                analyzer,
                handler}).

start(Index) ->
    {ok, P} = qilr_analyzer_sup:new_analyzer(),
    {ok, Client} = riak_search:local_client(),
    read_input(#state{client=Client,
                      index=Index,
                      analyzer=P,
                      handler=fun riak_search_shell:search/2}, []),
    qilr_analyzer:close(P).

start() ->
    start("search").

search(Query, #state{index=Index}) ->
    Start = erlang:now(),
    R = search:search(Index, Query),
    End = erlang:now(),
    io:format("Query took ~pms~n", [erlang:trunc(timer:now_diff(End, Start) / 1000)]),
    case R of
        {error, Error} ->
            io:format("Error: ~p~n", [Error]);
        {_, Results} when length(Results) == 0 ->
            io:format("No records found~n");
        {_, Results} ->
            io:format("Found ~p records:~n", [length(Results)]),
            [io:format("~p~n", [Result]) || Result <- Results]
    end.

parse(Query, #state{analyzer=Analyzer}) ->
    io:format("~p~n", [qilr_parse:string(Analyzer, Query)]).

graph(Query, #state{client=Client, index=Index}) ->
    case Client:parse_query(Query) of
        {ok, AST} ->
            io:format("~p~n", [Client:query_as_graph(Client:explain(Index, AST))]);
        Error->
            io:format("Error: ~p~n", [Error])
    end.

%% Internal functions
read_input(#state{handler=Handler}=State, Accum0) ->
    Accum = Accum0 ++ read_line("riak_search> "),
    case string:rstr(Accum, " \\") of
        0 ->
            case Accum of
                "q()" ->
                    io:format("Exiting shell...~n"),
                    ok;
                "g()" ->
                    read_input(State#state{handler=fun riak_search_shell:graph/2}, []);
                "p()" ->
                    read_input(State#state{handler=fun riak_search_shell:parse/2}, []);
                "s()" ->
                    read_input(State#state{handler=fun riak_search_shell:search/2}, []);
                "h()" ->
                    print_help(),
                    read_input(State, []);
                "i()" ->
                    print_info(State),
                    read_input(State, []);
                _ ->
                    case catch Handler(Accum, State) of
                        Error ->
                            io:format("~p~n", [Error])
                    end,
                    read_input(State, [])
            end;
        Cont ->
            read_input(State, string:substr(Accum, 1, Cont))
    end.

print_help() ->
    Help = "q(): Exit shell~n" ++
           "g(): Parse query and print op graph~n" ++
           "p(): Parse query and print AST~n" ++
           "s(): Execute query and print results~n" ++
           "i(): Print basic shell environment information~n" ++
           "h(): Print this help~n~n",
    io:format(Help).

print_info(#state{index=Index, handler=Handler}) ->
    G = fun riak_search_shell:graph/2,
    P = fun riak_search_shell:parse/2,
    S = fun riak_search_shell:search/2,
    Mode = case Handler of
               G ->
                   graph;
               P ->
                   parse;
               S ->
                   search
           end,
    io:format("Index: ~p~nMode: ~p~n", [Index, Mode]).

read_line(Prompt) ->
    [_|Line] = lists:reverse(io:get_line(Prompt)),
    lists:reverse(Line).
