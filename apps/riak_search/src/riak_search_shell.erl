-module(riak_search_shell).

-export([start/0, search/2, parse/2, graph/2]).

-record(state, {client,
                index,
                default_field,
                analyzer,
                handler}).

start(Index, DefaultField) ->
    {ok, P} = qilr_analyzer_sup:new_analyzer(),
    {ok, Client} = riak_search:local_client(),
    read_input(#state{client=Client,
                      index=Index,
                      default_field=DefaultField,
                      analyzer=P,
                      handler=fun riak_search_shell:search/2}, []),
    exit(P, shutdown).

start() ->
    start("search", "payload").

search(Query, #state{client=Client, index=Index, default_field=DefaultField}) ->
    Start = erlang:now(),
    R = Client:search(Index, DefaultField, Query),
    End = erlang:now(),
    io:format("Query took ~pms~n", [erlang:trunc(timer:now_diff(End, Start) / 1000)]),
    case R of
        {error, Error} ->
            io:format("Error: ~p~n", [Error]);
        Results when is_list(Results) ->
            io:format("Found ~p records:~n", [length(Results)]),
            [io:format("~p~n", [Result]) || Result <- Results]
    end.

parse(Query, #state{analyzer=Analyzer}) ->
    io:format("~p~n", [qilr_parse:string(Analyzer, Query)]).

graph(Query, #state{client=Client, index=Index, default_field=DefaultField}) ->
    io:format("~p~n", [Client:query_as_graph(Client:explain(Index, DefaultField, Query))]).

%% Internal functions
read_input(#state{handler=Handler}=State, Accum0) ->
    Accum = Accum0 ++ read_line("riak_search> "),
    case string:rstr(Accum, " \\") of
        0 ->
            case Accum of
                "q()" ->
                    io:format("Exiting shell..."),
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
                    Handler(Accum, State),
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

print_info(#state{index=Index, default_field=DefaultField, handler=Handler}) ->
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
    io:format("Index: ~p~nDefault field: ~p~nMode: ~p~n", [Index,
                                                           DefaultField,
                                                           Mode]).

read_line(Prompt) ->
    [_|Line] = lists:reverse(io:get_line(Prompt)),
    lists:reverse(Line).
