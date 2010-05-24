-module(search).
-export([search/1,
         doc_search/1,
         explain/1,
         graph/1,
         index_dir/1
%%     index_file/1, %index_file/3,
%%     index_term/3 %index_term/5
]).
-define(IS_CHAR(C), ((C >= $A andalso C =< $Z) orelse (C >= $a andalso C =< $z))).
-define(DEFAULT_INDEX, "search").
-define(DEFAULT_FIELD, "default").
-define(DEFAULT_FACETS, ["search.color", "search.direction", "search.subterm", "search.subterm"]).
-include("riak_search.hrl").

search(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:search("search", "payload", Q).

doc_search(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:doc_search("search", "payload", Q).

explain(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:explain("search", "payload", Q).

graph(Q) ->
    {ok, Client} = riak_search:local_client(),
    Client:query_as_graph(Client:explain("search", "payload", Q)),
    ok.

%% Run the specified search query.
%% search(Q) ->
%%     {ok, Qilr} = qilr_parse:string(Q),
%%     {ok, Results} = riak_search:execute(Qilr, ?DEFAULT_INDEX, ?DEFAULT_FIELD, ?DEFAULT_FACETS),
%%     F = fun({Key, Props}) ->
%%                 io:format("key:   ~p~n", [Key]),
%%                 io:format("props: ~p~n", [Props]),
%%                 io:format("~n")
%%         end,
%%     [F(X) || X <- Results],
%%     io:format("Found ~p results~n", [length(Results)]),
%%     ok.

%% %% Display the execution path of the specified search query.
%% explain(Q) ->
%%     {ok, Qilr} = qilr_parse:string(Q),
%%     riak_search:explain(Qilr, ?DEFAULT_INDEX, ?DEFAULT_FIELD, ?DEFAULT_FACETS).


%% Full-text index the files within the specified directory.
index_dir(Directory) ->
    index_dir(Directory, ?DEFAULT_INDEX, ?DEFAULT_FIELD).

index_dir(Directory, Index, Field) ->
    io:format(" :: Indexing directory: ~s~n", [Directory]),
    {ok, AnalyzerPid} = qilr_analyzer_sup:new_analyzer(),
    {T1, T2, T3} = erlang:now(),
    random:seed(T1, T2, T3),
    %% Get a list of files in the directory, and index them.
    Directory1 = case string:str(Directory, "*") of
        0 -> filename:join([Directory, "*"]);
        _ -> Directory
    end,
    Files = filelib:wildcard(Directory1),
    io:format(" :: Found ~p files...~n", [length(Files)]),
%%    [index_file(SearchClient, File, Index, Field) || File <- Files],
     plists:map(fun(File) -> index_file(AnalyzerPid, File, Index, Field) end,
         Files, {processes, 4}),
    qilr_analyzer:close(AnalyzerPid),
    ok.

%% %% Full-text index the specified file.
%% index_file(SearchClient, File) ->
%%     index_file(SearchClient, File, ?DEFAULT_INDEX, ?DEFAULT_FIELD).
index_file(AnalyzerPid, File, Index, Field) ->
%%     Props = random_properties(),
    Basename = filename:basename(File),
    io:format(" :: Indexing file: ~s~n", [Basename]),
    {ok, SearchClient} = riak_search:local_client(),
    case file:read_file(File) of
        {ok, Bytes} ->
            %% build doc
            IdxDoc = riak_indexed_doc:new(Basename, Index),
            IdxDoc2 = riak_indexed_doc:set_fields([
                {"payload", binary_to_list(Bytes)}], IdxDoc),
            %% index
            SearchClient:index_doc(AnalyzerPid, IdxDoc2);
        {error, eisdir} ->
            io:format("following directory: ~p~n", [File]),
            index_dir(File, Index, Field);
        Err ->
            io:format("index_file(~p, ~p, ~p): error: ~p~n",
                [File, Index, Field, Err])
    end,
    ok.

%% %% Full-text index the files within the specified directory.
%% index_dir(Directory) ->
%%     index_dir(Directory, ?DEFAULT_INDEX, ?DEFAULT_FIELD).

%% index_dir(Directory, Index, Field) ->
%%     io:format(" :: Indexing directory: ~s~n", [Directory]),

%%     %% Get a list of files in the directory, and index them.
%%     Directory1 = case string:str(Directory, "*") of
%%         0 -> filename:join([Directory, "*"]);
%%         _ -> Directory
%%     end,
%%     Files = filelib:wildcard(Directory1),
%%     io:format(" :: Found ~p files...~n", [length(Files)]),
%%     [index_file(File, Index, Field) || File <- Files],
%%     ok.



%% %% Full-text index the specified file.
%% index_file(File) ->
%%     index_file(File, ?DEFAULT_INDEX, ?DEFAULT_FIELD).

%% index_file(File, Index, Field) ->
%%     %% TODO - discover something about the file for properties.
%%     Props = random_properties(),
%%     Basename = filename:basename(File),

%%     io:format(" :: Indexing file: ~s~n", [Basename]),

%%     %% Get the bytes...
%%     {ok, Bytes} = file:read_file(File),
%%     Words = bytes_to_words(Bytes),

%%     %% Calculate word positions...
%%     F1 = fun(Word, {Position, Acc}) ->
%%         case gb_trees:lookup(Word, Acc) of
%%             {value, Positions} ->
%%                 {Position + 1, gb_trees:update(Word, [Position|Positions], Acc)};
%%             none ->
%%                 {Position + 1, gb_trees:insert(Word, [Position], Acc)}
%%         end
%%     end,
%%     {_, PositionTree} = lists:foldl(F1, {1, gb_trees:empty()}, Words),

%%     %% Index the words...
%%     F2 = fun(Word, SubType, SubTerm) ->
%%         {value, Positions} = gb_trees:lookup(Word, PositionTree),
%%         NewProps = Props ++ [{word_pos, Positions}, {freq, length(Positions)}],
%%         index_term(Index, Field, Word, SubType, SubTerm, Basename, NewProps)
%%     end,
%%     [F2(X, 0, 0) || X <- Words],

%%     %% Now index based on date...
%%     case re:run(Bytes, "Date:\s*(.*)", [{capture, all, list}]) of
%%         {match, [_, Date|_]} ->
%%             case riak_search_utils:parse_datetime(Date) of
%%                 {YMD, HMS} ->
%%                     SubType = 1,
%%                     SubTerm = riak_search_utils:date_to_subterm({YMD, HMS}),
%%                     [F2(Word, SubType, SubTerm) || Word <- Words],
%%                     ok;
%%                 _ ->
%%                     io:format("Could not parse date: ~p~n", [Date])
%%             end;
%%         Other ->
%%             io:format("Could not find date: ~p~n", [Other])
%%     end,
%%     ok.

%% %% Index
%% index_term(Term, Value, Props) ->
%%     index_term(?DEFAULT_INDEX, ?DEFAULT_FIELD, Term, Value, Props).

%% index_term(Index, Field, Term, Value, Props) ->
%%     riak_search:index(Index, Field, Term, 0, 0, Value, Props).

%% index_term(Index, Field, Term, SubType, SubTerm, Value, Props) ->
%%     riak_search:index(Index, Field, Term, SubType, SubTerm, Value, Props).

%% This method returns fake properties. It is called by index_file and
%% is here so that you can play around with facet search.
%% random_properties() ->
%%     Colors = ["red", "orange", "yellow", "green", "blue"],
%%     ColorNum = random:uniform(length(Colors)),
%%     Color = lists:nth(ColorNum, Colors),

%%     Directions = ["north", "south", "east", "west"],
%%     DirectionNum = random:uniform(length(Directions)),
%%     Direction = lists:nth(DirectionNum, Directions),

%%     [{"color", Color}, {"direction", Direction}].


%% bytes_to_words(B) ->
%%     bytes_to_words(B, []).
%% bytes_to_words(<<>>, []) ->
%%     [];
%% bytes_to_words(<<>>, Acc) ->
%%     Word = string:to_lower(lists:reverse(Acc)),
%%     [Word];
%% bytes_to_words(<<C, Rest/binary>>, Acc) when ?IS_CHAR(C) ->
%%     bytes_to_words(Rest, [C|Acc]);
%% bytes_to_words(<<_, Rest/binary>>, []) ->
%%     bytes_to_words(Rest, []);
%% bytes_to_words(<<_, Rest/binary>>, Acc) ->
%%     Word = string:to_lower(lists:reverse(Acc)),
%%     [Word|bytes_to_words(Rest, [])].

random_indexer(Pool) ->
    lists:nth(random:uniform(length(Pool)), Pool).

indexer() ->
    {ok, Pid} = qilr_analyzer_sup:new_analyzer(),
    {ok, Client} = riak_search:local_client(),
    do_index(Pid, Client).

do_index(AnalyzerPid, SearchClient) ->
    receive
        {index, Doc} ->
            SearchClient:index_doc(AnalyzerPid, Doc),
            do_index(AnalyzerPid, SearchClient);
        stop ->
            io:format("(~p) indexer complete~n", [self()]),
            ok
    end.

%% NOTE: This was used during early testing against a demo
%% backend. Keeping it around for a little while longer.

%% tests() ->
%%     %%     test("+b +c b", "c"), %% NOT CLEAR HOW TO HANDLE THIS CASE
%%     %%     test("+a -b (ab)", "ab"), %% PARSER FAILS HERE

%%     %%     %% TEST +/-
%%     %%     test("c b a", "abc"),
%%     %%     test("+c +b +a", ""),
%%     %%     test("+c ac", "c"),
%%     %%     test("+c OR +b AND -a", "bc"),
%%     %%     test("(+abc -def -a)", "bc"),

%%     %%     %% TEST AND
%%     %%     test("a AND a", "a"),
%%     %%     test("aa AND a", "a"),
%%     %%     test("aa AND aa", "aa"),
%%     %%     test("a AND b", ""),
%%     %%     test("a AND ab", "a"),
%%     %%     test("a AND ba", "a"),
%%     %%     test("ab AND ba", "ab"),
%%     %%     test("ba AND ba", "ab"),
%%     %%     test("ba AND bac", "ab"),
%%     %%     test("abc AND bcd AND cde", "c"),
%%     %%     test("abcd AND bcde AND cdef AND defg", "d"),

%%     %%     %% TEST NOT
%%     %%     test("aa AND (NOT a)", ""),
%%     %%     test("aa AND (NOT b)", "aa"),
%%     %%     test("a AND (NOT ab)", ""),
%%     %%     test("a AND (NOT ba)", ""),
%%     %%     test("ab AND (NOT a)", "b"),
%%     %%     test("cba AND (NOT b)", "ac"),

%%     %%     %% TEST OR
%%     %%     test("a OR a", "a"),
%%     %%     test("aa OR a", "aa"),
%%     %%     test("a OR ab", "ab"),
%%     %%     test("a OR ab", "ab"),
%%     %%     test("abc OR def", "abcdef"),
%%     %%     test("abc OR def OR ghi", "abcdefghi"),

%%     %%     %% TEST GROUPS
%%     %%     test("a OR (a OR (a OR (a OR b)))", "ab"),
%%     %%     test("a OR (b OR (c OR (d OR a)))", "abcd"),
%%     ok.

%% test(Q, ExpectedResults) ->
%%     %% Parse...
%%     {ok, Qilr} = qilr_parse:string(Q),
%%     %%     ?PRINT(Qilr),

%%     %% Execute...
%%     {ok, Results} = execute(Qilr),
%%     %%     ?PRINT(Results),

%%     %% Check result...
%%     case Results == ExpectedResults of
%%         true ->
%%             io:format(" [.] PASS - ~s~n", [Q]);
%%         false ->
%%             io:format(" [X] FAIL - ~s~n", [Q])
%%     end.


%% test(Q) ->
%%     {ok, Qilr} = qilr_parse:string(Q),
%%     riak_search_preplan:preplan(Qilr, "defIndex", "defField", ["price", "color"]).
