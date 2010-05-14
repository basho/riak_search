#!/usr/bin/env escript
%%! +Bi

usage() -> "
  Usage:   ./bin/generate_data.escript BASENAME COUNT
  Example: ./bin/generate_data.escript sample_data/sample100 100

  This will automatically generate sample data in Solr XML format
  that can be used for writing test queries against Riak Search.
".

main(Args) ->
  try
    false == lists:member("-?", Args),
    [BaseName, CountS] = Args,
    Count = list_to_integer(CountS),
    try 
      write(BaseName, Count),
      io:format("~n"),
      io:format("Generated ~p lines of test data in files:~n", [Count]),
      io:format(" - ~s.add~n", [BaseName]),
      io:format(" - ~s.delete~n~n", [BaseName])
    catch _Type1 : Message1 ->
	io:format("Error: ~p~n~p", Message1, erlang:get_stacktrace())
    end
  catch _Type2 : Message2 ->
    io:format("~s", [usage()]),
    io:format("~s", [Message2])      
  end.

-define(COLORS, ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]).
-define(PARITY, ["odd", "even"]).

write(Filename, Size) ->
  write_add_file(Filename, Size),
  write_delete_file(Filename, Size),
  ok.

write_add_file(Filename, Size) ->
  Prefix = filename:basename(Filename) ++ "_",
  Contents = assemble_add_file(Prefix, Size),
  {ok, IO} = file:open(Filename ++ ".add", [write]),
  ok = file:write(IO, list_to_binary(Contents)),
  ok = file:close(IO).

write_delete_file(Filename, Size) ->
  Prefix = filename:basename(Filename) ++ "_",
  Contents = assemble_delete_file(Prefix, Size),
  {ok, IO} = file:open(Filename ++ ".delete", [write]),
  ok = file:write(IO, Contents),
  ok = file:close(IO).

assemble_add_file(Prefix, N) ->
  [
    "<add>\n",
    [doc(Prefix, X) || X <- lists:seq(0, N - 1)],
    "</add>"
  ].

assemble_delete_file(Prefix, N) ->
  [
    "<delete>\n",
    [["<id>", Prefix, acc(X, 3), "</id>\n"] || X <- lists:seq(0, N - 1)],
    "</delete>"
  ].

doc(Prefix, N) ->
  [
    "<doc>\n", 
    "<field name=\"id\">", Prefix, acc(N, 3), "</field>",
    "<field name=\"key_t\">", Prefix, acc(N, 3), "</field>",
    "<field name=\"acc_t\">", accs(N, 3), "</field>",
    "<field name=\"color_t\">", color(N), "</field>",
    "<field name=\"parity_t\">", parity(N), "</field>",
    "</doc>\n"
  ].

accs(Int, Width) ->
  [[acc(X, Width), " "] || X <- lists:seq(0, Int)].

acc(Int, Width) ->
  S = acc(Int),
  string:copies("A", Width - length(S)) ++ S.

acc(Int) -> lists:reverse(inner_acc(Int)).
inner_acc(0)   -> [];
inner_acc(Int) -> [$A + (Int rem 10)|inner_acc(Int div 10)].

color(N) -> 
  lists:nth((N rem length(?COLORS)) + 1, ?COLORS).

parity(N) ->
  lists:nth((N rem length(?PARITY)) + 1, ?PARITY).
