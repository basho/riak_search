-module(riak_solr_sort).

-export([sort/2]).

sort(Data, "none") ->
    Data;
sort(Data, Flags) ->
    PFlags = parse_flags(Flags),
    F = fun(F, S) ->
                F1 = riak_indexed_doc:fields(F),
                S1 = riak_indexed_doc:fields(S),
                compare(F1, S1, PFlags) > -1 end,
    lists:sort(F, Data).

parse_flags(Flags) ->
    Stmts = string:tokens(Flags, ","),
    [{string:strip(Field, both),
      string:strip(Dir, both)} || [Field, Dir] <- [format(string:tokens(Stmt, " ")) || Stmt <- Stmts]].

format([Field]) ->
    [Field, "asc"];
format([Field, Dir]) ->
    [Field, Dir];
format([Field|_]) ->
    [Field, "asc"].

compare(_F, _S, []) ->
    0;
compare(F, S, [H|T]) ->
    case compare(F, S, H) of
        0 ->
            compare(F, S, T);
        V ->
            V
    end;
compare(F, S, {Field, Dir}) ->
    FV = proplists:get_value(Field, F),
    SV = proplists:get_value(Field, S),
    case Dir of
        "asc" ->
            if
                FV > SV ->
                    -1;
                FV < SV ->
                    1;
                true ->
                    0
            end;
        "desc" ->
            if
                FV > SV ->
                    1;
                FV < SV ->
                    -1;
                true ->
                    1
            end
    end.
