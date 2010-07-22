%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_sort).

-export([sort/3]).

sort(Data, "none", _Schema) ->
    Data;
sort(Data, Flags, Schema) ->
    PFlags = parse_flags(Flags, Schema),
    F = fun(F, S) ->
                F1 = riak_indexed_doc:fields(F),
                S1 = riak_indexed_doc:fields(S),
                compare(F1, S1, PFlags) > -1 
        end,
    lists:sort(F, Data).

parse_flags(Flags, Schema) ->
    Stmts = string:tokens(Flags, ","),
    [{string:strip(Field, both),
      string:strip(Dir, both),
      compare_type(Field, Schema)} || [Field, Dir] <- [format(string:tokens(Stmt, " ")) || Stmt <- Stmts]].

format([Field]) ->
    [Field, "asc"];
format([Field, Dir]) ->
    [Field, Dir];
format([Field|_]) ->
    [Field, "asc"].

compare_type(Name, Schema) ->
    Field = Schema:find_field(Name),
    case Schema:field_type(Field) of
        integer ->
            intstr;
        _ ->
            binary
    end.
        
compare(_F, _S, []) ->
    0;
compare(F, S, [H|T]) ->
    case compare(F, S, H) of
        0 ->
            compare(F, S, T);
        V ->
            V
    end;
compare(F, S, {Field, Dir, CompareType}) ->
    FV = get_value(Field, F, CompareType),
    SV = get_value(Field, S, CompareType),
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
                    0
            end
    end.

%% Extract the field value and convert it to the correct type for comparison.
get_value(Field, Props, intstr) ->  %% Field contains an integer stored in string representation
    V = proplists:get_value(Field, Props),
    list_to_integer(binary_to_list(V));
get_value(Field, Props, binary) ->  %% Compare in binary form
    proplists:get_value(Field, Props).
    
    

