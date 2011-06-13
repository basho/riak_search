%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_sort).

-export([sort/3]).
-include("riak_search.hrl").
-import(riak_search_utils, [to_binary/1, to_atom/1, to_integer/1]).



sort(Data, "none", _Schema) ->
    Data;
sort(Data, SortClauses, Schema) ->
    SortFlags = parse_flags(SortClauses, Schema),
    F = fun(F, S) ->
                F1 = riak_indexed_doc:fields(F),
                S1 = riak_indexed_doc:fields(S),
                compare(F1, S1, SortFlags) > -1 
        end,
    lists:sort(F, Data).

parse_flags(SortClauses, Schema) ->
    F = fun(SortClause) ->
                {FieldName, Dir} = normalize_sort_clause(SortClause),
                CompareType = compare_type(FieldName, Schema),
                {FieldName, Dir, CompareType}
        end,
    [F(X) || X <- string:tokens(SortClauses, ",")].

normalize_sort_clause(SortClause) ->
    case string:tokens(SortClause, " ") of
        [Field, Dir] when Dir == "asc" orelse Dir == "desc" -> 
            Field1 = to_binary(string:strip(Field, both)),
            Dir1 = to_atom(string:strip(Dir, both)),
            {Field1, Dir1};
        [Field|_] -> 
            Field1 = to_binary(string:strip(Field, both)),
            {Field1, 'asc'}
    end.

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
        'asc' ->
            if
                FV > SV ->
                    -1;
                FV < SV ->
                    1;
                true ->
                    0
            end;
        'desc' ->
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
    to_integer(V);
get_value(Field, Props, binary) ->  %% Compare in binary form
    proplists:get_value(Field, Props).
    
    

