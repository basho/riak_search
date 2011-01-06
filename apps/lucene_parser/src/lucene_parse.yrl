%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

Nonterminals
queries
query
.

Terminals
%% +/-
required prohibited

%% %% AND/OR/NOT
intersection union negation

%% Groups
group_start group_end

%% Ranges
inclusive_start inclusive_end exclusive_start exclusive_end range_to

%% Other
scope fuzzy proximity boost string wildcard_char wildcard_glob
.

Left 100 intersection.
Left 100 union.
Right 200 scope.
Right 200 negation.

Rootsymbol queries.

%% Sequence of queries.
queries -> query : 
    ['$1'].

queries -> queries query :
    '$1' ++ ['$2'].

%% Group
query -> group_start queries group_end : 
    case '$2' of
        [Rec] -> Rec;
        Recs  -> #group { ops=Recs }
    end.

%% +string
query -> required string :
    #string { s=val('$2'), flags=[required] }.

query -> required string scope string :
    {Index, Field} = split_scope(val('$2')),
    TermOp = #string { s=val('$4'), flags=[required] },
    #scope { index=Index, field=Field, ops=[TermOp]}.

%% -string
query -> prohibited query :
    #negation { op='$2' }.

%% query AND query, query OR query
query -> query intersection query :
    Ops = lists:flatten([unwrap(intersection, '$1'), unwrap(intersection, '$3')]),
    #intersection { ops=Ops }.

query -> query union query :
    Ops = lists:flatten([unwrap(union, '$1'), unwrap(union, '$3')]),
    #union { ops=Ops }.

%% ranges
query -> inclusive_start string range_to string inclusive_end :
    #range { from={inclusive, val('$2')}, to={inclusive, val('$4')}}.

query -> exclusive_start string range_to string inclusive_end :
    #range { from={exclusive, val('$2')}, to={inclusive, val('$4')}}.

query -> inclusive_start string range_to string exclusive_end :
    #range { from={inclusive, val('$2')}, to={exclusive, val('$4')}}.

query -> exclusive_start string range_to string exclusive_end :
    #range { from={exclusive, val('$2')}, to={exclusive, val('$4')}}.

%% NOT query
query -> negation query :
    #negation { op='$2' }.

%% string~0.5
query -> string fuzzy :
    #string { s=val('$1'), flags=[{fuzzy, val('$2')}]}.

%% string~5
query -> string proximity :
    #string { s=val('$1'), flags=[{proximity, val('$2')}]}.

%% string^5.0
query -> string boost :
    #string { s=val('$1'), flags=[{boost, val('$2')}]}.

%% scope:query 
query -> string scope query :
    {Index, Field} = split_scope(val('$1')),
    #scope { index=Index, field=Field, ops=['$3']}.

query -> wildcard_char :
    S1 = val('$1'),
    Length = length(S1),
    S2 = string:substr(S1, 1, Length - 1),
    #string { s=S2, flags=[{wildcard, char}]}.

query -> wildcard_glob :
    S1 = val('$1'),
    Length = length(S1),
    S2 = string:substr(S1, 1, Length - 1),
    #string { s=S2, flags=[{wildcard, glob}]}.

query -> string :
    #string { s=val('$1') }.

Erlang code.

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
    
