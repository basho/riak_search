-module(riak_search_dateutil).
-export([
    date_to_subterm/1,
    parse_datetime/1
]).

date_to_subterm(min) ->
    <<0, 0, 0, 0, 0, 0, 0>>;
date_to_subterm(max) ->
    <<255, 255, 255, 255, 255, 255, 255, 255>>;
date_to_subterm({{Y,M,D}, min}) ->
    EndBits = <<0, 0, 0, 0, 0>>,
    <<SubTerm:64/integer>> = <<Y:16/integer, M:4/integer, D:4/integer, EndBits:5/binary>>,
    SubTerm;
date_to_subterm({{Y,M,D}, max}) ->
    EndBits = <<255, 255, 255, 255, 255>>,
    <<SubTerm:64/integer>> = <<Y:16/integer, M:4/integer, D:4/integer, EndBits:5/binary>>,
    SubTerm;
date_to_subterm({{Y,M,D}, {HH,MM,SS}}) ->
    <<SubTerm:64/integer>> = <<Y:16/integer, M:4/integer, D:4/integer, HH:4/integer, MM:4/integer, SS:4/integer, 0:28/integer>>,
    SubTerm;
date_to_subterm(Other) ->
    throw({date_to_subterm, unknown_format, Other}).

%% EXAMPLE: Wed, 7 Feb 2001 09:07:00 -0800
parse_datetime([_,_,_,_,$\s,D1,$\s,M1,M2,M3,$\s,Y1,Y2,Y3,Y4,$\s,HH1,HH2,$:,MM1,MM2,$:,SS1,SS2|_]) ->
    YMD = {list_to_integer([Y1,Y2,Y3,Y4]), month([M1,M2,M3]), list_to_integer([D1])},
    HMS = {list_to_integer([HH1,HH2]), list_to_integer([MM1,MM2]), list_to_integer([SS1,SS2])},
    {YMD, HMS};
        
%% EXAMPLE: Wed, 14 Feb 2001 09:07:00 -0800
parse_datetime([_,_,_,_,$\s,D1,D2,$\s,M1,M2,M3,$\s,Y1,Y2,Y3,Y4,$\s,HH1,HH2,$:,MM1,MM2,$:,SS1,SS2|_]) ->
    YMD = {list_to_integer([Y1,Y2,Y3,Y4]), month([M1,M2,M3]), list_to_integer([D1, D2])},
    HMS = {list_to_integer([HH1,HH2]), list_to_integer([MM1,MM2]), list_to_integer([SS1,SS2])},
    {YMD, HMS};


%% EXAMPLE: 20081015
parse_datetime([Y1,Y2,Y3,Y4,M1,M2,D1,D2]) ->
    {parse_date([Y1,Y2,Y3,Y4,M1,M2,D1,D2]), {0,0,0}};


%% EXAMPLE: 2004-10-14
parse_datetime([Y1,Y2,Y3,Y4,$-,M1,M2,$-,D1,D2]) ->
    {parse_date([Y1,Y2,Y3,Y4,M1,M2,D1,D2]), {0,0,0}};

parse_datetime(_) ->
    error.

%% @private
parse_date([Y1,Y2,Y3,Y4,M1,M2,D1,D2]) ->
    Y = list_to_integer([Y1, Y2, Y3, Y4]),
    M = list_to_integer([M1, M2]),
    D = list_to_integer([D1, D2]),
    {Y, M, D}.

%% @private
month("Jan") -> 1;
month("Feb") -> 2;
month("Mar") -> 3;
month("Apr") -> 4;
month("May") -> 5;
month("Jun") -> 6;
month("Jul") -> 7;
month("Aug") -> 8;
month("Sep") -> 9;
month("Oct") -> 10;
month("Nov") -> 11;
month("Dec") -> 12.
