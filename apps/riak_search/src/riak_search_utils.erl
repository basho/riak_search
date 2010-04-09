-module(riak_search_utils).
-export([
    iterator_chain/2,
    combine_terms/2,
    date_to_subterm/1,
    parse_datetime/1
]).
-include("riak_search.hrl").


%% Chain a list of iterators into what looks like one single iterator.
%% The SelectFun/2 takes two iterators (which each provide {Value,
%% Props, IteratorFun}). The SelectFun is responsible for choosing
%% which value is next in the series, and returning {Value, Props,
%% NewIteratorFun}.
iterator_chain(_, [Op]) ->
    iterator_chain_op(Op);
iterator_chain(SelectFun, [Op|OpList]) ->
    OpIterator = iterator_chain_op(Op),
    GroupIterator = iterator_chain(SelectFun, OpList),
    fun() -> SelectFun(OpIterator(), GroupIterator()) end;
iterator_chain(_, []) ->
    fun() -> {eof, false} end.

%% Chain an operator, and build an iterator function around it. The
%% iterator will return {Result, NotFlag, NewIteratorFun} each time it is called, or block
%% until one is available. When there are no more results, it will
%% return {eof, NotFlag, NewIteratorFun}.
iterator_chain_op(Op) ->
    %% Spawn a collection process...
    Ref = make_ref(),
    Pid = spawn_link(fun() -> collector_loop(Ref, []) end),
    
    %% Chain the op...
    riak_search_op:chain_op(Op, Pid, Ref),

    %% Return an iterator function. Returns
    %% a new result.
    fun() -> iterator_chain_inner(Pid, make_ref(), Op) end.


%% Iterator function body.
iterator_chain_inner(Pid, Ref, Op) ->
    Pid!{get_result, self(), Ref},
    receive 
        {result, eof, Ref} ->
            {eof, Op};

        {result, Result, Ref} ->
            {Result, Op, fun() -> iterator_chain_inner(Pid, Ref, Op) end}
    end.


%% Collect messages in the process's mailbox, and wait until someone
%% requests it.
collector_loop(Ref, []) ->
    receive
        {results, Results, Ref} ->
            collector_loop(Ref, Results);
        {disconnect, Ref} ->
            collector_loop(Ref, eof)
    end;
collector_loop(Ref, [Result|Results]) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, Result, OutputRef},
            collector_loop(Ref, Results)
    end;
collector_loop(_Ref, eof) ->
    receive
        {get_result, OutputPid, OutputRef} ->
            OutputPid!{result, eof, OutputRef}
    end.


%% Gine
combine_terms({Value, Props1}, {Value, Props2}) ->
    NewProps = sets:to_list(sets:intersection(sets:from_list(Props1), sets:from_list(Props2))),
    {Value, NewProps};
combine_terms(Other1, Other2) ->
    error_logger:error_msg("Could not combine terms: [~p, ~p]~n", [Other1, Other2]),
    throw({could_not_combine, Other1, Other2}).



%%% Convert a date to a 64-bit SubTerm integer.
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

%% Parse a list date into {{Y, M, D}, {H, M, S}}.

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
