-module(riak_search_op_term).
-export([
         preplan_op/2,
         chain_op/3
        ]).

-include("riak_search.hrl").

preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef) ->
    Q = Op#term.q,
    Facets = proplists:get_all_values(facets, Op#term.options),
    spawn_link(fun() -> start_loop(Q, Facets, OutputPid, OutputRef) end),
    {ok, 1}.

start_loop(Q, Facets, OutputPid, OutputRef) ->
    {Index, Field, Term} = Q,
    {SubType, StartSubTerm, EndSubTerm} = detect_subterms(lists:flatten(Facets)),

    %% Stream the results...
    Fun = fun(_Value, Props) ->
        riak_search_facets:passes_facets(Props, Facets)
    end,
    {ok, Ref} = riak_search:stream(Index, Field, Term, SubType, StartSubTerm, EndSubTerm, Fun),

    %% Gather the results...
    loop(Ref, OutputPid, OutputRef).

loop(Ref, OutputPid, OutputRef) ->
    receive 
        {result, '$end_of_table', Ref} ->
            OutputPid!{disconnect, OutputRef};

        {result, {Key, Props}, Ref} ->
            OutputPid!{results, [{Key, Props}], OutputRef},
            loop(Ref, OutputPid, OutputRef)
    end.

%% Detect a subterm filter if it's a top level facet
%% operation. Otherwise, if it's anything fancy, we will handle it
%% with the FilterFunction passed to the backend, using
%% riak_searc_facets:passes_facets/2. We do this because there might
%% be some crazy ANDs/NOTs/ORs that we just can't handle here.
detect_subterms(Ops) ->
    case detect_subterms_inner(Ops) of
        {SubType, StartSubTerm, EndSubTerm} ->
            {SubType, StartSubTerm, EndSubTerm};
        undefined ->
            SubType = detect_subtype(Ops),
            {SubType, all, all}
    end.

detect_subterms_inner([Op|Ops]) when is_record(Op, inclusive_range) ->
    StartOp = hd(Op#inclusive_range.start_op),
    EndOp = hd(Op#inclusive_range.end_op),
    case {StartOp#term.q, EndOp#term.q} of
        {{_, subterm, {SubType, StartSubTerm}}, {_, subterm, {SubType, EndSubTerm}}} ->
            {SubType, StartSubTerm, EndSubTerm};
        _ ->
            detect_subterms_inner(Ops)
    end;
detect_subterms_inner([Op|Ops]) when is_record(Op, exclusive_range) ->
    StartOp = hd(Op#exclusive_range.start_op),
    EndOp = hd(Op#exclusive_range.end_op),
    case {StartOp#term.q, EndOp#term.q} of
        {{_, subterm, {SubType, StartSubTerm}}, {_, subterm, {SubType, EndSubTerm}}} ->
            {SubType, StartSubTerm, EndSubTerm};
        _ ->
            detect_subterms_inner(Ops)
    end;
detect_subterms_inner([Op|Ops]) when is_record(Op, term) ->
    case Op#term.q of
        {_, subterm, {SubType, SubTerm}} ->
            {SubType, SubTerm, SubTerm};
        _ ->
            detect_subterms_inner(Ops)
    end;
detect_subterms_inner([_|Ops]) ->
    detect_subterms_inner(Ops);
detect_subterms_inner([]) ->
    undefined.


detect_subtype([Op|Ops]) when is_record(Op, term) ->
    case Op#term.q of
        {_, subterm, {SubType, _}} -> 
            SubType;
        _ ->
            detect_subtype(Ops)
    end;
detect_subtype([Op|Ops]) when is_tuple(Op) ->
    case detect_subtype(element(2, Op)) of
        0 -> detect_subtype(Ops);
        Value -> Value
    end;
detect_subtype([Op|Ops]) when is_list(Op) ->
    case detect_subtype(Op) of
        0 -> detect_subtype(Ops);
        Value -> Value
    end;
detect_subtype([]) ->
    0.
