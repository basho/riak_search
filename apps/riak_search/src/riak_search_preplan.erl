-module(riak_search_preplan).
-export([
         preplan/4
]).
-include("riak_search.hrl").
-record(config, { default_index, default_field, facets }).

preplan(OpList, DefaultIndex, DefaultField, Facets) ->
    preplan(OpList, #config { 
              default_index=DefaultIndex,
              default_field=DefaultField,
              facets=Facets }).

preplan(OpList, Config) ->
    OpList0 = #group { ops=OpList },
    OpList1 = pass1(OpList0, Config),
    OpList2 = pass2(OpList1, Config),
    OpList3 = pass3(OpList2, Config),
    OpList4 = pass4(OpList3, Config),
    OpList5 = pass5(OpList4, Config),
    pass6(OpList5, Config).
    
%% FIRST PASS - Normalize incoming qil. 
%% - We should move this to the qilr project.
%% - Turn field/4 into field/3
%% - Turn group statements into ANDs and ORs.
%% - Wrap prohibited terms into an #lnot.
pass1(OpList, Config) when is_list(OpList) ->
    lists:flatten([pass1(X, Config) || X <- OpList]);

pass1({field, Field, Q, Options}, Config) ->
    TermOp = #term { q=Q, options=Options},
    pass1(#field { field=Field, ops=[TermOp]}, Config);

pass1(Op = #group {}, Config) ->
    %% Qilr parser returns nested lists in a group.
    OpList = lists:flatten([Op#group.ops]),
    case length(OpList) == 1 of
        true -> 
            %% Single op, so get rid of the group.
            pass1(OpList, Config);
        false -> 
            %% Multiple ops. Pull out any terms where required flag is
            %% set, make those on AND. The rest are an OR. AND the
            %% results together.
            F = fun(X) -> is_record(X, term) andalso (?IS_TERM_REQUIRED(X) orelse ?IS_TERM_PROHIBITED(X)) end,
            {RequiredOps, NonRequiredOps} = lists:splitwith(F, OpList),
            if 
                RequiredOps /= [] andalso NonRequiredOps == [] ->
                    pass1(#land { ops=RequiredOps }, Config);
                RequiredOps == [] andalso NonRequiredOps /= [] ->
                    pass1(#lor { ops=NonRequiredOps }, Config);
                true ->
                    NewOp = #land { ops=[#land { ops=RequiredOps },
                                         #lor { ops=NonRequiredOps }]},
                    pass1(NewOp, Config)
            end
    end;

pass1(Op = #term {}, Config) ->
    IsProhibited = ?IS_TERM_PROHIBITED(Op),
    IsProximity = ?IS_TERM_PROXIMITY(Op),
    if 
        IsProhibited ->
            %% Rewrite a prohibited term to be a not.
            NewOp = #lnot { ops=[Op#term { options=[] }] },
            pass1(NewOp, Config);
        IsProximity ->
            Proximity = proplists:get_value(proximity, Op#term.options),
            NewOptions = Op#term.options -- [{proximity, Proximity}],
            Tokens = string:tokens(Op#term.q, " "),
            NewOps = [#term { q=X, options=NewOptions }|| X <- Tokens],
            pass1(#proximity { ops=NewOps, proximity=Proximity }, Config);
        true ->
            Op
    end;

pass1(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass1(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).

%% SECOND PASS
%% - Flatten nested ands/ors. (Also move to qilr project.)
%% - Normalize #term queries.
pass2(OpList, Config) when is_list(OpList) ->
    [pass2(Op, Config) || Op <- OpList];

pass2(Op = #field {}, Config) ->
    {Index, Field} = normalize_field(Op#field.field, Config),
    NewConfig = Config#config {
        default_index = Index,
        default_field = Field
    },
    case Op#field.ops of
        [Op1] -> pass2(Op1, NewConfig);
        Ops -> pass2(Ops, NewConfig)
    end;

pass2(Op = #term {}, Config) ->
    NewQ = normalize_term(Op#term.q, Config),
    Options = Op#term.options,
    rewrite_term(NewQ, Options, Config);

pass2(Op = #land {}, Config) ->
    %% Collapse nested and operations.
    F = fun
        (X = #land {}, {_, Acc}) -> {loop, X#land.ops ++ Acc};
        (X, {Again, Acc}) -> {Again, [X|Acc]}
        end,
    {Continue, NewOps} = lists:foldl(F, {stop, []}, Op#land.ops),

    %% If anything changed, do another round of collapsing...
    case Continue of
        stop ->
            Op#land { ops=pass2(NewOps, Config) };
        loop -> 
            pass2(Op#land { ops=NewOps }, Config)
    end;

pass2(Op = #lor {}, Config) ->
    %% Collapse nested or operations.
    F = fun
        (X = #lor {}, {_, Acc}) -> {loop, X#lor.ops ++ Acc};
        (X, {Again, Acc}) -> {Again, [X|Acc]}
        end,
    {Continue, NewOps} = lists:foldl(F, {stop, []}, Op#lor.ops),

    %% If anything changed, do another round of collapsing...
    case Continue of
        stop ->
            Op#lor { ops=pass2(NewOps, Config) };
        loop -> 
            pass2(Op#lor { ops=NewOps }, Config)
    end;

pass2(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass2(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).

%% Return true if this Query is a facet.
is_facet({Index, Field, _}, Config) ->
    FacetList = Config#config.facets,
    FacetList1 = [normalize_field(X, Config) || X <- FacetList],
    F = fun({FacetIndex, FacetField}) ->
        Index == FacetIndex andalso Field == FacetField
    end,
    lists:any(F, FacetList1).
    
normalize_field(OriginalField, Config) when is_list(OriginalField) ->
    DefIndex = Config#config.default_index,
    case string:tokens(OriginalField, ".") of
        [Field] -> {DefIndex, Field};
        [Index, Field] -> {Index, Field};
        _ -> throw({could_not_normalize_field, OriginalField})
    end;
normalize_field(Field, _) when is_tuple(Field) -> Field.


%% Possibly rewrite a term. The term may be a "special" term, so 
%% we rewrite it to a subterm_type. Or, it may be a facet.
rewrite_term({Index, "date", Term}, Options, _Config) ->
    case riak_search_utils:parse_datetime(Term) of
        {YMD, HMS} ->
            SubTerm = riak_search_utils:date_to_subterm({YMD, HMS}),
            #term { q={Index, subterm, {1, SubTerm}}, options=[facet|Options]};
        error ->
            throw({could_not_parse_date, Term})
    end;
    
rewrite_term(Q, Options, Config) ->
    case is_facet(Q, Config) of
        true -> 
            [#term { q=Q, options=[facet|Options] }];
        false -> 
            {Index, Field, Term} = Q,
            {ok, {Term, Node, Count}} = riak_search:info(Index, Field, Term),
            Weights = [{node_weight, Node, Count}],
            [#term { q=Q, options=Weights ++ Options }]
    end.


%% Convert a string field into {Index, Field, Term}.
normalize_term(OriginalField, Config) when is_binary(OriginalField) ->
    normalize_term(binary_to_list(OriginalField), Config);
normalize_term(OriginalTerm, Config) when is_list(OriginalTerm) ->
    DefIndex = Config#config.default_index,
    DefField = Config#config.default_field,    
    OriginalTerm1 = string:strip(OriginalTerm, right, $*),
    OriginalTerm2 = string:strip(OriginalTerm1, right, $?),
    case string:tokens(OriginalTerm2, ".") of
        [Term] -> {DefIndex, DefField, Term};
        [Field, Term] -> {DefIndex, Field, Term};
        [Index, Field, Term] -> {Index, Field, Term};
        _ -> throw({could_not_normalize_term, OriginalTerm})
    end;
normalize_term(Term, _) when is_tuple(Term) -> Term.


%% THIRD PASS
%% - Collapse facets into the other branches of the query.
pass3(OpList, Config) when is_list(OpList) ->
    [pass3(Op, Config) || Op <- OpList];

pass3(Op = #land {}, Config) ->
    OpList = facetize(Op#land.ops),
    Op#land { ops=pass3(OpList, Config) };

pass3(Op = #lor {}, Config) ->
    OpList = facetize(Op#lor.ops),
    Op#lor { ops=pass3(OpList, Config) };

pass3(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass3(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).

%% Given a list of operations, fold the facets into the non-facets.
facetize(Ops) ->
    %% Get all of the facets...
    Ops1 = lists:flatten(Ops),
    F1 = fun(Op) -> is_all_facets(Op) end,
    {Facets, NonFacets} = lists:partition(F1, Ops1), 
    [inject_facets(X, Facets) || X <- NonFacets].

%% Return true if a structure consists only of facet terms, possibly
%% joined with lands and lors.
is_all_facets(Op) when is_record(Op, term) ->
    ?IS_TERM_FACET(Op);
is_all_facets(Op) when is_tuple(Op) ->
    is_all_facets(element(2, Op));
is_all_facets(Ops) when is_list(Ops) ->
    lists:all(fun(X) -> is_all_facets(X) end, Ops).

%% Walk through an operator injecting facets into any found terms.
inject_facets(Op, Facets) when is_record(Op, term) ->
    NewOptions = [{facets, Facets}|Op#term.options],
    Op#term { options=NewOptions };
inject_facets(Op, Facets) when is_tuple(Op) ->
    Ops = element(2, Op),
    NewOps = inject_facets(Ops, Facets),
    setelement(2, Op, NewOps);
inject_facets(Ops, Facets) when is_list(Ops) ->
    [inject_facets(X, Facets) || X <- Ops].

%% FOURTH PASS
%% Expand wildcards and ranges into a #lor operator.
%% Add doc_freq counts to all terms.
pass4(OpList, Config) when is_list(OpList) ->
    [pass4(X, Config) || X <- OpList];

pass4(Op = #inclusive_range {}, Config) ->
    Start = hd(Op#inclusive_range.start_op),
    End = hd(Op#inclusive_range.end_op),
    Facets = proplists:get_all_values(facets, Start#term.options),
    range_to_lor(Start#term.q, End#term.q, true, Facets, Config);

pass4(Op = #exclusive_range {}, Config) ->
    Start = hd(Op#exclusive_range.start_op),
    End = hd(Op#exclusive_range.end_op),
    Facets = proplists:get_all_values(facets, Start#term.options),
    range_to_lor(Start#term.q, End#term.q, false, Facets, Config);

pass4(Op = #term {}, Config) ->
    IsWildcardAll = ?IS_TERM_WILDCARD_ALL(Op),
    IsWildcardOne = ?IS_TERM_WILDCARD_ONE(Op),
    Facets = proplists:get_all_values(facets, Op#term.options),
    if 
        IsWildcardAll ->
            Start = Op#term.q,
            End = wildcard_all,
            range_to_lor(Start, End, true, Facets, Config);
        IsWildcardOne ->
            Start = Op#term.q,
            End = wildcard_one,
            range_to_lor(Start, End, true, Facets, Config);
        true ->
            Op
    end;

pass4(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass4(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).

range_to_lor(Start, End, Inclusive, Facets, _Config) ->
    {Index, Field, StartTerm, EndTerm, Size} = normalize_range(Start, End, Inclusive),

    %% Results are of form {node, Index.Field.Term, Count}
    {ok, Results} = riak_search:info_range(Index, Field, StartTerm, EndTerm, Size),
    
    %% Collapse results into a gb_tree to combine...
    F1 = fun({Term, Node, Count}, Acc) ->
        NewOption = {node_weight, Node, Count},
        case gb_trees:lookup(Term, Acc) of
            {value, Options} ->
                gb_trees:update(Term, [NewOption|Options], Acc);
            none ->
                gb_trees:insert(Term, [NewOption], Acc)
        end
    end,
    Results1 = lists:foldl(F1, gb_trees:empty(), lists:flatten(Results)),
    Results2 = gb_trees:to_list(Results1),

    %% Create the lor operation.
    F2 = fun({Term, Options}) ->
        Q = {Index, Field, Term},
        #term { q=Q, options=[{facets, Facets}|Options] }
    end,
    Ops = [F2(X) || X <- Results2],
    #lor { ops=Ops }.

normalize_range({Index, Field, StartTerm}, {Index, Field, EndTerm}, Inclusive) ->
    {StartTerm1, EndTerm1} = case Inclusive of
        true -> {StartTerm, EndTerm};
        false ->{binary_inc(StartTerm, +1), binary_inc(EndTerm, -1)}
    end,
    {Index, Field, StartTerm1, EndTerm1, undefined};

normalize_range({Index, Field, Term}, wildcard_all, _Inclusive) ->
    {StartTerm, EndTerm} = wildcard(Term),
    {Index, Field, StartTerm, EndTerm, undefined};

normalize_range({Index, Field, Term}, wildcard_one, _Inclusive) ->
    {StartTerm, EndTerm} = wildcard(Term),
    {Index, Field, StartTerm, EndTerm, typesafe_size(Term) + 1};

normalize_range(Q1, Q2, Inclusive) ->
    throw({unhandled_case, normalize_range, Q1, Q2, Inclusive}).

binary_inc(Term, Amt) when is_list(Term) ->
    NewTerm = binary_inc(list_to_binary(Term), Amt),
    binary_to_list(NewTerm);
binary_inc(Term, Amt) when is_binary(Term) ->
    Bits = size(Term) * 8,
    <<Int:Bits/integer>> = Term,
    NewInt = binary_inc(Int, Amt),
    <<NewInt:Bits/integer>>;
binary_inc(Term, Amt) when is_integer(Term) ->
    Term + Amt;
binary_inc(Term, _) ->
    throw({unhandled_type, binary_inc, Term}).

wildcard(Term) when is_binary(Term) ->
    Size = size(Term),
    {<<Term:Size/binary>>, <<Term:Size/binary, 255:8/integer>>};
wildcard(Term) when is_list(Term) ->
    {Term, Term ++ [255]}.

typesafe_size(Term) when is_binary(Term) -> size(Term);
typesafe_size(Term) when is_list(Term) -> length(Term).

%% FIFTH PASS
%% Collapse nested NOTs.
pass5(OpList, Config) when is_list(OpList) ->
    [pass5(X, Config) || X <- OpList];

pass5(Op = #lnot {}, Config) ->
    F = fun(X) ->
                case is_record(X, lnot) of
                    true ->
                        pass5(X, Config);
                    false ->
                        #lnot { ops=to_list(pass5(X, Config)) }
                end
        end,
    [F(X) || X <- to_list(Op#lnot.ops)];

pass5(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass5(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).


%% SIXTH PASS - Wrap #lor and #land operations with a #node operation
%% based on where the weightiest node is coming from. By now, all
%% terms will have a weight associated with them of the form
%% {node_weight, Node, Weight} where Weight is simply the number of
%% documents. We want to make sure that #lors and #lands happen on the
%% node with the most weight.
pass6(OpList, Config) when is_list(OpList) ->
    [pass6(X, Config) || X <- OpList];

pass6(Op = #land {}, Config) ->
    Node = get_preferred_node(Op),
    Ops = Op#land.ops,
    NewOp = Op#land { ops=pass6(Ops, Config) },
    #node { ops=NewOp, node=Node };

pass6(Op = #lor {}, Config) ->
    Node = get_preferred_node(Op),
    Ops = Op#lor.ops,
    NewOp = Op#lor { ops=pass6(Ops, Config) },
    #node { ops=NewOp, node=Node };

pass6(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass6(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).



%% Return the node with the highest combined weight.
get_preferred_node(Op) ->
    Weights = get_preferred_node_inner(Op),
    F = fun({Node, Weight}, Acc) ->
        case gb_trees:lookup(Node, Acc) of
            {value, OldWeight} ->
                gb_trees:update(Node, OldWeight + Weight, Acc);
            none ->
                gb_trees:enter(Node, Weight, Acc)
        end
    end,
    StartingTree = gb_trees:from_orddict([{node(), 0}]),
    Tree = lists:foldl(F, StartingTree, lists:flatten(Weights)),
    List = gb_trees:to_list(Tree),
    get_largest(List, node(), 0).

get_largest([], Node, _Weight) -> Node;
get_largest([{NewNode, NewWeight}|T], Node, Weight) ->
    case NewWeight > Weight of
        true  -> get_largest(T, NewNode, NewWeight);
        false -> get_largest(T, Node, Weight)
    end.

%% Given a nested list of operations, return a list of [{Node, Weight}].
get_preferred_node_inner(Op) when is_record(Op, term) -> 
    [{Node, Weight} || {node_weight, Node, Weight} <- Op#term.options];
get_preferred_node_inner(Op) ->
    Ops = element(2, Op),
    [get_preferred_node_inner(X) || X <- Ops].

    
to_list(L) when is_list(L) -> L;
to_list(T) when is_tuple(T) -> [T].

