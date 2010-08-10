-module(riak_search_preplan).

-include("riak_search.hrl").

-export([preplan/2]).

-define(VISITOR(FUNNAME, ARGS), fun(N) -> FUNNAME(N, ARGS) end).

preplan(AST, Schema) ->
    %% pass 1 - Convert field & terms
    AST1 = visit(AST, ?VISITOR(convert_terms, Schema), true),
    %% pass 2 - Flatten and consolidate boolean ops
    AST2 = visit(AST1, ?VISITOR(flatten_bool, Schema), false),
    %% pass 3 - Inject facets or node weights for terms
    AST3 = visit(AST2, ?VISITOR(insert_facet_or_weight, Schema), true),
    %% pass 4 - convert range and wildcard expressions
    AST4 = visit(AST3, ?VISITOR(wildcard_range_to_or, Schema), true),
    %% pass 5 - pick node for boolean ops
    visit(AST4, ?VISITOR(select_node, Schema), true).

%% Internal functions

%% AST Transformers

%% Select weightiest node for ANDs & ORs
select_node(#land{ops=Ops}=Op, _Schema) ->
    Node = find_heaviest_node(Ops),
    #node{node=Node, ops=Op};
select_node(#lor{ops=Ops}=Op, _Schema) ->
    Node = find_heaviest_node(Ops),
    #node{node=Node, ops=Op};
select_node(Op, _Schema) ->
    Op.

%% Convert wildcards and ranges into lor operations
wildcard_range_to_or(#field{field=FieldName, ops=Ops}, Schema)
  when is_record(Ops, inclusive_range);
       is_record(Ops, exclusive_range)->
    Field = Schema:find_field(FieldName),
    {_Index, FieldName, StartValue, EndValue, Size} =
        normalize_range({Schema:name(), FieldName, range_start(Ops)},
                        {Schema:name(), FieldName, range_end(Ops)},
                        range_type(Ops) =:= inclusive),
    case range_to_terms(Schema:name(), FieldName, StartValue,
                        EndValue, Size, Schema:field_type(Field)) of
        [] ->
            skip;
        Results ->
            #lor{ops=terms_from_range_results(Schema, FieldName, Results)}
    end;
wildcard_range_to_or({field, FieldName, T, _}=F, Schema) when is_record(T, term) ->
    #term{q=Q, options=Opts}=T,
    Field = Schema:find_field(FieldName),
    case proplists:get_value(wildcard, Opts) of
        undefined ->
            F;
        all ->
            Sz = size(Q),
            {Start, End} = {<<Q:Sz/binary>>, <<Q:Sz/binary, 255:8/integer>>},
            case range_to_terms(Schema:name(), FieldName, Start,
                                End, all, Schema:field_type(Field)) of
                [] ->
                    skip;
                Results ->
                    #lor{ops=terms_from_range_results(Schema, FieldName, Results)}
            end;
        one ->
            Sz = size(Q),
            {Start, End} = {<<Q:Sz/binary>>, <<Q:Sz/binary, 255:8/integer>>},
            case range_to_terms(Schema:name(), FieldName, Start,
                                End, Sz + 1, Schema:field_type(Field)) of
                [] ->
                    skip;
                Results ->
                    #lor{ops=terms_from_range_results(Schema, FieldName, Results)}
            end
    end;
wildcard_range_to_or(#term{q={Index, FieldName, Q}, options=Opts}=T, Schema) ->
    Field = Schema:find_field(FieldName),
    case proplists:get_value(wildcard, Opts) of
        undefined ->
            T;
        all ->
            Sz = size(Q),
            {Start, End} = {<<Q:Sz/binary>>, <<Q:Sz/binary, 255:8/integer>>},
            case range_to_terms(Index, FieldName, Start,
                                End, all, Schema:field_type(Field)) of
                [] ->
                    skip;
                Results ->
                    #lor{ops=terms_from_range_results(Schema, FieldName, Results)}
            end;
        one ->
            Sz = size(Q),
            {Start, End} = {<<Q:Sz/binary>>, <<Q:Sz/binary, 255:8/integer>>},
            case range_to_terms(Index, FieldName, Start,
                                End, Sz + 1, Schema:field_type(Field)) of
                [] ->
                    skip;
                Results ->
                    #lor{ops=terms_from_range_results(Schema, FieldName, Results)}
            end
    end;
wildcard_range_to_or(Op, _Schema) ->
    Op.

%% Detect and annotate facets & node weights
insert_facet_or_weight(T, Schema) when is_record(T, term) ->
    FieldName = Schema:default_field(),
    add_facet_or_weight(T, {FieldName, Schema});
insert_facet_or_weight(#field{field=FieldName, ops=T}, Schema) when is_record(T, term) ->
    T1 = add_facet_or_weight(T, {FieldName, Schema}),
    #field{field=FieldName, ops=T1};
insert_facet_or_weight(#field{field=FieldName, ops=Ops}=F, Schema) when is_list(Ops) ->
    NewOps = visit(Ops, ?VISITOR(add_facet_or_weight, {FieldName, Schema}), true),
    F#field{ops=NewOps};
insert_facet_or_weight(Op, _Schema) ->
    Op.

add_facet_or_weight(#term{q=Q, options=Opts}=T, {FieldName, Schema}) ->
    Field = Schema:find_field(FieldName),
    NewOpts = case Schema:is_field_facet(Field) of
                  true ->
                      case proplists:get_value(facet, Opts) of
                          undefined ->
                              [facet|Opts];
                          _ ->
                              Opts
                      end;
                  false ->
                      Text = case Q of
                                 {_, _, Q1} ->
                                     Q1;
                                 _ ->
                                     Q
                             end,
                      node_weights_for_term(Schema:name(), FieldName, Text) ++ Opts
              end,
    T#term{options=NewOpts};
add_facet_or_weight(Op, _) ->
    Op.

%% Nested bool flattening
flatten_bool(#land{ops=Ops}=Bool, Schema) ->
    case length(Ops) == 1 of
        true ->
            [Op] = Ops,
            case is_record(Op, land) of
                true ->
                    flatten_bool(Op, Schema);
                false ->
                    Bool
            end;
        false ->
            Bool
    end;
flatten_bool(#lor{ops=Ops}=Bool, Schema) ->
    case length(Ops) == 1 of
        true ->
            [Op] = Ops,
            case is_record(Op, lor) of
                true ->
                    flatten_bool(Op, Schema);
                false ->
                    Bool
            end;
        false ->
            Bool
    end;
flatten_bool(Op, _Schema) ->
    Op.

%% Term conversion
convert_terms({field, Name, Body, _Opts}, Schema) when is_list(Body) ->
    visit(Body, ?VISITOR(convert_field_terms, {Name, Schema}), true);
convert_terms({field, Name, Body, _Opts}, Schema) when is_record(Body, phrase) ->
    convert_field_terms(Body, {Name, Schema});
convert_terms({field, Name, {term, Body, Opts}, _}, Schema) ->
    #term{q={Schema:name(), Name, Body}, options=Opts};
convert_terms({field, Name, Body, _}, _Schema) ->
    #field{field=Name, ops=Body};
convert_terms({term, Body, Opts}, Schema) ->
    #term{q={Schema:name(), Schema:default_field(), Body}, options=Opts};
convert_terms(#phrase{phrase=Phrase0, props=Props}=Op, Schema) ->
    Phrase = list_to_binary([C || C <- binary_to_list(Phrase0),
                                  C /= 34]), %% double quotes
    BQ = proplists:get_value(base_query, Props),
    {Mod, BQ1} = case is_tuple(BQ) of
                     true ->
                         {riak_search_op_term, BQ};
                     false ->
                         case hd(BQ) of
                             {land, [Term]} ->
                                 {riak_search_op_term, Term};
                             {land, Terms} ->
                                 {riak_search_op_node, {land, Terms}}
                         end
                 end,
    [BQ2] = preplan([BQ1], Schema),
    Props1 = proplists:delete(base_query, Props),
    Props2 = [{base_query, BQ2},
              {op_mod, Mod}] ++ Props1,
    Op#phrase{phrase=Phrase, props=Props2};
convert_terms(Node, _Schema) ->
    Node.

convert_field_terms({term, Body, Opts}, {FieldName, Schema}) ->
    #term{q={Schema:name(), FieldName, Body}, options=Opts};
convert_field_terms({phrase, Phrase0, Props}=Op, {FieldName, Schema}) ->
    Phrase = list_to_binary([C || C <- binary_to_list(Phrase0),
                                  C /= 34]), %% double quotes
    BQ = proplists:get_value(base_query, Props),
    {Mod, BQ1} = case is_tuple(BQ) of
                     true ->
                         {riak_search_op_term, {field, FieldName, BQ, Props}};
                     false ->
                         case hd(BQ) of
                             {land, [T]} ->
                                 {riak_search_op_term, {field, FieldName, T, Props}};
                             {land, Terms} ->
                                 F = fun(Term) -> {field, FieldName, Term, Props} end,
                                 {riak_search_op_node, {land, [F(T) || T <- Terms]}}
                         end
                 end,
    [BQ2] = preplan([BQ1], Schema),
    Props1 = proplists:delete(base_query, Props),
    Props2 = [{base_query, BQ2},
              {op_mod, Mod}] ++ Props1,
    Op#phrase{phrase=Phrase, props=Props2};
convert_field_terms(Op, _) ->
    Op.


%% AST traversal logic
visit(AST, Callback, FollowSubTrees) ->
    visit(AST, Callback, FollowSubTrees, []).

visit([], _Callback, _FollowSubTrees, Accum) ->
    lists:flatten(lists:reverse(Accum));
visit([{Type, Nodes}|T], Callback, FollowSubTrees, Accum) when Type =:= land;
                                                               Type =:= lor ->
    NewNodes = case FollowSubTrees of
                   true ->
                       visit(Nodes, Callback, FollowSubTrees, []);
                   false ->
                       Nodes
               end,
    case Callback({Type, NewNodes}) of
        skip ->
            visit(T, Callback, FollowSubTrees, Accum);
        H1 ->
            visit(T, Callback, FollowSubTrees, [H1|Accum])
    end;
visit([{Type, MaybeNodeList}|T], Callback, FollowSubTrees, Accum) when Type =:= lnot ->
    NewNodes = case is_list(MaybeNodeList) =:= true andalso FollowSubTrees =:= true of
                   true ->
                       visit(MaybeNodeList, Callback, FollowSubTrees, []);
                   false ->
                       MaybeNodeList
               end,
    case Callback({Type, NewNodes}) of
        skip ->
            visit(T, Callback, FollowSubTrees, Accum);
        H1 ->
            visit(T, Callback, FollowSubTrees, [H1|Accum])
    end;
visit([H|T], Callback, FollowSubTrees, Accum) ->
    H1 = Callback(H),
    case Callback(H) of
        skip ->
            visit(T, Callback, FollowSubTrees, Accum);
        H1 ->
            visit(T, Callback, FollowSubTrees, [H1|Accum])
    end.

%% Misc. helper functions

%% Expand a range to a list of terms.  Handle the special cases for integer ranges
%% for negative numbers.  -0010 TO -0005 needs to be swapped to -0005 TO -0010.
%% If one number is positive and the other is negative then the range query needs
%% to be broken into a positive search and a negative search
range_to_terms(Index, Field, StartTerm, EndTerm, Size, integer) ->
    StartPolarity = hd(StartTerm),
    EndPolarity = hd(EndTerm),
    case {StartPolarity,EndPolarity} of
        {$-,$-} ->
            call_info_range(Index, Field, EndTerm, StartTerm, Size);
        {$-,_} ->
            Len = length(StartTerm),
            MinusOne = make_minus_one(Len),
            Zero = make_zero(Len),
            call_info_range(Index, Field, MinusOne, StartTerm, Size) ++
                call_info_range(Index, Field, Zero, EndTerm, Size);
        {_,$-} ->
            %% Swap the range if "positive TO negative"
            range_to_terms(Index, Field, EndTerm, StartTerm, Size, integer);
        {_,_} ->
            call_info_range(Index, Field, StartTerm, EndTerm, Size)
    end;
range_to_terms(Index, Field, StartTerm, EndTerm, Size, _Type) ->
    call_info_range(Index, Field, StartTerm, EndTerm, Size).

call_info_range(Index, Field, StartTerm, EndTerm, Size) when StartTerm > EndTerm ->
    call_info_range(Index, Field, EndTerm, StartTerm, Size);
call_info_range(Index, Field, StartTerm, EndTerm, Size) ->
    {ok, Results} = riak_search:info_range(Index, Field, StartTerm, EndTerm, Size),
    Results.

make_minus_one(1) ->
    throw({unhandled_case, make_minus_one});
make_minus_one(2) ->
    "-1";
make_minus_one(Len) ->
    "-" ++ string:chars($0, Len-2) ++ "1".

make_zero(Len) ->
    string:chars($0, Len).

normalize_range({Index, Field, StartTerm}, {Index, Field, EndTerm}, Inclusive) ->
    {StartTerm1, EndTerm1} = case Inclusive of
        true -> {StartTerm, EndTerm};
        false ->{binary_inc(StartTerm, +1), binary_inc(EndTerm, -1)}
    end,
    {Index, Field, StartTerm1, EndTerm1, all}.

%% Uncomment these when merge_index supports wildcarding
%% normalize_range({Index, Field, Term}, wildcard_all, _Inclusive) ->
%%     {StartTerm, EndTerm} = wildcard(Term),
%%     {Index, Field, StartTerm, EndTerm, all};

%% normalize_range({Index, Field, Term}, wildcard_one, _Inclusive) ->
%%     {StartTerm, EndTerm} = wildcard(Term),
%%     {Index, Field, StartTerm, EndTerm, typesafe_size(Term) + 1};

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

range_start(#inclusive_range{start_op=Op}) ->
    Op;
range_start(#exclusive_range{start_op=Op}) ->
    Op.

range_end(#inclusive_range{end_op=Op}) ->
    Op;
range_end(#exclusive_range{end_op=Op}) ->
    Op.

range_type(#inclusive_range{}) ->
    inclusive;
range_type(#exclusive_range{}) ->
    exclusive.

terms_from_range_results(Schema, FieldName, Results) ->
    F = fun({Term, Node, Count}, Acc) ->
                Opt = {node_weight, Node, Count},
                case gb_trees:lookup(Term, Acc) of
                    none ->
                        gb_trees:insert(Term, [Opt], Acc);
                    {value, Options} ->
                        gb_trees:update(Term, [Opt|Options], Acc)
                end
        end,
    Results1 = lists:foldl(F, gb_trees:empty(), lists:flatten(Results)),
    Results2 = gb_trees:to_list(Results1),
    Field = Schema:find_field(FieldName),
    IsFacet = Schema:is_field_facet(Field),
    F1 = fun({Q, Options}) ->
                 Options1 = if
                                IsFacet =:= true ->
                                    [facet|Options];
                                true ->
                                    node_weights_for_term(Schema:name(),
                                                          FieldName, Q) ++ Options
                            end,
                 #term{q={Schema:name(), FieldName, Q}, options=Options1} end,
    [F1(X) || X <- Results2].

node_weights_for_term(IndexName, FieldName, Term) ->
    {ok, Weights0} = riak_search:info(IndexName, FieldName, Term),
    [{node_weight, {Node, Count}} || {_, Node, Count} <- Weights0].

find_heaviest_node(Ops) ->
    NodeWeights = collect_node_weights(Ops, []),
    %% Sort weights in descending order
    F = fun({_, Weight1}, {_, Weight2}) ->
                Weight1 >= Weight2 end,
    case NodeWeights of
        [] ->
            Node = hd(Ops),
            Node#node.node;
        _ ->
            {Node, _Weight} = hd(lists:sort(F, NodeWeights)),
            Node
    end.

collect_node_weights([], Accum) ->
    Accum;
collect_node_weights([#term{options=Opts}|T], Accum) ->
    Weights = proplists:get_all_values(node_weight, Opts),
    F = fun(W, Acc) ->
                case lists:member(W, Acc) of
                    false ->
                        [W|Acc];
                    true ->
                        Acc
                end end,
    collect_node_weights(T, lists:foldl(F, Accum, Weights));
collect_node_weights([#lnot{ops=Ops}|T], Accum) ->
    NewAccum = collect_node_weights(Ops, Accum),
    collect_node_weights(T, NewAccum);
collect_node_weights([_|T], Accum) ->
    collect_node_weights(T, Accum).
