%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_inlines).
-export([passes_inlines/3]).
-include("riak_search.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

%% Convert all fields to list.
-import(riak_search_utils, [to_list/1, to_binary/1]).

-record(state, { 
          schema="", 
          field=""
         }).

passes_inlines(_Schema, _Props, []) ->
    true;
passes_inlines(Schema, Props, FilterOps) ->
    State = #state { schema=Schema },
    passes_inlines_1(Props, FilterOps, State).

passes_inlines_1(Props, Ops, State) when is_list(Ops) ->
    Schema = State#state.schema,
    case Schema:default_op() of
        'and' -> NewOp = #intersection { ops=Ops };
        _  -> NewOp = #union { ops=Ops }
    end,
    passes_inlines_1(Props, NewOp, State);

passes_inlines_1(Props, #scope { field=Field, ops=Ops }, State) ->
    NewField = riak_search_utils:coalesce(Field, State#state.field),
    NewState = State#state { field=to_binary(NewField) },
    passes_inlines_1(Props, Ops, NewState);
      
passes_inlines_1(Props, #intersection { ops=Ops }, State) ->
    F = fun(X) -> passes_inlines_1(Props, X, State) end,
    lists:all(F, Ops);

passes_inlines_1(Props, #union { ops=Ops }, State) -> 
    F = fun(X) -> passes_inlines_1(Props, X, State) end,
    lists:any(F, Ops);

passes_inlines_1(Props, #group { ops=Ops }, State) -> 
    Schema = State#state.schema,
    case Schema:default_op() of
        'and' -> 
            passes_inlines_1(Props, #intersection { ops=Ops }, State);
        'or' -> 
            passes_inlines_1(Props, #intersection { ops=Ops }, State)
    end;

passes_inlines_1(Props, #negation { op=Op }, State) -> 
    not passes_inlines_1(Props, Op, State);

passes_inlines_1(Props, #term { s=Term }, State) -> 
    Field = State#state.field,
    Terms = proplists:get_value(Field, Props, []),
    lists:member(Term, Terms);

passes_inlines_1(Props, Op = #string {}, State) -> 
    %% The following code is adapted from riak_search_op_string...
    Schema = State#state.schema,
    FieldName = State#state.field,
    TermString = to_binary(Op#string.s),

    %% Analyze the string and call preplan to get the #term properties.
    {ok, Terms} = riak_search_op_string:analyze_term(Schema, FieldName, TermString),
    WildcardType = riak_search_op_string:detect_wildcard(Op#string.flags),
    ProximityVal = riak_search_op_string:detect_proximity_val(Op#string.flags),
    case Terms of
        [skip] ->
            throw({error, stopword_not_allowed_in_filter, TermString});
        [Term] when WildcardType == none -> 
            %%  Match a single term...
            passes_inlines_1(Props, #term { s=Term }, State);
        [Term] when WildcardType == glob -> 
            %% Match a '*' wildcard...
            {FromTerm, ToTerm} = riak_search_op_string:calculate_range(Term, all),
            passes_range(Props, FieldName, FromTerm, ToTerm, all);
        [Term] when WildcardType == char -> 
            %% Match a '?' wildcard...
            {FromTerm, ToTerm} = riak_search_op_string:calculate_range(Term, single),
            passes_range(Props, FieldName, FromTerm, ToTerm, erlang:size(Term) + 1);
        _ when is_integer(ProximityVal) ->
            throw({error, proximity_not_allowed_in_filter, TermString});
        _ when ProximityVal == undefined ->
            throw({error, phrase_not_allowed_in_filter, TermString})
    end;

passes_inlines_1(Props, Op = #range {}, State) -> 
    %% The following code is adapted from riak_search_op_range...
    Schema = State#state.schema,
    FieldName = State#state.field,

    %% Parse the FromTerms, ensure that there is only 1, otherwise throw an error.
    {FromBorder, FromString} = Op#range.from,
    {ok, FromTerms} = riak_search_op_string:analyze_term(Schema, FieldName, to_binary(FromString)),
    length(FromTerms) == 1 orelse throw({error, too_many_terms, FromTerms}),
    FromTerm = hd(FromTerms),
    FromTerm /= skip orelse throw({error, stopword_not_allowed_in_range, FromString}),

    %% Parse the ToTerms, ensure that there is only 1, otherwise throw an error.
    {ToBorder, ToString} = Op#range.to,
    {ok, ToTerms} = riak_search_op_string:analyze_term(Schema, FieldName, to_binary(ToString)),
    length(ToTerms) == 1 orelse throw({error, too_many_terms, ToTerms}),
    ToTerm = hd(ToTerms),
    ToTerm /= skip orelse throw({error, stopword_not_allowed_in_range, ToString}),

    %% Check if the current field is an integer. If so, then we'll
    %% need to OR together two range ops.
    Field = Schema:find_field(FieldName),
    DifferentSigns = riak_search_op_range:is_negative(FromTerm) xor riak_search_op_range:is_negative(ToTerm),
    case Schema:field_type(Field) of
        integer when DifferentSigns ->
            %% Create two range operations, one on the negative side,
            %% one on the positive side. Don't need to worry about
            %% putting the terms in order here, this is taken care of
            %% by the passes_range/2 function
            passes_range(Props, FieldName, {FromBorder, FromTerm}, {inclusive, riak_search_op_range:to_zero(FromTerm)}, all) 
                orelse
                passes_range(Props, FieldName, {inclusive, riak_search_op_range:to_zero(ToTerm)}, {ToBorder,ToTerm}, all);
        _ ->
            passes_range(Props, FieldName, {FromBorder, FromTerm}, {ToBorder, ToTerm}, all)
    end;

passes_inlines_1(_Props, Op, _State) ->
    throw({riak_search_inlines, unexpected_operation, Op}).

passes_range(Props, Field, From, To, Size) ->
    {NewFrom, NewTo} = riak_search_op_range_sized:correct_term_order(From, To),
    passes_range_inner(Props, Field, NewFrom, NewTo, Size).
passes_range_inner(Props, Field, {FromBorder, FromTerm}, {ToBorder, ToTerm}, Size) ->
    F = fun(X) -> 
                (X > FromTerm orelse (FromBorder == inclusive andalso X == FromTerm))
                    andalso (X < ToTerm orelse (ToBorder == inclusive andalso X == ToTerm))
                    andalso (Size == all orelse length(to_list(X)) == Size)
        end,
    Terms = proplists:get_value(Field, Props, []),
    
    lists:any(F, Terms).

-ifdef(TEST).


-define(INDEX, <<"riak_search_inlines">>).

%% Return the schema...
schema() ->
    {schema,
     [
      {version, "1.1"},
      {n_val, 3},
      {default_field, "value"},
      {analyzer_factory, {erlang, text_analyzers, whitespace_analyzer_factory}}
     ],
     [
      {dynamic_field, [
                       {name, "regular_*"},
                       {type, string},
                       {analyzer_factory, {erlang, text_analyzers, whitespace_analyzer_factory}}
                      ]},
      {dynamic_field, [
                       {name, "int_*"},
                       {type, integer},
                       {inline, true}
                      ]},
      {dynamic_field, [
                       {name, "*"},
                       {type, string},
                       {inline, true}
                      ]}
     ]
    }.

doc(Schema) ->
    Fields = [
              {<<"regular_field">>, <<"value">>},
              {<<"field1">>, <<"value1">>},
              {<<"field2">>, <<"value2">>},
              {<<"field3">>, <<"value3">>},
              {<<"field4">>, <<"This is an inline field with multiple postings.">>},
              {<<"int_field1">>, <<"0">>},
              {<<"int_field2">>, <<"50">>},
              {<<"int_field3">>, <<"-50">>}
             ],
    riak_indexed_doc:new(Schema, <<"doc1">>, Fields, []).


term1_test() ->
    test_helper("field1:value1", true).

term2_test() ->
    test_helper("int_field1:0", true).

term3_test() ->
    test_helper("int_field2:50", true).

term4_test() ->
    test_helper("int_field3:'-50'", true).

term5_test() ->
    test_helper("int_field1:999", false).

or1_test() ->
    test_helper("field1:nomatch OR field1:value1", true).

or2_test() ->
    test_helper("field1:value1 OR field1:nomatch", true).

and1_test() ->
    test_helper("field1:value1 AND field2:value2", true).

and2_test() ->
    test_helper("field1:value1 AND int_field2:50", true).

group1_test() ->
    test_helper("(field1:value1 OR field1:nomatch) OR (field2:nomatch AND field3:nomatch)", true).

group2_test() ->
    test_helper("(field1:value1 OR field1:nomatch) AND (field2:value2 AND field3:value3)", true).

group3_test() ->
    test_helper("(field1:value1 OR field1:nomatch) AND (field2:value2 AND field3:nomatch)", false).

range1_test() ->
    test_helper("field1:[value1 TO value2]", true).

range2_test() ->
    test_helper("field1:{value1 TO value2}", false).

range3_test() ->
    test_helper("int_field1:[0 TO 50]", true).

range4_test() ->
    test_helper("int_field1:[50 TO 0]", true).

range5_test() ->
    test_helper("int_field1:['-50' TO 50]", true).

range6_test() ->
    test_helper("int_field1:{'-50' TO 50}", true).

range7_test() ->
    test_helper("int_field1:{0 TO 50}", false).

range8_test() ->
    test_helper("int_field1:{50 TO 0}", false).

range9_test() ->
    test_helper("int_field1:{'-50' TO 0}", false).

range10_test() ->
    test_helper("int_field1:{0 TO '-50'}", false).

wildcard1_test() ->
    test_helper("field1:value*", true).

wildcard2_test() ->
    test_helper("field1:valu*", true).

wildcard3_test() ->
    test_helper("field1:val*", true).

wildcard4_test() ->
    test_helper("field1:va*", true).

%% Should work, but doesn't. Problem in lucene_parser.
%% wildcard5_test() ->
%%     test_helper("field1:v*", true).

%% Should work, but doesn't. Problem in lucene_parser.
%% wildcard6_test() ->
%%     test_helper("field1:*", true).

wildcard7_test() ->
    test_helper("field1:valueZZZ*", false).

single1_test() ->
    test_helper("field1:value?", true).

single2_test() ->
    test_helper("field2:value?", true).

single3_test() ->
    test_helper("field2:valu?", false).

single4_test() ->
    test_helper("field2:valueZZZ?", false).

%% Ugh, theres are few things I don't like about this but for the sake
%% of just getting the tests to run I'll bypass `riak_search_config'
%% and `riak_search_client'.  The main problem is that many funs are
%% impure (i.e. rely on an actual running Riak instance) when they
%% don't need to be.  This is because they call
%% `riak_search_config:get_schema' which requires a running instance
%% if you pass in an index.  Furthermore the put_schema and client
%% require a running instance.  A much saner way to do this would be
%% to get the schema at the outskirt of the API and thread the
%% resulting datastructure through the rest of the calls.  This way
%% the functions are once again pure and easily tested.
test_helper(Query, ExpectedResult) ->
    %% Set the schema...
    RawSchema = iolist_to_binary(io_lib:format("~p.", [schema()])),
    {ok, RawSchema2} = riak_search_utils:consult(RawSchema),
    {ok, Schema} = riak_search_schema_parser:from_eterm(?INDEX, RawSchema2),
    
    %% Create the doc, analyze, and get the properties from the first
    %% posting...
    IdxDoc = riak_indexed_doc:analyze(doc(Schema)),
    Postings = riak_indexed_doc:postings(IdxDoc),
    Props = element(5, hd(Postings)),

    {ok, FilterOps} = lucene_parser:parse(
                        riak_search_utils:to_list(Schema:name()),
                        riak_search_utils:to_list(Schema:default_field()),
                        riak_search_utils:to_list(Query)),

    ?assertEqual(passes_inlines(Schema, Props, FilterOps), ExpectedResult).
           

-endif.
