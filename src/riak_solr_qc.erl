%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_qc).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("riak_search.hrl").

-compile(export_all).

-define(SCHEMA, "search").
-record(state, { docs=[] }).
-record(doc, { id, fields=[] }).


-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

test() -> 
    test(100).
test(N) ->
    eqc:quickcheck(numtests(N, prop_test())),
    ok.



%% FUNCTIONS

index_doc(Doc) ->
    {ok, Schema} = riak_search_config:get_schema(?SCHEMA),
    XML = to_xml_add_doc(Doc),
    {ok, Client} = get_solr_client(),
    {ok, Command, ParsedDocs} = Client:parse_solr_xml(Schema, XML),
    Client:run_solr_command(Schema, Command, ParsedDocs).

delete_doc_by_id(DocID) ->
    {ok, Schema} = riak_search_config:get_schema(?SCHEMA),
    XML = to_xml_delete_doc_by_id(DocID),
    {ok, Client} = get_solr_client(),
    {ok, Command, ParsedDocs} = Client:parse_solr_xml(Schema, XML),
    Client:run_solr_command(Schema, Command, ParsedDocs).

delete_doc_by_query(Query) ->
    {ok, Schema} = riak_search_config:get_schema(?SCHEMA),
    StringQuery = to_query_string(Query),
    XML = to_xml_delete_doc_by_query(StringQuery),
    {ok, Client} = get_solr_client(),
    {ok, Command, ParsedDocs} = Client:parse_solr_xml(Schema, XML),
    Client:run_solr_command(Schema, Command, ParsedDocs).

search_docs(_) ->
    ok.


%% STATES

running(S) ->
    [
        {running, {call, ?MODULE, index_doc, [
            oneof([new_doc(), existing_doc(S), duplicate_id_doc(S)])]}},
        {running, {call, ?MODULE, delete_doc_by_id, [
            oneof([new_doc_id(), existing_doc_id(S)])]}},
        {running, {call, ?MODULE, delete_doc_by_query, [
            generate_search_terms(S)]}},
        {running, {call, ?MODULE, search_docs, [
            generate_search_terms(S)]}}
    ].
    
initial_state() -> running.
initial_state_data() -> #state{ docs=[] }.


%% STATE TRANSFORMS

%% Add a new or existing doc to the "Index"...
next_state_data(running, running, State, _Result, {call, ?MODULE, index_doc, [Doc]}) ->
    NewDocs = lists:keystore(Doc#doc.id, #doc.id, State#state.docs, Doc),
    State#state { docs=NewDocs };

%% Remove an old or existing doc from the Index...
next_state_data(running, running, State, _Result, {call, ?MODULE, delete_doc_by_id, [DocID]}) ->
    NewDocs = lists:keydelete(DocID, #doc.id, State#state.docs),
    State#state { docs=NewDocs };

%% Remove an old or existing doc from the Index...
next_state_data(running, running, State, _Result, {call, ?MODULE, delete_doc_by_query, [Query]}) ->
    LocalResults = local_query(Query, State),
    NewDocs = State#state.docs -- LocalResults,
    State#state { docs=NewDocs };

%% Search docs for some terms. 
next_state_data(running, running, State, _SearchTerms, {call, ?MODULE, search_docs, _}) ->
    %% Not yet handled.
    State.



%% PRECONDITIONS

precondition(_From,_To,_S,{call,_,_,_}) ->
    true.



%% POSTCONDITIONS

postcondition(running, running, _State, {call, ?MODULE, index_doc, [Doc]}, _Res) ->
    %% Get the stored document...
    {ok, Client} = get_client(),
    F = fun() ->
        case Client:get_idx_doc(?SCHEMA, Doc#doc.id) of
            {error, notfound} -> 
                IdxDoc = undefined, % make compiler happy.
                throw({document_not_found, Doc});
            IdxDoc -> 
                IdxDoc
        end,

        %% Check that the ids match...
        (Doc#doc.id == IdxDoc#riak_idx_doc.id) orelse throw({document_id_conflict, Doc, IdxDoc}),

        %% Check that the field names match...
        Fields = [{X, string:join(Y, " ")} || {X, Y} <- Doc#doc.fields],
        IdxFields = IdxDoc#riak_idx_doc.fields,
        (lists:sort(Fields) == lists:sort(IdxFields)) orelse throw({document_field_conflict, Doc, IdxDoc}),
        true
    end,
    %% Try up to 5 times before finally giving up.
    try_real_hard(F, 5);

postcondition(running, running, _State, {call, ?MODULE, delete_doc_by_id, [DocID]}, _Res) ->
    %% Ensure that the document was deleted. 
    {ok, Client} = get_client(),
    F = fun() -> 
        case Client:get_idx_doc(?SCHEMA, DocID) of
            {error, notfound} -> true;
            IdxDoc -> throw({document_not_deleted, IdxDoc})
        end
    end,
    %% Try up to 5 times before finally giving up.
    try_real_hard(F, 5);

postcondition(running, running, State, {call, ?MODULE, delete_doc_by_query, [Query]}, _Res) ->
    StringQuery = to_query_string(Query),

    %% Run the query...
    {ok, Client} = get_client(),
    {ok, QueryOps} = Client:parse_query(?SCHEMA, StringQuery),
    {_Length, Results} = Client:search_doc(?SCHEMA, QueryOps, 0, infinity, 10000),
    IdxDocIDs = [X#riak_idx_doc.id || X <- Results],

    %% Get the local doc ids...
    LocalDocIDs = [X#doc.id || X <- State#state.docs],
    
    %% After the delete has run, there should be no overlap...
    case sets:is_disjoint(sets:from_list(IdxDocIDs), sets:from_list(LocalDocIDs)) of
        true -> 
            true;
        false ->
            throw({delete_doc_by_query, Query, Results, State#state.docs})
    end;

postcondition(running, running, State, {call, ?MODULE, search_docs, [Query]}, _Res) ->
    StringQuery = to_query_string(Query),

    %% Run the query...
    {ok, Client} = get_client(),
    {ok, QueryOps} = Client:parse_query(?SCHEMA, StringQuery),
    {_Length, Results} = Client:search(?SCHEMA, QueryOps, 0, infinity, 10000),
    IdxDocIDs = [Value || {Value, _Props} <- Results],

    %% Check against our results. 
    LocalResults = local_query(Query, State),
    LocalDocIDs = [X#doc.id || X <- LocalResults],

    %% Everything in DocIDs should also be in IdxDocIDs.
    case sets:is_subset(sets:from_list(LocalDocIDs), sets:from_list(IdxDocIDs)) of
        true -> 
            true;
        false ->
            throw({search_docs, IdxDocIDs, LocalDocIDs})
    end;

postcondition(running, running, _S, {call, _, _, _}, _Res) ->
    true.


prop_test() ->
    ?FORALL(Cmds,commands(?MODULE),
        begin
            {H,S,Res} = run_commands(?MODULE,Cmds),
            ?WHENFAIL(
                io:format("History: ~p\nState: ~p\nRes: ~p\n",[H,S,Res]),
                Res == ok)
    end).

%% Weight for transition (this callback is optional).
%% Specify how often each transition should be chosen
weight(_From,_To,{call,_,_,_}) ->
    1.

%% GENERATORS

new_doc_id() -> 
    non_empty(list(choose($a, $z))).

existing_doc_id(S) -> 
    ?LET(Doc, oneof(S#state.docs), Doc#doc.id).

new_doc() -> 
    #doc {
        id = new_doc_id(),
        fields=new_doc_fields()
    }.

existing_doc(S) -> 
    oneof(S#state.docs).

duplicate_id_doc(S) -> 
    #doc {
        id = ?LET(Doc, oneof(S#state.docs), Doc#doc.id),
        fields=new_doc_fields()
    }.

new_doc_fields() ->
    oneof([
        [],
        [{?LAZY(field_name()), ?LAZY(field_value())}|?LAZY(new_doc_fields())]
    ]).

field_name() -> 
    oneof([
        "title",
        "tags",
        "body",
        "extra1",
        "extra2"
    ]).

field_value() ->
    oneof([
        [],
        [term()|?LAZY(field_value())]
    ]).

%% Generate a list of AND'ed field:terms pulling from existing docs.
generate_search_terms(S) -> 
    oneof([
        [generate_search_term(S)],
        [generate_search_term(S)|?LAZY(generate_search_terms(S))]
    ]).

generate_search_term(S) ->
    ?LET(Doc, oneof(S#state.docs), begin
        ?LET({FieldName, FieldValue}, oneof(Doc#doc.fields), begin
            {FieldName, oneof(FieldValue)}
        end)
    end).


%% Convert query in form [{Field, Term}] to a string of the form
%% "Field1:Term1 AND Field2:Term2 AND ..."
to_query_string(Query) ->
    F1 = fun({Field, Term}) ->
        Field ++ ":" ++ Term
    end,
    string:join([F1(X) || X <- Query], " AND ").

local_query(Query, State) ->
    F1 = fun(Doc) ->
        F2 = fun({Field, Term}) ->
            FieldValue = proplists:get_value(Field, Doc#doc.fields, []),
            lists:member(Term, FieldValue)
        end,
        lists:all(F2, Query)
    end,
    lists:filter(F1, State#state.docs).
    

%% Given a doc, create a binary XML document.
to_xml_add_doc(Doc) when is_record(Doc, doc) ->
    F = fun({FieldName, FieldValue}) ->
        [
            "<field name=\"", FieldName, "\">",
            case FieldValue == [] of
                true -> "";
                false -> string:join(FieldValue, " ")
            end,
            "</field>"
        ]
    end,
    L = [
        "<add>",
        "<doc>",
        "<field name=\"id\">", Doc#doc.id, "</field>",
        [F(X) || X <- Doc#doc.fields],
        "</doc>",
        "</add>"
    ],
    list_to_binary(L).

to_xml_delete_doc_by_id(DocID) ->
    L = [
        "<delete>",
        "<id>", DocID, "</id>",
        "</delete>"        
    ],
    list_to_binary(L).

to_xml_delete_doc_by_query(StringQuery) ->
    L = [
        "<delete>",
        "<query>", StringQuery, "</query>",
        "</delete>"                
    ],
    list_to_binary(L).

get_solr_client() ->
    riak_search:local_solr_client().

get_client() ->
    riak_search:local_client().

%% Try to run the provided function. If it errors, wait a small amount
%% of time and then try again.
try_real_hard(F, Count) ->
    try F() 
    catch Type : Error ->
        case Count of
            0 -> erlang:Type(Error);
            _ -> 
                timer:sleep(100),
                try_real_hard(F, Count - 1)
        end
    end.

term() ->
    oneof([
        "bus",
        "Busaos",
        "busby",
        "buscarl",
        "buscarle",
        "bush",
        "bushbeater",
        "bushbuck",
        "bushcraft",
        "bushed",
        "bushel",
        "busheler",
        "bushelful",
        "bushelman",
        "bushelwoman",
        "busher"
        "bushfighter",
        "bushfighting",
        "bushful",
        "bushhammer",
        "bushi",
        "bushily",
        "bushiness",
        "bushing",
        "bushland",
        "bushless",
        "bushlet",
        "bushlike",
        "bushmaker",
        "bushmaking",
        "Bushman",
        "bushmanship",
        "bushmaster",
        "bushment",
        "Bushongo",
        "bushranger",
        "bushranging",
        "bushrope",
        "bushveld",
        "bushwa",
        "bushwhack",
        "bushwhacker",
        "bushwhacking",
        "bushwife",
        "bushwoman",
        "bushwood",
        "bushy",
        "busied",
        "busily",
        "busine",
        "business",
        "businesslike",
        "businesslikeness",
        "businessman",
        "businesswoman",
        "busk",
        "busked",
        "busker",
        "busket",
        "buskin",
        "buskined",
        "buskle",
        "busky",
        "busman",
        "buss",
        "busser",
        "bussock",
        "bussu",
        "bust",
        "bustard",
        "busted",
        "bustee",
        "buster",
        "busthead",
        "bustic",
        "busticate",
        "bustle",
        "bustled",
        "bustler",
        "bustling",
        "bustlingly",
        "busy",
        "busybodied",
        "busybody",
        "busybodyish",
        "busybodyism",
        "busybodyness",
        "Busycon",
        "busyhead",
        "busying",
        "busyish",
        "busyness",
        "busywork",
        "but",
        "butadiene",
        "butadiyne",
        "butanal",
        "butane",
        "butanoic",
        "butanol",
        "butanolid",
        "butanolide",
        "butanone",
        "butch",
        "butcher",
        "butcherbird",
        "butcherdom",
        "butcherer",
        "butcheress",
        "butchering",
        "butcherless",
        "butcherliness",
        "butcherly",
        "butcherous",
        "butchery",
        "Bute",
        "Butea",
        "butein",
        "butene",
        "butenyl",
        "Buteo",
        "buteonine",
        "butic",
        "butine",
        "butler",
        "butlerage",
        "butlerdom",
        "butleress",
        "butlerism",
        "butlerlike",
        "butlership",
        "butlery",
        "butment",
        "Butomaceae",
        "butomaceous",
        "Butomus",
        "butoxy",
        "butoxyl",
        "Butsu",
        "butt",
        "butte",
        "butter",
        "butteraceous",
        "butterback",
        "butterball",
        "butterbill",
        "butterbird",
        "butterbox",
        "butterbump",
        "butterbur",
        "butterbush",
        "buttercup",
        "buttered",
        "butterfat",
        "butterfingered",
        "butterfingers",
        "butterfish",
        "butterflower",
        "butterfly",
        "butterflylike",
        "butterhead",
        "butterine",
        "butteriness",
        "butteris",
        "butterjags",
        "butterless",
        "butterlike",
        "buttermaker",
        "buttermaking",
        "butterman",
        "buttermilk",
        "buttermonger",
        "buttermouth",
        "butternose",
        "butternut",
        "butterroot",
        "butterscotch",
        "butterweed",
        "butterwife",
        "butterwoman",
        "butterworker",
        "butterwort",
        "butterwright",
        "buttery",
        "butteryfingered",
        "buttgenbachite",
        "butting",
        "buttinsky",
        "buttle",
        "buttock",
        "buttocked",
        "buttocker",
        "button",
        "buttonball",
        "buttonbur",
        "buttonbush",
        "buttoned",
        "buttoner",
        "buttonhold",
        "buttonholder",
        "buttonhole",
        "buttonholer",
        "buttonhook",
        "buttonless",
        "buttonlike",
        "buttonmold",
        "buttons",
        "buttonweed",
        "buttonwood",
        "buttony",
        "buttress",
        "buttressless",
        "buttresslike",
        "buttstock",
        "buttwoman",
        "buttwood",
        "butty",
        "buttyman",
        "butyl",
        "butylamine",
        "butylation",
        "butylene",
        "butylic",
        "Butyn",
        "butyne",
        "butyr",
        "butyraceous",
        "butyral",
        "butyraldehyde",
        "butyrate",
        "butyric",
        "butyrically",
        "butyrin",
        "butyrinase",
        "butyrochloral",
        "butyrolactone",
        "butyrometer",
        "butyrometric",
        "butyrone",
        "butyrous",
        "butyrousness",
        "butyryl",
        "Buxaceae",
        "buxaceous",
        "Buxbaumia",
        "Buxbaumiaceae",
        "buxerry",
        "buxom",
        "buxomly",
        "buxomness",
        "Buxus",
        "buy",
        "buyable",
        "buyer",
        "Buyides",
        "buzane",
        "buzylene",
        "buzz",
        "buzzard",
        "buzzardlike",
        "buzzardly",
        "buzzer",
        "buzzerphone",
        "buzzgloak",
        "buzzies",
        "buzzing"
    ]).

-endif. %EQC
