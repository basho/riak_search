%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_ift_server).

-behaviour(gen_server).

%% API
-export([start_link/0,
         create_ift/3,
         find_ift/3,
         find_index/1, find_field/1, find_term/1,
         fold_ifts/6,
         reverse_ift/1,
         term_count/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, { index_id = 0,
                 field_id = 0,
                 term_id  = 0,
                 ift_file }).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create_ift(Index, Field, Term) ->
    IndexID = find_or_create(mi_ift_indices, to_binary(Index)),
    FieldID = find_or_create(mi_ift_fields, to_binary(Field)),
    TermID  = find_or_create(mi_ift_terms, to_binary(Term)),
    ift_pack(IndexID, FieldID, TermID).

find_ift(Index, Field, Term) ->
    IndexID = find(mi_ift_indices, to_binary(Index)),
    FieldID = find(mi_ift_fields, to_binary(Field)),
    TermID  = find(mi_ift_terms, to_binary(Term)),
    ift_pack(IndexID, FieldID, TermID).

find_index(Index) ->
    find(mi_ift_indices, to_binary(Index)).

find_field(Field) ->
    find(mi_ift_fields, to_binary(Field)).

find_term(Term) ->
    find(mi_ift_terms, to_binary(Term)).

fold_ifts(Index, Field, StartTerm, EndTerm, Fun, Acc) ->
    IndexID = find(mi_ift_indices, to_binary(Index)),
    FieldID = find(mi_ift_fields, to_binary(Field)),
    case IndexID /= undefined andalso FieldID /= undefined of
        true ->
            %% Both index ID and field ID are defined; scan terms
            Spec = [{{'$1', '$2'},
                     [{'=<', to_binary(StartTerm), '$1'}, {'=<', '$1', to_binary(EndTerm)}],
                     ['$_']}],
            %% TODO: Compile ets match spec?
            fold_ifts_loop(ets:select(mi_ift_terms, Spec, 100), IndexID, FieldID, Fun, Acc);

        false ->
            %% If either the index or field doesn't exist, don't bother looking
            %% for terms
            Acc
    end.

reverse_ift(IFT) when is_integer(IFT) ->
    {IndexID, FieldID, TermID} = ift_unpack(IFT),
    Index = find(mi_ift_rev_indicies, IndexID),
    Field = find(mi_ift_rev_fields, FieldID),
    Term  = find(mi_ift_rev_terms, TermID),
    {Index, Field, Term}.

term_count() ->
    ets:info(mi_ift_terms, size).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Open two tables for each type of data; a forward mapping (key->ID) and a
    %% reverse mapping (ID->key). Note that these tables are marked as protected
    %% so that the caller can use the table on their own processes. Only writes
    %% come through this server.
    ets:new(mi_ift_indices,      [ordered_set, protected, named_table]),
    ets:new(mi_ift_rev_indicies, [ordered_set, protected, named_table]),
    ets:new(mi_ift_fields,       [ordered_set, protected, named_table]),
    ets:new(mi_ift_rev_fields,   [ordered_set, protected, named_table]),
    ets:new(mi_ift_terms,        [ordered_set, protected, named_table]),
    ets:new(mi_ift_rev_terms,    [ordered_set, protected, named_table]),

    %% Look for the specified IFT file in the data root
    {ok, Root} = application:get_env(merge_index, data_root),
    IftFilename = filename:absname(filename:join(Root, "mi_ift.data")),
    ok = filelib:ensure_dir(IftFilename),
    case file:open(IftFilename, [read, write, append, binary, raw, read_ahead]) of
        {ok, IftFile} ->
            %% TODO: Add error checking and CRC checking on this file
            State = read_ift_file(#state { ift_file = IftFile }),
            {ok, State};

        {error, Reason} ->
            error_logger:error_msg("Failed to open IFT data ~p: ~p\n",
                                   [IftFilename, Reason]),
            {error, Reason}
    end.


handle_call({add, Table, Key}, _From, State) ->
    %% When adding a given key, always attempt an insert_new first to avoid race
    %% conditions where multiple processes look for a key, don't find it and
    %% queue up a {add, ...} message.
    Id = element(table_to_element(Table), State),
    case ets:insert_new(Table, {Key, Id}) of
        false ->
            %% Key was already present in table, return the ID for it
            ExistingId = ets:lookup_element(Table, Key, 2),
            {reply, ExistingId, State};
        true ->
            %% Key wasn't present, update reverse table, write value out to disk
            %% and return updated state
            ets:insert_new(table_to_rev_table(Table), {Id, Key}),
            write_ift_file(State, Table, Key, Id),
            {reply, Id, setelement(table_to_element(Table), State, Id+1)}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

table_to_element(mi_ift_indices) -> #state.index_id;
table_to_element(mi_ift_fields)  -> #state.field_id;
table_to_element(mi_ift_terms)   -> #state.term_id.


table_to_rev_table(mi_ift_indices) -> mi_ift_rev_indicies;
table_to_rev_table(mi_ift_fields)  -> mi_ift_rev_fields;
table_to_rev_table(mi_ift_terms)   -> mi_ift_rev_terms.


block_id_to_table(0) -> mi_ift_indices;
block_id_to_table(1) -> mi_ift_fields;
block_id_to_table(2) -> mi_ift_terms.


table_to_block_id(mi_ift_indices) -> 0;
table_to_block_id(mi_ift_fields)   -> 1;
table_to_block_id(mi_ift_terms)    -> 2.


to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.


find_or_create(Table, Key) ->
    case catch(ets:lookup_element(Table, Key, 2)) of
        {'EXIT', {badarg, _}} ->
            gen_server:call(?MODULE, {add, Table, Key}, infinity);
        Id ->
            Id
    end.

find(Table, Key) ->
    case catch(ets:lookup_element(Table, Key, 2)) of
        {'EXIT', {badarg, _}} ->
            undefined;
        Id ->
            Id
    end.

ift_pack(IndexID, FieldID, TermID)
  when IndexID /= undefined,
       FieldID /= undefined,
       TermID  /= undefined ->
    <<Ift:64/unsigned>> = <<IndexID:16/integer,
                            FieldID:16/integer,
                            TermID:32/integer>>,
    Ift;
ift_pack(_, _, _) ->
    undefined.

ift_unpack(Ift) ->
    <<IndexID:16/integer, FieldID:16/integer, TermID:32/integer>> = <<Ift:64/unsigned>>,
    {IndexID, FieldID, TermID}.


fold_ifts_loop('$end_of_table', _IndexID, _FieldID, _Fn, Acc0) ->
    Acc0;
fold_ifts_loop({Matches, Cont}, IndexID, FieldID, Fn, Acc0) ->
    Acc = fold_term_matches(Matches, IndexID, FieldID, Fn, Acc0),
    fold_ifts_loop(ets:select(Cont), IndexID, FieldID, Fn, Acc).


fold_term_matches([], _IndexID, _FieldID, _Fn, Acc0) ->
    Acc0;
fold_term_matches([{Term, TermID} | Rest], IndexID, FieldID, Fn, Acc0) ->
    IFT = ift_pack(IndexID, FieldID, TermID),
    fold_term_matches(Rest, IndexID, FieldID, Fn, Fn({Term, IFT}, Acc0)).


-define(BLOCK_HEADER_SZ, 9).


read_ift_file(State) ->
    case file:read(State#state.ift_file, ?BLOCK_HEADER_SZ) of
        {ok, <<Header:?BLOCK_HEADER_SZ/bytes>>} ->
            read_ift_file(State, Header);
        eof ->
            State
    end.

read_ift_file(State, <<>>) ->
    State;
read_ift_file(State, <<Type:8/unsigned, Id:32/unsigned, Size:32/unsigned>>) ->
    case file:read(State#state.ift_file, Size + ?BLOCK_HEADER_SZ) of
        {ok, <<Data:Size/bytes, Rest/binary>>} ->
            %% io:format("Loading ~p ~p ~p\n", [Type, Data, Id]),
            Table = block_id_to_table(Type),
            RevTable = table_to_rev_table(Table),
            ets:insert_new(Table, {Data, Id}),
            ets:insert_new(RevTable, {Id, Data}),
            MaxId = erlang:max(element(table_to_element(Table), State), Id),
            S2 = setelement(table_to_element(Table), State, MaxId),
            read_ift_file(S2, Rest);

        eof ->
            State
    end.

write_ift_file(State, Table, Key, Id) ->
    BlockId = table_to_block_id(Table),
    Size = size(Key),
    ok = file:write(State#state.ift_file,
                    [<<BlockId:8, Id:32/unsigned, Size:32/unsigned>>, Key]).



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

g_ift_tuple() ->
    {non_empty(binary()), non_empty(binary()), non_empty(binary())}.

prop_basic_test(Root) ->
    ?FORALL(Tuples, non_empty(list(g_ift_tuple())),
            begin
                %% Stop the server if it's running
                stop_server(),

                %% Delete old file
                file:delete(filename:join(Root, "mi_ift.data")),

                %% Spin up new instance of the server
                {ok, _} = mi_ift_server:start_link(),

                %% Plug each of the tuples into the server, saving the IFTs
                IFTs = [mi_ift_server:create_ift(I, F, T) || {I, F, T} <- Tuples],

                %% Check that the reverse of the IFTs match original tuples
                ?assertEqual(Tuples, [mi_ift_server:reverse_ift(IFT) || IFT <- IFTs]),

                %% Restart the server and verify all the data is still present
                stop_server(),
                {ok, _} = mi_ift_server:start_link(),

                ReloadedIFTs = [mi_ift_server:find_ift(I, F, T) || {I, F, T} <- Tuples],
                ?assertEqual(IFTs, ReloadedIFTs),
                true
            end).


prop_basic_test_() ->
    test_spec("/tmp/mi_ift_server_basic", fun prop_basic_test/1).

test_spec(Root, PropertyFn) ->
    {timeout, 60, fun() ->
                          application:load(merge_index),
                          application:set_env(merge_index, data_root, Root),
                          os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
                          ?assert(eqc:quickcheck(eqc:numtests(250, ?QC_OUT(PropertyFn(Root)))))
                  end}.

stop_server() ->
    case whereis(?MODULE) of
        undefined ->
            ok;
        Pid ->
            Mref = erlang:monitor(process, Pid),
            gen_server:call(Pid, stop, infinity),
            receive
                {'DOWN', Mref, _, _, _} ->
                    ok
            end
    end.


-endif. %EQC
-endif
.
