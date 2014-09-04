%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%%% Riak Search schemas exist in two forms. The "raw schema" is the
%%% schema that can be found in a schema.def file. The raw schema is
%%% saved as a binary in a Riak object in the <<"_rs_schema">>
%%% bucket. It is saved as a binary so that formatting and comments
%%% are preserved.
%%%
%%% The schema returned by get_schema/1 is a result of parsing the raw
%%% schema into a riak_search_schema parameterized module.

-module(riak_search_config).
-include("riak_search.hrl").
-behaviour(gen_server).

-define(SCHEMA_BUCKET, <<"_rs_schema">>).
-define(DEFAULT_SCHEMA, filename:join([code:priv_dir(riak_search), "default.def"])).

%% API
-export([
         clear/0,
         get_all_schemas/0,
         get_raw_schema/1,
         get_schema/1,
         put_raw_schema/2,
         start_link/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {client, schema_table}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Clear cached schemas.
clear() ->
    gen_server:call(?SERVER, clear, infinity).

%% @doc Return list of all `Schemas'.
-spec get_all_schemas() -> Schemas::[any()].
get_all_schemas() ->
    {ok, C} = riak:local_client(),
    {ok, Keys} = C:list_keys(?SCHEMA_BUCKET),
    [S || {ok,S} <- [get_schema(K) || K <- Keys]].

%% Get schema information for the provided index name.
%% @param Schema - Either the name of an index, or a schema record.
%%
%% TODO This spec/fun needs help
-spec get_schema(any()) -> {ok, any()} | {error, badarg} | any().
get_schema(Schema) when is_tuple(Schema) ->
    case element(1, Schema) of
        riak_search_schema ->
            {ok, Schema};
        _ ->
            {error, badarg}
    end;
get_schema(Index) ->
    SchemaName = riak_search_utils:to_binary(Index),
    case ets:lookup(schema_table, SchemaName) of
        [{SchemaName, Schema}] ->
            {ok, Schema};
        _ ->
            gen_server:call(?SERVER, {get_schema, SchemaName}, infinity)
    end.

%% @doc Return the raw schema (as an Erlang term) for a given index.
get_raw_schema(Index) ->
    IndexB = riak_search_utils:to_binary(Index),
    gen_server:call(?SERVER, {get_raw_schema, IndexB}, infinity).

%% @doc Update Riak Search with new schema information.
put_raw_schema(Index, RawSchemaBinary) when is_binary(RawSchemaBinary) ->
    IndexB = riak_search_utils:to_binary(Index),
    gen_server:call(?SERVER, {put_raw_schema, IndexB, RawSchemaBinary}, infinity).


init([]) ->
    {ok, Client} = riak:local_client(),
    Table = ets:new(schema_table, [named_table, public, set]),
    {ok, #state{client=Client, schema_table=Table}}.

handle_call(clear, _From, State) ->
    ets:delete_all_objects(State#state.schema_table),
    {reply, ok, State};

handle_call({get_schema, SchemaName}, _From, State) ->
    Table = State#state.schema_table,
    Client = State#state.client,
    case get_raw_schema_from_kv(Client, SchemaName) of
        {ok, RawSchemaBinary} ->
            %% Parse and cache the schema...
            {ok, RawSchema} = riak_search_utils:consult(RawSchemaBinary),
            {ok, Schema} = riak_search_schema_parser:from_eterm(SchemaName, RawSchema),
            true = ets:insert(Table, {SchemaName, Schema}),

            %% Update buckets n_val...
            ensure_n_val_setting(Schema),

            {reply, {ok, Schema}, State};
        {error, Reason} = Error ->
            lager:critical("Error getting schema '~s': ~p", [SchemaName,
                    file:format_error(Reason)]),
            throw(Error)
end;

handle_call({get_raw_schema, SchemaName}, _From, State) ->
    Client = State#state.client,
    case get_raw_schema_from_kv(Client, SchemaName) of
        {ok, RawSchema} ->
            {reply, {ok, RawSchema}, State};
        {error, Reason} = Error ->
            lager:critical("Error getting schema '~s': ~p", [SchemaName,
                    file:format_error(Reason)]),
            throw(Error)
    end;

handle_call({put_raw_schema, SchemaName, RawSchemaBinary}, _From, State) ->
    %% Parse the schema so that we can update buckets appropriately,
    %% and so that we don't put an incorrectly formatted schema into
    %% KV.
    case riak_search_utils:consult(RawSchemaBinary) of
        {ok, RawSchema} ->
            {ok, Schema} = riak_search_schema_parser:from_eterm(SchemaName, RawSchema),
            %% Update buckets n_val...
            ensure_n_val_setting(Schema),

            %% Clear the local cache entry...
            Table = State#state.schema_table,
            true = ets:insert(Table, {SchemaName, Schema}),

            %% Write to Riak KV...
            Client = State#state.client,
            put_raw_schema_to_kv(Client, SchemaName, RawSchemaBinary),
            {reply, ok, State};
        Error ->
            lager:error("Could not parse schema: ~p", [Error]),
            Error
    end;

handle_call(Request, _From, State) ->
    ?PRINT({unhandled_call, Request}),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    ?PRINT({unhandled_cast, Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?PRINT({unhandled_info, Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

%% Return the schema for an index. If not found, use the default.
%% @param Index - the name of an index.
%% @return {ok, RawSchemaBinary}.
get_raw_schema_from_kv(Client, SchemaName) when is_binary(SchemaName) ->
    case Client:get(?SCHEMA_BUCKET, SchemaName) of
        {ok, Entry} ->
            {ok, riak_object:get_value(Entry)};
        {error, notfound} ->
            file:read_file(?DEFAULT_SCHEMA)
    end.

%% Set the schema for an index.
%% @param Index - the name of an index.
%% @param RawSchema - The raw schema tuple.
put_raw_schema_to_kv(Client, SchemaName, RawSchemaBinary) when is_binary(SchemaName), is_binary(RawSchemaBinary) ->
    case Client:get(?SCHEMA_BUCKET, SchemaName) of
        {ok, Obj} ->
            NewObj = riak_object:update_value(Obj, RawSchemaBinary);
        {error, notfound} ->
            NewObj = riak_object:new(?SCHEMA_BUCKET, SchemaName, RawSchemaBinary)
    end,
    Client:put(NewObj).

%% ensure_n_val_setting/1 - Ensure that the n_val setting for the
%% bucket where riak_idx_docs are written matches the n_val setting of
%% the schema.
ensure_n_val_setting(Schema) ->
    BucketName = riak_indexed_doc:idx_doc_bucket(Schema:name()),
    BucketProps = riak_core_bucket:get_bucket(BucketName),
    NVal = Schema:n_val(),
    CurrentNVal = proplists:get_value(n_val,BucketProps),
    CurrentAllowMult = proplists:get_value(allow_mult, BucketProps),
    case NVal =:= CurrentNVal andalso CurrentAllowMult =:= false of
        true ->
            ok;
        false ->
            riak_core_bucket:set_bucket(BucketName, [{n_val, NVal},
                                                    {allow_mult, false}])
    end.
