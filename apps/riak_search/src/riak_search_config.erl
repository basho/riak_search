-module(riak_search_config).

-include("riak_search.hrl").

-behaviour(gen_server).

-define(SCHEMA_KEY, <<"_rs_schema">>).
-define(DEFAULT_SCHEMA, filename:join([code:priv_dir(riak_search), "default.def"])).

%% API
-export([start_link/0, clear/0, get_schema/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {client,
                schemas}).

-define(DEFAULT_FIELD, #riak_search_field{name="value",
                                          type=string,
                                          required=false,
                                          facet=false}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

clear() ->
    gen_server:call(?SERVER, clear).

get_schema(Schema) when is_tuple(Schema) ->
    case element(1, Schema) of
        riak_search_schema ->
            {ok, Schema};
        _ ->
            {error, badarg}
    end;
get_schema(Schema) ->
    gen_server:call(?SERVER, {get_schema, riak_search_utils:to_binary(Schema)}).

init([]) ->
    {ok, Client} = riak:local_client(),
    {ok, #state{client=Client,
                schemas=dict:new()}}.

handle_call(clear, _From, State) ->
    {reply, ok, State#state{schemas=dict:new()}};
handle_call({get_schema, SchemaName}, _From, #state{client=Client,
                                                    schemas=Schemas}=State) ->
    %% Look up the schema in cache...
    case dict:find(SchemaName, Schemas) of
        {ok, Schema} ->
            %% Return the schema...
            {reply, {ok, Schema}, State};
        _ ->
            %% Load schema from file...
            case load_schema(Client, SchemaName) of
                {ok, Schema} ->
                    NewSchemas = dict:store(SchemaName, Schema, Schemas),
                    NewState = State#state { schemas=NewSchemas },
                    {reply, {ok, Schema}, NewState};
                Error ->
                    error_logger:error_msg("Error loading schema '~s': ~n~p~n", [SchemaName, Error]),
                    {reply, undefined, State}
            end
    end;
handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
load_schema(Client, SchemaName) ->
    case Client:get(SchemaName, ?SCHEMA_KEY) of
        {ok, Entry} ->
            case parse_entry(Entry) of
                {ok, RawSchema} ->
                    riak_search_schema_parser:from_eterm(RawSchema);
                Error ->
                    error_logger:error_msg("Error parsing schema ~p: ~p~n", [SchemaName, Error]),
                    %% Error in schema definition, so let's return
                    %% the error instead of loading the default
                    Error
            end;
        {error, notfound} ->
            %% Schema entry not found so let's load the
            %% default schema and use it
            error_logger:error_msg("Schema entry \"_rs_schema\" not found in bucket ~p. Using default schema.~n",
                                   [riak_search_utils:to_list(SchemaName)]),
            load_default_schema(SchemaName)
    end.

parse_entry(Entry) ->
    EntryBody = riak_object:get_value(Entry),
    case erl_scan:string(riak_search_utils:to_list(EntryBody)) of
        {ok, Tokens, _} ->
            case erl_parse:parse_exprs(Tokens) of
                {ok, AST} ->
                    case erl_eval:exprs(AST, []) of
                        {value, Schema, _} ->
                            {ok, Schema};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

load_default_schema(SchemaName) ->
    case file:consult(?DEFAULT_SCHEMA) of
        {ok, [RawSchema]} ->
            case riak_search_schema_parser:from_eterm(RawSchema) of
                {ok, Schema} ->
                    {ok, Schema:set_name(riak_search_utils:to_list(SchemaName))};
                Error ->
                    Error
            end;
        Error ->
            error_logger:error_msg("Error loading default schema: ~p~n", [Error]),
            Error
    end.
