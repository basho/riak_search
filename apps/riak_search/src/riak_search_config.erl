-module(riak_search_config).

-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0, clear/0, get_schema/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {schema_dir,
                schemas}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

clear() ->
    gen_server:call(?SERVER, clear).

get_schema(IndexOrSchema) ->
    case is_tuple(IndexOrSchema) andalso element(1, IndexOrSchema) == riak_search_schema of
        true  ->
            {ok, IndexOrSchema};
        false ->
            Index = riak_search_utils:to_list(IndexOrSchema),
            gen_server:call(?SERVER, {get_schema, Index})
    end.

init([]) ->
    SchemaDir = case application:get_env(riak_search, schema_dir) of
                    undefined ->
                        code:priv_dir(riak_search);
                    {ok, Dir} ->
                        Dir
                end,
    {ok, #state{schema_dir=SchemaDir,
                schemas=dict:new()}}.

handle_call(clear, _From, State) ->
    {reply, ok, State#state{schemas=dict:new()}};
handle_call({get_schema, SchemaName}, _From, #state{schemas=Schemas}=State) ->
    %% Look up the schema in cache...
    case dict:find(SchemaName, Schemas) of
        {ok, Schema} ->
            %% Return the schema...
            {reply, {ok, Schema}, State};
        _ ->
            %% Load schema from file...
            case load_schema(SchemaName, State#state.schema_dir) of
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
load_schema(SchemaName, SchemaDir) ->
    SchemaFile = io_lib:format("~s.def", [SchemaName]),
    Result = file:consult(filename:join([SchemaDir, SchemaFile])),
    case Result of
        {ok, [RawSchema]} ->
            %% Parse from file...
            riak_search_schema_parser:from_eterm(RawSchema);
        {error, enoent} ->
            %% Not found, use default...
            error_logger:warning_msg("Could not find schema '~s', using defaults.~n", [SchemaName]),
            Schema = riak_search_schema:new(SchemaName, 0, "value", [], 'or', undefined),
            {ok, Schema};
        Error ->
            %% Some other error, so return...
            Error
    end.
