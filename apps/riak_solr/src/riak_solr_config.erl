-module(riak_solr_config).

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

get_schema(SchemaName) ->
    gen_server:call(?SERVER, {get_schema, SchemaName}).

init([]) ->
    SchemaDir = case application:get_env(riak_solr, schema_dir) of
                    undefined ->
                        code:priv_dir(riak_solr);
                    {ok, Dir} ->
                        Dir
                end,
    {ok, #state{schema_dir=SchemaDir,
                schemas=dict:new()}}.

handle_call(clear, _From, State) ->
    {reply, ok, State#state{schemas=dict:new()}};
handle_call({get_schema, SchemaName}, _From, #state{schemas=Schemas}=State) ->
    {Reply, State1} = case dict:find(SchemaName, Schemas) of
                          {ok, Schema} ->
                              {{ok, Schema}, State};
                          _ ->
                              load_schema(SchemaName, State)
                      end,
    {reply, Reply, State1};
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
load_schema(SchemaName, #state{schema_dir=Dir, schemas=Schemas}=State) ->
    Result = file:consult(filename:join([Dir, SchemaName ++ ".def"])),
    case Result of
        {ok, [RawSchema]} ->
            case riak_solr_schema_parser:from_eterm(RawSchema) of
                {ok, Schema} ->
                    {{ok, Schema}, State#state{schemas=dict:store(SchemaName, Schema, Schemas)}};
                Error ->
                    {Error, State}
            end;
        Error ->
            {Error, State}
    end.
