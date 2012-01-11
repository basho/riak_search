%% Generating keys...
%% 1. assume file is lines of JSON
%% 2. make sure file exists and open
%% 3. read a line or exit
%% 4. decode JSON using mochijson2:decode
%% 5. filter out only list of fields passed (all by default) and create {Field,Val} pairs
%% 6. map analyzer across Vals creating {Field, Terms} pair, use schema info to pick correct analyzer
%% 7. goto step 3
-module(rs_bb_line_proc).
-behavior(gen_server).
-compile(export_all).

%% Callbacks
-export([init/1, handle_call/3]).

-define(MB, 1048576).
-record(state, {cache, fields, file, schema}).

-type field() :: binary().
-type sterm() :: binary().
-type sterms() :: [sterm()].

%% ====================================================================
%% API
%% ====================================================================

%% @doc Return a {Field, Terms} pair.
-spec get_ft(pid()) -> {field(), sterms()}.
get_ft(LP) -> get_ft(LP, 1).

%% @doc Return a {Field, Terms} pair.  An attempt will be made to
%% return N terms but could be less.
-spec get_ft(pid(), pos_integer()) -> {field(), sterms()}.
get_ft(LP, N) -> gen_server:call(LP, {ft, N}).

start_link(Path, Fields, Schema) ->
    gen_server:start_link(?MODULE, [Path, Fields, Schema], []).

%% ====================================================================
%% Callbacks
%% ====================================================================

%% -spec init([string(), [binary()], string()]) -> {ok, #state{}}.
init([Path, Fields, SchemaPath]) ->
    {ok, F} = file:open(Path, [read, raw, binary, {read_ahead, ?MB}]),
    Schema = load_schema(SchemaPath),
    {ok, #state{cache=[], fields=Fields, file=F, schema=Schema}}.

handle_call({ft, N}, _From, S=#state{cache=[],
                                     fields=Fields,
                                     file=F,
                                     schema=Schema}) ->
    case file:read_line(F) of
        {ok, Line} -> ok;
        eof ->
            file:position(F, bof),
            {ok, Line} = file:read_line(F),
            ok
    end,
    Pairs = riak_search_kv_json_extractor:extract_value(Line, default_field,
                                                        ignored),
    Pairs2 = lists:filter(match(Fields), Pairs),
    Pairs3 = lists:map(analyze(Schema), Pairs2),
    {Pair, Cache2} = get_terms(Pairs3, N),
    {reply, Pair, S#state{cache=Cache2}};
handle_call({ft, N}, _From, S=#state{cache=Cache}) ->
    {Pair, Cache2} = get_terms(Cache, N),
    {reply, Pair, S#state{cache=Cache2}}.

%% ====================================================================
%% Private
%% ====================================================================

get_terms([{Field, Terms}|Pairs], N) ->
    {T1, T2} = lists:split(N, Terms),
    Pair = {Field, T1},
    case length(T2) of
        0 -> {Pair, Pairs};
        _ -> {Pair, [{Field, T2}|Pairs]}
    end.

match(Fields) ->
    S = sets:from_list(Fields),
    fun({Field, _}) ->
            sets:is_element(Field, S)
    end.

analyze(Schema) ->
    fun({Field, Val}) ->
            X = Schema:find_field(Field),
            {erlang, M, F} = Schema:analyzer_factory(X),
            A = Schema:analyzer_args(X),
            {ok, Res} = M:F(Val, A),
            {Field, Res}
    end.

load_schema(Path) ->
    {ok, S1} = file:read_file(Path),
    {ok, S2} = riak_search_utils:consult(S1),
    {ok, S3} = riak_search_schema_parser:from_eterm(<<"test">>, S2),
    S3.
