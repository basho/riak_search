%% @doc This server is used to generate {Field, Terms} pairs.  It is
%% given a file, a list of fileds, and a schema which it then uses to
%% iterate the file, line by line, extracting {Field, Terms} pairs as
%% they are requested.
%%
%% 1/11/12 - Using my MBP with a SSD and 4 concurrent workers I was
%% able to get thoughput of 6K/s with an average latency below 1ms and
%% 99.9th of 6ms.  Therefore, if you start to see benchmarks against
%% search at 6K/s thru and 6ms 99.9th latency then this server should
%% be revisited for performance improvements.  Until then it should be
%% good enough.  That said, this may not be sound advice on a
%% mechanical disk.  I'm assuming SSD for
%% now (http://www.youtube.com/watch?v=H7PJ1oeEyGg).
-module(rs_bb_line_proc).
-behavior(gen_server).
-compile(export_all).

%% Callbacks
-export([init/1, handle_call/3, terminate/2]).

-include_lib("basho_bench/include/basho_bench.hrl").
-define(MB, 1048576).
-record(state, {cache, fields, file, schema}).

-type field() :: binary().
-type sterm() :: binary().
-type sterms() :: [sterm()].

%% ====================================================================
%% API
%% ====================================================================

%% @doc Return a {Field, Terms} pair.
-spec get_ft() -> {field(), sterms()}.
get_ft() -> get_ft(1).

%% @doc Return a {Field, Terms} pair.  An attempt will be made to
%% return N terms but could be less.
-spec get_ft(pos_integer()) -> {field(), sterms()}.
get_ft(N) -> gen_server:call(?MODULE, {ft, N}).

start_link(Path, Fields, Schema) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [Path, Fields, Schema], []).

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
    Pairs = read_pairs(F),
    Pairs2 = lists:filter(match(Fields), Pairs),
    Pairs3 = lists:map(analyze(Schema), Pairs2),
    {Pair, Cache2} = get_terms(Pairs3, N),
    {reply, Pair, S#state{cache=Cache2}};
handle_call({ft, N}, _From, S=#state{cache=Cache}) ->
    {Pair, Cache2} = get_terms(Cache, N),
    {reply, Pair, S#state{cache=Cache2}}.

terminate(_Reason, _State) -> ignore.

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

read_pairs(F) ->
    read_pairs(F, 0).

read_pairs(_F, 100) ->
    %% Guard against infinite loop
    throw({field_extraction, too_many_retries});

read_pairs(F, Retry) ->
    case file:read_line(F) of
        {ok, Line} -> ok;
        eof ->
            file:position(F, bof),
            {ok, Line} = file:read_line(F),
            ok
    end,

    %% Guard against invalid JSON
    try
        riak_search_kv_json_extractor:extract_value(Line, default_field,
                                                    ignored)
    catch _:Reason ->
            ?ERROR("Failed to extract line: ~p", [Reason]),
            read_pairs(F, Retry+1)
    end.
