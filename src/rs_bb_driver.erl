%% @doc A Basho Bench driver for Riak Search.
-module(rs_bb_driver).

%% Callbacks
-export([new/1,
         run/4]).

%% Key Gens
-export([always/2, fruit_key_val_gen/1, valgen/4, valgen_i/1]).

-include_lib("basho_bench/include/basho_bench.hrl").
-record(state, {pb_conns, index, iurls, surls}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    ibrowse:start(),
    Index = basho_bench_config:get(rs_index, "test"),
    Ports = basho_bench_config:get(rs_ports, [{"127.0.0.1", 8098}]),
    PBPorts = basho_bench_config:get(pb_ports, [{"127.0.0.1", 8087}]),
    IPath = basho_bench_config:get(rs_index_path, "/riak/test"),
    SPath = basho_bench_config:get(rs_search_path, "/search/test"),
    IURLs = array:from_list(lists:map(make_url(IPath), Ports)),
    SURLs = array:from_list(lists:map(make_url(SPath), Ports)),
    Conns = array:from_list(lists:map(fun make_conn/1, PBPorts)),
    N = length(Ports),
    M = length(PBPorts),

    {ok, #state{pb_conns={Conns, {0,M}},
                index=list_to_binary(Index),
                iurls={IURLs, {0,N}},
                surls={SURLs, {0,N}}}}.

run(search, _KeyGen, ValGen, S=#state{surls=URLs}) ->
    Base = get_base(URLs),
    {Field, [Term]} = ValGen(search),
    Qry = ?FMT("~s:~s", [Field, Term]),
    Params = mochiweb_util:urlencode([{<<"q">>, Qry}]),
    URL = ?FMT("~s?~s", [Base, Params]),
    %% ?ERROR("search url: ~p~n", [URL]),
    S2 = S#state{surls=wrap(URLs)},
    case http_get(URL) of
        ok -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run(index, _KeyGen, ValGen, S=#state{iurls=URLs}) ->
    Base = get_base(URLs),
    {Key, Line} = ValGen(index),
    URL = ?FMT("~s/~s", [Base, Key]),
    S2 = S#state{iurls=wrap(URLs)},
    case http_put(URL, "application/json", Line) of
        ok -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run(load_fruit, KeyValGen, _, S=#state{iurls=URLs}) ->
    Base = get_base(URLs),
    {Key, Val} = KeyValGen(),
    URL = ?FMT("~s/~p", [Base, Key]),
    S2 = S#state{iurls=wrap(URLs)},
    case http_put(URL, "plain/text", Val) of
        ok -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run(search_pb, _, QueryGen, S=#state{index=Index, pb_conns=Conns}) ->
    Conn = get_conn(Conns),
    Query = QueryGen(),
    S2 = S#state{pb_conns=wrap(Conns)},
    case search_pb(Conn, Index, Query) of
        ok -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run(show, KeyGen, _ValGen, S) ->
    {K, V} = KeyGen(),
    io:format("~p: ~p~n", [K, V]),
    {ok, S}.

search_pb(Conn, Index, Query) ->
    case riakc_pb_socket:search(Conn, Index, Query) of
        {ok, _Result} -> ok;
        Other -> Other
    end.

%% ====================================================================
%% Key Gens
%% ====================================================================

%% @doc Allow to have different valgen depending on operation.
valgen(Id, Path, Fields, Schema) ->
    N = basho_bench_config:get(concurrent),
    if Id == N ->
            {ok, _} = rs_bb_file_terms:start_ts(Path, Fields, Schema),
            {ok, _} = rs_bb_file_terms:start_ls(Path);
       true -> ok
    end,
    fun ?MODULE:valgen_i/1.

valgen_i(index) ->
    rs_bb_file_terms:get_line();
valgen_i(search) ->
    rs_bb_file_terms:get_ft().

-define(K100, 100000).
-define(K10, 10000).
-define(K1, 1000).

-define(FRUITS,
        [{?K100, "apple grape orange pineapple strawberry kiwi"},
         {?K10, "avocado raspberry persimmon blackberry cherry tomato"},
         {?K1, "clementine lime lemon melon plum pear"},
         {100, "marang nutmeg olive pecan peanut tangerine"},
         {10, "nunga nance mulberry langsat karonda kumquat"},
         {1, "korlan jocote genip elderberry citron jujube"}]).


%% generates key and value because value is based on key
fruit_key_val_gen(Id) ->
    Fruits2 = [{N, combine(?FRUITS, N)} || N <- [1, 10, 100, ?K1, ?K10, ?K100]],
    StartKey = 1,
    NumKeys = ?K100,
    Workers = basho_bench_config:get(concurrent),
    Range = NumKeys div Workers,
    MinValue = StartKey + Range * (Id - 1),
    MaxValue = StartKey + Range * Id,
    Ref = make_ref(),
    ?DEBUG("ID ~p generating range ~p to ~p\n", [Id, MinValue, MaxValue]),
    fun() ->
            K = basho_bench_keygen:sequential_int_generator(Ref, Range) + MinValue,
            V = first_large_enough(K, Fruits2),
            {K, V}
    end.

always(_Id, Val) ->
    fun() -> Val end.

%% ====================================================================
%% Private
%% ====================================================================

%% @doc Return line count for given `File'.
%% -spec wcl(string()) -> non_neg_integer().
%% wcl(File) ->
%%     S = os:cmd("wc -l " ++ File ++ " | awk -v ORS='' '{print $1}' "),
%%     list_to_integer(S).

combine(Fruits, N) ->
    string:join([Str || {Count, Str} <- Fruits, Count >= N], " ").

first_large_enough(K, [{Count, Str}|Fruits]) ->
    if Count >= K -> Str;
       true -> first_large_enough(K, Fruits)
    end.

get_base({URLs, {I,_}}) -> array:get(I, URLs).

get_conn({Conns, {I,_}}) -> array:get(I, Conns).

make_url(Path) ->
    fun({IP, Port}) -> ?FMT("http://~s:~w~s", [IP, Port, Path]) end.

make_conn({IP, Port}) ->
    {ok, Conn} = riakc_pb_socket:start_link(IP, Port),
    Conn.

http_get(URL) ->
    case ibrowse:send_req(URL, [], get) of
        {ok, "200", _, _} -> ok;
        {ok, Status, _, _} -> {error, {bad_status, Status, URL}};
        {error, Reason} -> {error, Reason}
    end.

http_put(URL, CT, Body) ->
    case ibrowse:send_req(URL, [{content_type, CT}], put, Body, [{content_type, CT}]) of
        {ok, "200", _, _} -> ok;
        {ok, "201", _, _} -> ok;
        {ok, "204", _, _} -> ok;
        {ok, Status, _, Resp} -> {error, {bad_status, Status, URL, Resp}};
        {error, Reason} -> {error, Reason}
    end.

wrap({URLs, {I,N}}) when I == N - 1 -> {URLs, {0,N}};
wrap({URLs, {I,N}}) -> {URLs, {I+1,N}}.
