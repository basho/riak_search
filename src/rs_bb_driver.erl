%% @doc A Basho Bench driver for Riak Search.
-module(rs_bb_driver).

%% Callbacks
-export([new/1,
         run/4]).

%% Key Gens
-export([valgen/4, valgen_i/1]).

-include_lib("basho_bench/include/basho_bench.hrl").
-record(state, {iurls, surls}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    ibrowse:start(),
    Ports = basho_bench_config:get(rs_ports, [{"127.0.0.1", 8098}]),
    IPath = basho_bench_config:get(rs_index_path, "/riak/test"),
    SPath = basho_bench_config:get(rs_search_path, "/search/test"),
    IURLs = array:from_list(lists:map(make_url(IPath), Ports)),
    SURLs = array:from_list(lists:map(make_url(SPath), Ports)),
    N = length(Ports),
    {ok, #state{iurls={IURLs, {0,N}}, surls={SURLs, {0,N}}}}.

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

-define(?FRUITS,
        [{?K100, "apple"}, {?K100, "grape"}, {?K100, "orange"},
         {?K100, "pineapple"}, {?K100, "strawberry"}, {?K100, "kiwi"},
         {?K10, "avocado"}, {?K10, "raspberry"}, {?K10, "persimmon"},
         {?K10, "blackberry"}, {?K10, "cherry"}, {?K10, "tomato"},
         {?K1, "clementine"}, {?K1, "lime"}, {?K1, "lemon"},
         {?K1, "melon"}, {?K1, "plum"}, {?K1, "pear"},
         {100, "marang"}, {100, "nutmeg"}, {100, "olive"},
         {100, "pecan"}, {100, "peanut"}, {100, "tangerine"},
         {10, "nunga"}, {10, "nance"}, {10, "mulberry"},
         {10, "langsat"}, {10, "karonda"}, {10, "kumquat"},
         {1, "korlan"}, {1, "jocote"}, {1, "genip"},
         {1, "elderberry"}, {1, "citron"}, {1, "jujube"}]).

%% generates key and value because value is based on key
fruit_key_val_gen(Id) ->
    StartKey = 0,
    NumKeys = ?K100,
    Workers = basho_bench_config:get(concurrent),
    Range = NumKeys div Workers,
    MinValue = StartKey + Range * (Id - 1),
    MaxValue = StartKey + Range * Id,
    Ref = make_ref(),
    ?DEBUG("ID ~p generating range ~p to ~p\n", [Id, MinValue, MaxValue]),
    fun() ->
            K = sequential_int_generator(Ref, Range) + MinValue,
            lists:filter(
    end.

%% ====================================================================
%% Private
%% ====================================================================

%% @doc Return line count for given `File'.
%% -spec wcl(string()) -> non_neg_integer().
%% wcl(File) ->
%%     S = os:cmd("wc -l " ++ File ++ " | awk -v ORS='' '{print $1}' "),
%%     list_to_integer(S).

get_base({URLs, {I,_}}) -> array:get(I, URLs).

make_url(Path) ->
    fun({IP, Port}) -> ?FMT("http://~s:~w~s", [IP, Port, Path]) end.

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
