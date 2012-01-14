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
    N = basho_bench_config:get(concurrent),
    {ok, #state{iurls={IURLs, {0,N}}, surls={SURLs, {0,N}}}}.

run(search, _KeyGen, ValGen, S=#state{surls=URLs}) ->
    Base = get_base(URLs),
    {Field, [Term]} = ValGen(search),
    Qry = ?FMT("~s:~s", [Field, Term]),
    Params = mochiweb_util:urlencode([{<<"q">>, Qry}]),
    URL = ?FMT("~s?~s", [Base, Params]),
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
        {ok, "204", _, _} -> ok;
        {ok, Status, _, Resp} -> {error, {bad_status, Status, URL, Resp}};
        {error, Reason} -> {error, Reason}
    end.

wrap({URLs, {I,N}}) when I == N - 1 -> {URLs, {0,N}};
wrap({URLs, {I,N}}) -> {URLs, {I+1,N}}.
