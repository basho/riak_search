%% @doc A Basho Bench driver for Riak Search.
-module(rs_bb_driver).

%% Callbacks
-export([new/1,
         run/4]).

%% Key Gens
-export([terms_from_file/4]).

-include_lib("basho_bench/include/basho_bench.hrl").
-record(state, {i, urls}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    ibrowse:start(),
    Ports = basho_bench_config:get(rs_ports, [{"127.0.0.1", 8098}]),
    Path = basho_bench_config:get(rs_path, "/search/test"),
    URLs = array:from_list(lists:map(make_url(Path), Ports)),
    N = basho_bench_config:get(concurrent),
    {ok, #state{i={0, N-1}, urls=URLs}}.

%% steps:
%% 1. get URL for index I
%% 2. get field/term pair
%% 3. build query str
%% 4. append query str to URL
%% 5. send HTTP req to URL
%% 6. verify 200 or log error
run(search, KeyGen, _ValGen, S=#state{i={Idx,_}=I, urls=URLs}) ->
    Base = array:get(Idx, URLs),
    {Field, [Term]} = KeyGen(),
    Qry = ?FMT("~s:~s", [Field, Term]),
    Params = mochiweb_util:urlencode([{<<"q">>, Qry}]),
    URL = ?FMT("~s?~s", [Base, Params]),
    S2 = S#state{i=wrap(I)},
    case http_get(URL) of
        ok -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end.

%% ====================================================================
%% Key Gens
%% ====================================================================

terms_from_file(Id, Path, Fields, Schema) ->
    %% Terms are shared amongst workers so that each worker isn't
    %% simply querying for the same terms.
    N = basho_bench_config:get(concurrent),
    if Id == N ->
            {ok, _} = rs_bb_file_terms:start_link(Path, Fields, Schema);
       true -> ok
    end,
    fun() ->
            rs_bb_file_terms:get_ft()
    end.

%% ====================================================================
%% Private
%% ====================================================================

%% @doc Return line count for given `File'.
%% -spec wcl(string()) -> non_neg_integer().
%% wcl(File) ->
%%     S = os:cmd("wc -l " ++ File ++ " | awk -v ORS='' '{print $1}' "),
%%     list_to_integer(S).

make_url(Path) ->
    fun({IP, Port}) -> ?FMT("http://~s:~w~s", [IP, Port, Path]) end.

http_get(URL) ->
    case ibrowse:send_req(URL, [], get) of
        {ok, "200", _, _} -> ok;
        {ok, Status, _, _} -> {error, {bad_status, Status, URL}};
        {error, Reason} -> {error, Reason}
    end.

wrap({I,N}) when I == N -> {0,N};
wrap({I,N}) -> {I+1,N}.
