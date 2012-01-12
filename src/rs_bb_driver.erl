%% @doc A Basho Bench driver for Riak Search.
-module(rs_bb_driver).

%% Callbacks
-export([new/1,
         run/4]).

%% Key Gens
-export([terms_from_file/4]).

-include_lib("basho_bench/include/basho_bench.hrl").
-record(state, {urls}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    Ports = basho_bench_config:get(rs_ports, [{"127.0.0.1", 8098}]),
    Path = basho_bench_config:get(rs_path, "/search/test"),
    URLs = lists:map(make_url(Path), Ports),
    {ok, #state{urls=URLs}}.

run(search, KeyGen, _ValGen, S=#state{urls=URLs}) ->
    K = KeyGen(),
    {ok, S}.

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
