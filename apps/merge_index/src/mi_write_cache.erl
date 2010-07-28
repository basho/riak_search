-module(mi_write_cache).

-export([setup/1,
         flush/1,
         purge/1,
         write/2]).

setup(FH) ->
    put({w__write_cache, FH}, []),
    put({w__write_cache_size, FH}, 0),
    ok.

flush(FH) ->
    ok = file:write(FH, lists:reverse(get({w__write_cache, FH}))),
    setup(FH),
    ok.

purge(FH) ->
    erase({w__write_cache, FH}),
    erase({w__write_cache, FH}),
    ok.

write(FH, Bytes) ->
    case get({w__write_cache_size, FH}) of
        N when N > 5*1024*1024 ->
            flush(FH),
            write(FH, Bytes);
        N ->
            put({w__write_cache, FH}, [Bytes|get({w__write_cache, FH})]),
            put({w__write_cache_size, FH}, N + iolist_size(Bytes)),
            ok
    end.

