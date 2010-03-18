-module(riak_search_op_term).
-export([
         preplan_op/2,
         chain_op/3
        ]).
-include("riak_search.hrl").

%% This is a stub for debugging purposes. This generates data by
%% taking the search term and splitting it into characters, which
%% makes it easy to write test queries.
%%
%% For example, the word 'fat' is in documents 'f', 'a', and 't'. The
%% document 'cat' is in 'c', 'a', and 't'. So if you search for 'fat
%% AND cat', then you should get the documents 'a' and 't' in the
%% result.

preplan_op(Op, _F) -> Op.

chain_op(Op, OutputPid, OutputRef) ->
    String = Op#term.string,
    spawn(fun() -> send_results(String, OutputPid, OutputRef) end),
    {ok, 1}.

send_results(String, OutputPid, OutputRef) ->
    Term1 = lists:nth(3, string:tokens(String, ".")),
    Term2 = lists:sort(Term1),
    [OutputPid!{results, [X], OutputRef} || X <- Term2],
    OutputPid!{disconnect, OutputRef}.
