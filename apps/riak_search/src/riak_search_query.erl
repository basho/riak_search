-module(riak_search_query).
-export([
         execute/4
        ]).
-include("riak_search.hrl").

%% Execute the query operation, 
%% Accepts a Qilr query.
%% Returns {ok, Results}
execute(OpList, DefaultIndex, DefaultField, Facets) -> 
    %% Normalize, Optimize, and Expand Buckets.
    OpList1 = riak_search_preplan:preplan(OpList, DefaultIndex, DefaultField, Facets),

    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    {ok, NumInputs} = riak_search_op:chain_op(OpList1, self(), Ref),

    %% Gather and return results...
    Results = gather_results(NumInputs, Ref, []),
    {ok, Results}.

%% Gather results from all connections
gather_results(Connections, Ref, Acc) ->
    receive
	{results, Results, Ref} -> 
	    gather_results(Connections, Ref, Acc ++ Results);

	{disconnect, Ref} when Connections > 1  ->
	    gather_results(Connections - 1, Ref, Acc);

	{disconnect, Ref} when Connections == 1 ->
	    Acc;

	Other ->
	    throw({unexpected_message, Other})

    after 1 * 1000 ->
            ?PRINT(timeout),
            throw({timeout, Connections, Acc})
    end.
