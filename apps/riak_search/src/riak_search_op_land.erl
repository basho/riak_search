-module(riak_search_op_land).
-export([
         preplan_op/2,
         chain_op/3,
         chain_op/4
        ]).
-include("riak_search.hrl").

preplan_op(Op, F) ->
    Op#land { ops=F(Op#land.ops) }.

chain_op(Op, OutputPid, OutputRef) ->
    chain_op(Op, OutputPid, OutputRef, 'land').

chain_op(Op, OutputPid, OutputRef, Type) ->
    %% Set some vars...
    OpList = Op#land.ops,
    Connections = length(OpList),
    Ref = make_ref(),

    %% Spawn up a collection Pid.
    Queues = [{is_record(X, lnot), undefined, queue:new()} || X <- OpList],
    F = fun() -> gather_results(Connections, Ref, OutputPid, OutputRef, Queues, Type) end,
    Pid = spawn_link(F),

    %% Build the rest of the op chain...
    inner_chain_op(OpList, Pid, {Ref, 1}),
    {ok, Connections}.

%% Create the op chain, giving each operation it's own unique Ref of
%% the form {Ref, N}. This lets us keep separate queues of incoming
%% results.
inner_chain_op([], _, _) ->
    [];
inner_chain_op([Op|Rest], Pid, {Ref, N}) ->
    riak_search_op:chain_op(Op, Pid, {Ref, N}),
    inner_chain_op(Rest, Pid, {Ref, N + 1}).


%% Gather results from all connections.
gather_results(0, _, OutputPid, OutputRef, _, _) ->
    OutputPid!{disconnect, OutputRef};
gather_results(Connections, Ref, OutputPid, OutputRef, Queues, Type) ->
    receive
	{results, Results, {Ref, N}} -> 
            NewQueues = add_results(N, Results, Queues),
            NewQueues1 = possibly_send(OutputPid, OutputRef, NewQueues, Type),
	    gather_results(Connections, Ref, OutputPid, OutputRef, NewQueues1, Type);

	{disconnect, {Ref, N}} ->
            NewQueues = add_results(N, ['$end_of_table'], Queues),
            NewQueues1 = possibly_send(OutputPid, OutputRef, NewQueues, Type),
            gather_results(Connections - 1, Ref, OutputPid, OutputRef, NewQueues1, Type);

	Other ->
            ?PRINT({unexpected_message, Other}),
	    throw({unexpected_message, Other})
    end.


%% Add incoming results to a queue.  Walks through a list of queues
%% until it gets to the nth one. Then, adds the result to the end of
%% the queue.
add_results(N, Results, [H|T]) when N /= 1->
    [H|add_results(N - 1, Results, T)];

add_results(1, Results, [{NotFlag, Pop, Q}|Rest]) ->
    %% Push the results on to this queue one by one.
    F = fun(Result, Acc) ->
                queue:in(Result, Acc)
        end,
    NewQ = lists:foldl(F, Q, Results),
    [{NotFlag, Pop, NewQ}|Rest];

add_results(N, _, []) ->
    throw({past_end_of_results, N}).

%% Do an AND comparision, and send the results.
possibly_send(OutputPid, OutputRef, Queues, Type) ->
    Queues1 = pop_values(Queues),
    
    %% Pull out a list of values.
    HasUndefined = length([1 || {_, undefined, _} <- Queues1]) > 0,
    Pops = [Pop || {false, Pop, _} <- Queues1, Pop /= undefined, Pop /= '$end_of_table'],
    case HasUndefined orelse (length(Pops) == 0) of
        true -> 
            Queues1;
        false -> 
            %% Get the value to possibly send.
            Value = lists:min(Pops),

            %% Check if we should send, if so, then send.
            NewQueues1 = case check_and_send(Value, Queues1, Type) of
                             {true, NewQueues} ->
                                 OutputPid!{results, [Value], OutputRef},
                                 NewQueues;
                             {false, NewQueues} ->
                                 NewQueues
                         end,
            possibly_send(OutputPid, OutputRef, NewQueues1, Type)
    end.


%% Ensure that each queue has had the first result popped
%% off if possible, and make sure the minimum result is NOT 
%% on a NOT-flagged queue.
pop_values(Queues) ->
    Queues1 = pop_undefined_values(Queues),
    Queues2 = pop_minimum_not_values(Queues1),
    case Queues == Queues2 of
        true -> Queues;
        false -> pop_values(Queues2)
    end.
    
pop_undefined_values([{NotFlag, undefined, Q}|Rest]) ->
    case queue:out(Q) of 
        {{value, NewValue}, NewQ} ->
            [{NotFlag, NewValue, NewQ}|pop_undefined_values(Rest)];
        {empty, NewQ} -> 
            [{NotFlag, undefined, NewQ}|pop_undefined_values(Rest)]
    end;
pop_undefined_values([H|T]) -> 
    [H|pop_undefined_values(T)];
pop_undefined_values([]) -> 
    [].


%% Get the smallest value. If it is on a queue with the not flag set,
%% then we don't need it, so discard it.
pop_minimum_not_values(Queues) ->
    %% Get the minimum value where NotFlag is clear...
    Pops = [Pop || {false, Pop, _} <- Queues],
    MinValue = case lists:member(undefined, Pops) of
                   true -> undefined;
                   false -> lists:min(Pops)
               end,

    %% Get the minimum value where NotFlag is set...
    NotPops = [Pop || {true, Pop, _} <- Queues],
    HasUndefined = lists:member(undefined, NotPops),
    IsEmpty = (NotPops == []),
    MinNotValue = case HasUndefined orelse IsEmpty of
                      true -> undefined;
                      false -> lists:min(NotPops)
                  end,

    %% If we can get the minimum values, and the NotFlag'd value is
    %% the smallest, then discard it.
    case MinValue /= undefined andalso MinNotValue /= undefined andalso MinNotValue < MinValue of
        true ->
            pop_minimum_not_values_inner(MinNotValue, Queues);
        false -> 
            Queues
    end.

pop_minimum_not_values_inner(Value, [{true, Value, Q}| Rest]) ->
    case queue:out(Q) of 
        {{value, NewValue}, NewQ} ->
            [{true, NewValue, NewQ}|pop_minimum_not_values_inner(Value, Rest)];
        {empty, NewQ} -> 
            [{true, undefined, NewQ}|pop_minimum_not_values_inner(Value, Rest)]
    end;

pop_minimum_not_values_inner(Value, [H|T]) -> 
    [H|pop_minimum_not_values_inner(Value, T)];
pop_minimum_not_values_inner(_, []) -> 
    [].
    

%% Return true if we should send the supplied value, and pop it off of
%% any queues where NotFlag is NOT set. (We may still need it to weed
%% out other results.)
check_and_send(Value, Queues, Type) ->
    %% Check each queue to see if we should send.
    L = [check_and_send_inner(Value, X) || X <- Queues],
    {ShouldSends, NewQueues} = lists:unzip(L),

    %% If no queue returns false, 
    ShouldSend = case Type of 
                     'land' -> (not lists:member(false, ShouldSends));
                     'lor'  -> lists:member(true, ShouldSends)
                 end,
    {ShouldSend, NewQueues}.

check_and_send_inner(Value, {NotFlag, Pop, Queue}) ->
    ShouldSend = 
        (Value == Pop andalso not NotFlag) orelse 
        (Value /= Pop andalso NotFlag),

    %% Discard if values match and notflag is not set.
    DiscardValue = 
        (Value == Pop andalso not NotFlag),
    
    case DiscardValue of
        true -> {ShouldSend, {NotFlag, undefined, Queue}};
        false -> {ShouldSend, {NotFlag, Pop, Queue}}
    end.
