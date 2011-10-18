-module(riak_search_ring_utils).
-export([
         get_covering_preflist/1,
         zip_with_partition_and_index/1,
         calc_partition/3
        ]).

-include("riak_search.hrl").

%% Pick out the preflist of covering nodes. There are two approaches
%% in the face of down nodes. One is to minimize the amount of
%% duplicate data that we read. The other is maximize load
%% distribution. We take the latter approach, because otherwise one or
%% two down nodes could cause all range queries to take the same set
%% of covering nodes. Works like this: rotate the ring a random
%% amount, then clump the preflist into groups of size NVal and take
%% the first up node in the list. If everything goes perfectly, this
%% will be the first node in each list, and we'll have very little
%% duplication.  If one of the nodes is down, then we'll take the next
%% node in the list down, then just take the next vnode in the list.
get_covering_preflist(NVal) ->
    %% Get the full preflist...
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    VNodes = riak_core_ring:all_owners(Ring),

    NumExtraNodes = length(VNodes) rem NVal,
    {ExtraNodes, _} = lists:split(NumExtraNodes, VNodes),
    UpNodes = riak_core_node_watcher:nodes(riak_search),
    Preflist = get_range_preflist(NVal, VNodes ++ ExtraNodes, UpNodes),
    {ok, Preflist}.

%% get_range_preflist/3 - Get a list of VNodes that is guaranteed to
%% cover all of the data (it may duplicate some data.) Given nodes
%% numbered from 0 to 7, this function creates a structure like this:
%%
%% [
%%  [{0,[]}, {1,[6,7]}, {2,[7]}],
%%  [{3,[]}, {4,[1,2]}, {5,[2]}],
%%  [{6,[]}, {7,[4,5]}, {0,[5]}]
%% ]
%%
%% This means that, for example, if node 3 is down, then we need to
%% use node 4 plus either node 1 or node 2 to get complete
%% coverage. If node 3 AND 4 are down, then we need node 5 and node
%% 2. It then picks out the nodes from the structure and returns the
%% final unique preflist.
%% 
%% To create the structure, we first take the original set of X nodes,
%% figure out how many iterations we need via ceiling(
get_range_preflist(NVal, VNodes, UpNodes) ->
    %% Create an ordered set for fast repeated checking.
    UpNodesSet = ordsets:from_list(UpNodes),

    %% Randomly rotate the vnodes...
    random:seed(now()),
    RotationFactor = random:uniform(NVal),
    {Pre, Post} = lists:split(RotationFactor, VNodes),
    VNodes1 = Post ++ Pre,
    Iterations = ceiling(length(VNodes1), NVal),

    %% Create the preflist structure and then choose the preflist based on up nodes.
    Structure = create_preflist_structure(Iterations, NVal, VNodes1 ++ VNodes1),
    lists:usort(choose_preflist(Structure, UpNodesSet)).
    
create_preflist_structure(0, _NVal, _VNodes) -> 
    [];
create_preflist_structure(Iterations, NVal, VNodes) -> 
    {Backup, VNodes1} = lists:split(NVal, VNodes),
    {Primary, _} = lists:split(NVal, VNodes1),
    Group = [{hd(Primary), []}] ++ create_preflist_structure_1(tl(Primary), tl(Backup)),
    [Group|create_preflist_structure(Iterations - 1, NVal, VNodes1)].
create_preflist_structure_1([], []) -> 
    [];
create_preflist_structure_1([H|T], Backups) ->
    [{H, Backups}|create_preflist_structure_1(T, tl(Backups))].
    
%% Given a preflist structure, return the preflist.
choose_preflist([Group|Rest], UpNodesSet) ->
    choose_preflist_1(Group, UpNodesSet) ++ choose_preflist(Rest, UpNodesSet);
choose_preflist([], _) -> 
    [].
choose_preflist_1([{Primary, Backups}|Rest], UpNodesSet) ->
    {_, PrimaryNode} = Primary,
    AvailableBackups = filter_upnodes(Backups, UpNodesSet),
    case ordsets:is_element(PrimaryNode, UpNodesSet) of 
        true when AvailableBackups == [] ->
            [Primary];
        true when AvailableBackups /= [] ->
            [Primary, riak_search_utils:choose(AvailableBackups)];
        false ->
            choose_preflist_1(Rest, UpNodesSet)
    end;
choose_preflist_1([], _) -> 
    [].

%% Given a list of VNodes, filter out any that are offline.
filter_upnodes([{Index,Node}|VNodes], UpNodesSet) ->
    case ordsets:is_element(Node, UpNodesSet) of
        true -> 
            [{Index, Node}|filter_upnodes(VNodes, UpNodesSet)];
        false ->
            filter_upnodes(VNodes, UpNodesSet)
    end;
filter_upnodes([], _) ->
    [].

ceiling(Numerator, Denominator) ->
    case Numerator rem Denominator of
        0 -> Numerator div Denominator;
        _ -> (Numerator div Denominator) + 1
    end.

% @doc Returns a function F(Index, Field, Term) -> integer() that can
% be used to calculate the partition on the ring. It is used in places
% where we need to make repeated calls to get the actual partition
% (not the DocIdx) of an Index/Field/Term combination. NOTE: This, or something like it,
% should probably get moved to Riak Core in the future. 
-define(RINGTOP, trunc(math:pow(2,160)-1)).  % SHA-1 space

zip_with_partition_and_index(Postings) ->
    %% Get the number of partitions.
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    CHash =
        case Ring of
            {chstate, _, _, CH, _} -> CH;
            {chstate_v2, _, _, CH, _, _, _, _, _, _, _} -> CH
        end,
    {NumPartitions, _} = CHash,
    RingTop = ?RINGTOP,
    Inc = ?RINGTOP div NumPartitions,

    F = fun(Posting = {I,F,T,_,_,_}) ->
                <<IndexAsInt:160/integer>> = calc_partition(I, F, T),
                case (IndexAsInt - (IndexAsInt rem Inc) + Inc) of
                    RingTop   -> {{0, I}, Posting};
                    Partition -> {{Partition, I}, Posting}
                end;
           (Posting = {I,F,T,_,_}) ->
                <<IndexAsInt:160/integer>> = calc_partition(I, F, T),
                case (IndexAsInt - (IndexAsInt rem Inc) + Inc) of
                    RingTop   -> {{0, I}, Posting};
                    Partition -> {{Partition, I}, Posting}
                end
        end,
    [F(X) || X <- Postings].

%% The call to crypto:sha/N below *should* be a call to
%% riak_core_util:chash_key/N, but we don't allow Riak Search to
%% specify custom hashes. It just takes too long to look up for every
%% term and kills performance.
-spec calc_partition(index(), field(), term()) -> binary().
calc_partition(Index, Field, Term) ->
    crypto:sha(term_to_binary({Index, Field, Term})).
