-module(riak_search_preplan).
-export([
         preplan/3
]).
-include("riak_search.hrl").

preplan(DefIndex, DefField, OpList) ->
    OpList1 = pass1(DefIndex, DefField, OpList),
    OpList2 = pass2(OpList1),
    pass3(OpList2).
    
%% FIRST PASS
%% - Wrap prohibited terms into an #lnot.
%% - Turn #term strings into "index.field.term"
%% - Make sure all .ops are lists.
%% - Turn group statements into lists.
pass1(DefIndex, DefField, OpList) when is_list(OpList) ->
    lists:flatten([pass1(DefIndex, DefField, X) || X <- OpList]);

pass1(DefIndex, DefField, Op = #group {}) ->
    pass1(DefIndex, DefField, to_list(Op#group.ops));

pass1(DefIndex, _DefField, Op = #field {}) ->
    pass1(DefIndex, Op#field.field, to_list(Op#field.ops));

pass1(DefIndex, DefField, Op = #term {}) ->
    case ?IS_PROHIBITED(Op#term.flags) of
        true -> 
            %% Rewrite a prohibited term to be a not.
            NewOp = #lnot { 
              ops=[Op#term { flags=[] }] 
             },
            pass1(DefField, DefField, NewOp);
        false ->
            NewString = string:join([DefIndex, DefField, Op#term.string], "."),
            Op#term { string = NewString }
    end;

pass1(DefIndex, DefField, Op) -> 
    F = fun(X) -> lists:flatten([pass1(DefIndex, DefField, Y) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).



%% SECOND PASS
%% - TODO: Expand fuzzy terms into a bunch of or's.
%% - TODO: Expand range into a bunch of or's.
%% - TODO: Wrap things in #node to transfer control based on bucket stats.
pass2(OpList) when is_list(OpList) ->
    [pass2(Op) || Op <- OpList];

pass2(Op) -> 
    F = fun(X) -> lists:flatten([pass2(Y) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).



%% THIRD PASS
%% Collapse nested lnots.
pass3(OpList) when is_list(OpList) ->
    [pass3(X) || X <- OpList];

pass3(Op = #lnot {}) ->
    F = fun(X) ->
                case is_record(X, lnot) of
                    true ->
                        pass3(X);
                    false ->
                        #lnot { ops=to_list(pass3(X)) }
                end
        end,
    [F(X) || X <- to_list(Op#lnot.ops)];

pass3(Op) -> 
    F = fun(X) -> lists:flatten([pass3(Y) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).

to_list(L) when is_list(L) -> L;
to_list(T) when is_tuple(T) -> [T].

