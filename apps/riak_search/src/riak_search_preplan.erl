-module(riak_search_preplan).
-export([
         preplan/4
]).
-include("riak_search.hrl").
-record(config, { default_index, default_field, facets }).


preplan(OpList, DefaultIndex, DefaultField, Facets) ->
    preplan(OpList, #config { 
              default_index=DefaultIndex,
              default_field=DefaultField,
              facets=Facets }).

preplan(OpList, Config) ->
    OpList1 = pass1(OpList, Config),
    OpList2 = pass2(OpList1, Config),
    pass3(OpList2, Config).
    
%% FIRST PASS - Normalize incoming qil. 
%% - We should move this to the qilr parser.
%% - Turn field/4 into field/3
%% - Turn group statements into ANDs and ORs.
%% - Wrap prohibited terms into an #lnot.
pass1(OpList, Config) when is_list(OpList) ->
    lists:flatten([pass1(X, Config) || X <- OpList]);

pass1({field, Field, Term, Flags}, Config) ->
    TermOp = #term { string=Term, flags=Flags},
    pass1(#field { field=Field, ops=[TermOp]}, Config);

pass1(Op = #group {}, Config) ->
    %% Qilr parser returns nested lists in a group.
    OpList = lists:flatten([Op#group.ops]),
    case length(OpList) == 1 of
        true -> 
            %% Single op, so get rid of the group.
            pass1(OpList, Config);
        false -> 
            %% Multiple ops. Pull out any terms where required flag is
            %% set, make those on AND. The rest are an OR. AND the
            %% results together.
            F = fun(X) -> is_record(X, term) andalso (?IS_REQUIRED(X) orelse ?IS_PROHIBITED(X)) end,
            {RequiredOps, NonRequiredOps} = lists:splitwith(F, OpList),
            if 
                RequiredOps /= [] andalso NonRequiredOps == [] ->
                    pass1(#land { ops=RequiredOps }, Config);
                RequiredOps == [] andalso NonRequiredOps /= [] ->
                    pass1(#lor { ops=NonRequiredOps }, Config);
                true ->
                    NewOp = #land { ops=[#land { ops=RequiredOps },
                                         #lor { ops=NonRequiredOps }]},
                    pass1(NewOp, Config)
            end
    end;

pass1(Op = #term {}, Config) ->
    case ?IS_PROHIBITED(Op) of
        true -> 
            %% Rewrite a prohibited term to be a not.
            NewOp = #lnot { 
              ops=[Op#term { flags=[] }] 
             },
            pass1(NewOp, Config);
        false ->
            Op
    end;

pass1(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass1(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).



%% SECOND PASS
%% - Turn #term strings into "index.field.term"
%% - TODO: Collapse facets
%% - TODO: Expand fuzzy terms into a bunch of or's.
%% - TODO: Expand range into a bunch of or's.
%% - TODO: Wrap things in #node to transfer control based on bucket stats.
pass2(OpList, Config) when is_list(OpList) ->
    [pass2(Op, Config) || Op <- OpList];

pass2(Op = #field {}, Config) ->
    Config1 = Config#config { default_field=Op#field.field },
    pass2(to_list(Op#field.ops), Config1);

pass2(Op = #term {}, Config) ->
    DefIndex = Config#config.default_index,
    DefField = Config#config.default_field,
    NewString = string:join([DefIndex, DefField, Op#term.string], "."),
    Op#term { string = NewString };

pass2(Op = #land {}, Config) ->
    OpList = facetize(Op#land.ops, Config#config.facets),
    Op#land { ops=pass2(OpList, Config) };

pass2(Op = #lor {}, Config) ->
    OpList = facetize(Op#lor.ops, Config#config.facets),
    Op#lor { ops=pass2(OpList, Config) };

pass2(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass2(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).


%% Given a list of operations, fold the facets into the non-facets.
facetize(Ops, Facets) ->
    %% 
    


%% THIRD PASS
%% Collapse nested lnots.
pass3(OpList, Config) when is_list(OpList) ->
    [pass3(X, Config) || X <- OpList];

pass3(Op = #lnot {}, Config) ->
    F = fun(X) ->
                case is_record(X, lnot) of
                    true ->
                        pass3(X, Config);
                    false ->
                        #lnot { ops=to_list(pass3(X, Config)) }
                end
        end,
    [F(X) || X <- to_list(Op#lnot.ops)];

pass3(Op, Config) -> 
    F = fun(X) -> lists:flatten([pass3(Y, Config) || Y <- to_list(X)]) end,
    riak_search_op:preplan_op(Op, F).

to_list(L) when is_list(L) -> L;
to_list(T) when is_tuple(T) -> [T].

