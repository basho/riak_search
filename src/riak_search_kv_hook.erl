%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_kv_hook).
-export([install/1,
         uninstall/1,
         precommit_def/0,
         precommit/1,
         fixup/2]).

-include("riak_search.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([run_mod_fun_extract_test_fun/3]).
-endif.

-define(EX_PROP_NAME, search_extractor).
-define(LEGACY_EX_PROP_NAME, rs_extractfun).
-define(DEFAULT_EXTRACTOR, {riak_search_kv_extractor, extract, undefined}).

-type user_funterm() :: {modfun, user_modname(), user_funname()} |
                        {qfun, extract_qfun()} |
                        mfargs().
-type user_modname() :: string() | module().
-type user_funname() :: string() | atom().

-type mfargs() :: {atom(), atom(), args()}.
-type fargs() :: {function(), args()}.
-type extractdef() :: mfargs() | fargs().

-type obj() :: riak_object:riak_object().

-type extract_qfun() :: fun((obj(),any()) -> search_fields()).
-type args() :: any().

-type docid() :: binary().
-type idxdoc() :: #riak_idx_doc{}.

%% Bucket fixup hook for actually setting up the search hook
fixup(Bucket, BucketProps) ->
    case proplists:get_value(search, BucketProps) of
        true ->
            CurrentPrecommit = get_precommit(BucketProps),
    
            UpdPrecommit = case has_search_precommit(BucketProps) of
                false ->
                    CurrentPrecommit ++ [precommit_def()];
                _ ->
                    CurrentPrecommit
            end,

            %% Update the bucket properties
            {ok, lists:keystore(precommit, 1, BucketProps, 
                    {precommit, UpdPrecommit})};
        false ->
            %% remove the precommit hook, if any
            CleanPrecommit = strip_precommit(BucketProps),
            %% Update the bucket properties
            UpdBucketProps = lists:keystore(precommit, 1, BucketProps, 
                {precommit, CleanPrecommit}),
            {ok, UpdBucketProps};
        _ when Bucket /= default ->
            %% rolling upgrade or no search ever configured
            %% check if the hook is present.
            %% we don't do this on the default bucket because we don't want to
            %% inherit the search parameter.
            case has_search_precommit(BucketProps) of
                true ->
                    {ok, [{search, true}|BucketProps]};
                false ->
                    {ok, [{search, false}|BucketProps]}
            end;
        _ ->
            {ok, BucketProps}
    end.

%% Install the kv/search integration hook on the specified bucket
install(Bucket) -> 
    riak_core_bucket:set_bucket(Bucket, [{search, true}]).

%% Uninstall kv/search integration hook from specified bucket
uninstall(Bucket) ->
    riak_core_bucket:set_bucket(Bucket, [{search, false}]).

precommit_def() ->
    {struct, [{<<"mod">>,atom_to_binary(?MODULE, latin1)},
              {<<"fun">>,<<"precommit">>}]}.


%% Precommit hook for riak k/v and search integration.  Executes
%% the desired mapping on the riak object to produce a search
%% document to store in riak search.
-spec precommit(obj()) -> {fail, any()} | obj().
precommit(Obj) ->
    T1 = os:timestamp(),
    riak_search_stat:update(index_begin),
    Extractor = get_extractor(Obj),
    try
        case index_object(Obj, Extractor) of
            ok ->
                T2 = os:timestamp(),
                TD = timer:now_diff(T2, T1),
                riak_search_stat:update({index_end, TD}),
                Obj;
            {error, Reason1} ->
                {fail, Reason1}
        end
    catch
        throw:Reason2 ->
            {fail, Reason2}
    end.

%% Decide if an object should be indexed, and if so the extraction function to 
%% pull out the search fields.
-spec get_extractor(obj()) -> extractdef().
get_extractor(RiakObject) ->
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RiakObject)),
    Ex = try_keys([?LEGACY_EX_PROP_NAME, ?EX_PROP_NAME], BucketProps),
    validate_extractor(Ex).

%% Validate the extraction function and normalize to {FunTerm, Args}
-spec validate_extractor(undefined |
                         user_funterm() |
                         {user_funterm(), args()}) ->
                                extractdef().
validate_extractor(undefined) ->
    ?DEFAULT_EXTRACTOR;
validate_extractor({struct, Json}) ->
    validate_extractor(erlify_json_funterm(Json));
validate_extractor({{modfun, M, F}, Arg}) ->
    {to_modfun(M), to_modfun(F), Arg};
validate_extractor({modfun, M, F}) ->
    {to_modfun(M), to_modfun(F), undefined};
validate_extractor({{qfun, F}, Arg}) when is_function(F) -> {F, Arg};
validate_extractor({qfun, F}) when is_function(F) -> {F, undefined};
validate_extractor({M, F}) when is_atom(M), is_atom(F) -> {M, F, undefined};
validate_extractor({M, F, Arg}) when is_atom(M), is_atom(F) -> {M, F, Arg};
validate_extractor(Other) -> throw({invalid_extractor, Other}).

%% Decode a bucket property that was set using JSON/HTTP interface
erlify_json_funterm(Props) ->
    M = try_keys([<<"module">>, <<"mod">>], Props),
    F = try_keys([<<"function">>, <<"fun">>], Props),
    Arg = proplists:get_value(<<"arg">>, Props),
    {to_modfun(M), to_modfun(F), Arg}.

try_keys([], _) -> undefined;
try_keys([K|T], Props) ->
    case proplists:get_value(K, Props) of
        undefined -> try_keys(T, Props);
        V -> V
    end.

-spec to_modfun(list() | atom()) -> atom().
to_modfun(List) when is_list(List) ->
    %% Using list_to_atom here so that the extractor module
    %% does not need to be pre-loaded.  
    list_to_atom(List);
to_modfun(Binary) when is_binary(Binary) ->
    binary_to_atom(Binary, utf8);
to_modfun(Atom) when is_atom(Atom) ->
    Atom;
to_modfun(Val) ->
    throw({"cannot convert to module/function name", Val}).

%%
%% Index the provided riak object and return ok on success.
%%
-spec index_object(obj(), extractdef()) -> ok | {error, Reason::any()}.
index_object(RiakObject, Extractor) ->
    %% Set up
    {ok, RiakClient} = riak:local_client(),
    SearchClient = riak_search_client:new(RiakClient),
    Index = make_index(RiakObject),
    DocId = make_docid(RiakObject),

    %% Check the new doc is parsable and have it ready
    NewIdxDoc = make_indexed_doc(Index, DocId, RiakObject, Extractor),

    %% If all ok, remove the old entries and index the new
    riak_indexed_doc:remove_entries(RiakClient, SearchClient, Index, DocId),

    case NewIdxDoc of
        deleted ->
            riak_indexed_doc:delete(RiakClient, Index, DocId);
        _ ->
            %% Update the search index and store the indexed_doc in k/v
            Postings = riak_indexed_doc:postings(NewIdxDoc),
            riak_search_stat:update({index_entries, length(Postings)}),
            SearchClient:index_terms(Postings),
            riak_indexed_doc:put(RiakClient, NewIdxDoc)
    end.

%% Make an indexed document under Index/DocId from the RiakObject
-spec make_indexed_doc(index(), docid(), obj(), extractdef()) ->
                              idxdoc() | deleted.
make_indexed_doc(Index, DocId, RiakObject, Extractor) ->
    case riak_kv_util:is_x_deleted(RiakObject) of
        true ->
            deleted;
        false ->
            {ok, Schema} = riak_search_config:get_schema(Index),
            DefaultField = Schema:default_field(),
            Fields = run_extract(RiakObject, DefaultField, Extractor),
            IdxDoc0 = riak_indexed_doc:new(Index, DocId, Fields, []),
            IdxDoc = riak_indexed_doc:analyze(IdxDoc0),
            IdxDoc
    end.
 
-spec make_index(obj()) -> binary().
make_index(RiakObject) ->
    riak_object:bucket(RiakObject).

-spec make_docid(obj()) -> binary().
make_docid(RiakObject) ->
    riak_object:key(RiakObject).
    
%% Run the extraction function against the RiakObject to get a list of
%% search fields and data
-spec run_extract(obj(), string(), extractdef()) -> search_fields().
run_extract(RiakObject, DefaultField, {M, F, A}) ->
    M:F(RiakObject, DefaultField, A);
run_extract(RiakObject, DefaultField, {F, A}) ->
    F(RiakObject, DefaultField, A).
        
%% Get the precommit hook from the bucket and strip any
%% existing index hooks.
strip_precommit(BucketProps) ->
    %% Get the current precommit hook
    CurrentPrecommit = get_precommit(BucketProps),
    %% Add kv/search hook - make sure there are not duplicate entries
    CurrentPrecommit -- [precommit_def()].

%% Check if the precommit is already installed
has_search_precommit(BucketProps) ->
    Precommit = get_precommit(BucketProps),
    lists:member(precommit_def(), Precommit).

get_precommit(BucketProps) ->
    Precommit = case proplists:get_value(precommit, BucketProps, []) of
        X when is_list(X) ->
            X;
        {struct, _}=X ->
            [X]
    end,
    
    %% strip out any duplicate search hooks
    Count = lists:foldl(fun(E, Acc) ->
                case E == precommit_def() of
                    true ->
                        Acc +1;
                    _ ->
                        Acc
                end
        end, 0, Precommit),

    case Count > 1 of
        true ->
            %% more than one precommit found, remove all but one
            Precommit -- lists:duplicate(Count - 1, precommit_def());
        _ ->
            Precommit
    end.


-ifdef(TEST).

install_test() ->
    application:load(riak_core),
    application:set_env(riak_core, bucket_fixups, [{riak_search,
                riak_search_kv_hook}]),
    %% Make sure the bucket proplist is not an improper list by
    %% setting the defaults, normally this would be done by starting
    %% the riak_core app.
    riak_core_bucket:append_bucket_defaults([]),
    RingEvtPid = maybe_start_link(riak_core_ring_events:start_link()),
    RingMgrPid = maybe_start_link(riak_core_ring_manager:start_link(test)),

    WithoutPrecommitProps = [{n_val,3},
                             {allow_mult,false},
                             {last_write_wins,false},
                             {precommit,[]},
                             {postcommit,[]},
                             {chash_keyfun,{riak_core_util,chash_std_keyfun}},
                             {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
                             {old_vclock,86400},
                             {young_vclock,20},
                             {big_vclock,50},
                             {small_vclock,50},
                             {r,quorum},
                             {w,quorum},
                             {dw,quorum},
                             {rw,quorum}],
    WithPrecommitProps =  [{precommit,{struct,[{<<"mod">>,<<"mod">>},
                                               {<<"fun">>,<<"fun">>}]}} |
                           WithoutPrecommitProps],
    riak_core_bucket:set_bucket("no_precommit", WithoutPrecommitProps),
    riak_core_bucket:set_bucket("other_precommit", WithPrecommitProps),
    ?assertEqual(false, search_hook_present("no_precommit")),
    ?assertEqual(false, search_hook_present("other_precommit")),

    install("no_precommit"),
    ?assertEqual(true, search_hook_present("no_precommit")),

    install("no_precommit"),
    ?assertEqual(true, search_hook_present("no_precommit")),

    install("other_precommit"),
    ?assertEqual(true, search_hook_present("other_precommit")),

    install("other_precommit"),
    ?assertEqual(true, search_hook_present("other_precommit")),

    stop_pid(RingMgrPid),
    stop_pid(RingEvtPid),
    application:unset_env(riak_core, bucket_fixups),
    ok.

search_hook_present(Bucket) ->
    Props = riak_core_bucket:get_bucket(Bucket),
    Precommit = proplists:get_value(precommit, Props, []),
    IndexHook = precommit_def(),
    case Precommit of
        L when is_list(L) ->
            lists:member(IndexHook, Precommit);
        T when is_tuple(T) ->
            Precommit == IndexHook
    end.

extractor_test() ->
    ?assertEqual({riak_search_kv_extractor, extract, undefined},
                 validate_extractor(undefined)),
    ?assertEqual({m, f, arg}, validate_extractor({m, f, arg})),
    ?assertEqual({m, f, undefined}, validate_extractor({m, f})),

    J1 = {struct, [{<<"mod">>, <<"m">>}, {<<"fun">>, <<"f">>}]},
    ?assertEqual({m, f, undefined}, validate_extractor(J1)),

    J2 = {struct, [{<<"mod">>, <<"m">>}, {<<"fun">>, <<"f">>},
                   {<<"arg">>, <<"arg">>}]},
    ?assertEqual({m, f, <<"arg">>}, validate_extractor(J2)),

    %% TODO Remove legacy stuff in future version -- everything below
    %% is legacy support
    ?assertEqual({m, f, undefined},
                 validate_extractor({modfun, m, f})),

    ?assertEqual({m, f, arg},
                 validate_extractor({{modfun, m, f}, arg})),

    F = fun(_Obj, _Arg) -> noop end,
    ?assertEqual({F, undefined},
                 validate_extractor({qfun, F})),

    ?assertEqual({F, arg},
                 validate_extractor({{qfun, F}, arg})),

    J3 = {struct, [{<<"language">>, <<"erlang">>}, {<<"module">>, <<"m">>},
                   {<<"function">>, <<"f">>}]},
    ?assertEqual({m, f, undefined}, validate_extractor(J3)),

    J4 = {struct, [{<<"language">>, <<"erlang">>}, {<<"module">>, <<"m">>},
                   {<<"function">>, <<"f">>}, {<<"arg">>, <<"arg">>}]},
    ?assertEqual({m, f, <<"arg">>}, validate_extractor(J4)).

run_mod_fun_extract_test() ->
    %% Try the anonymous function
    TestObj = conflict_test_object(),
    Extractor = validate_extractor({?MODULE, run_mod_fun_extract_test_fun}),
    ?assertEqual([{<<"data">>,<<"some data">>}],
                 run_extract(TestObj, <<"data">>, Extractor)).
 
run_mod_fun_extract_test_fun(O, DefaultValue, _Args) ->
    StrVals = [binary_to_list(B) || B <- riak_object:get_values(O)],
    Data = string:join(StrVals, " "),
    [{DefaultValue, list_to_binary(Data)}].

run_qfun_extract_test() ->
    %% TODO qfun doesn't work on a running system, probably should
    %% remove support.
    TestObj = conflict_test_object(),
    Fun1 = fun(O, D, _Args) ->
                   StrVals = [binary_to_list(B) || B <- riak_object:get_values(O)],
                   Data = string:join(StrVals, " "),
                   [{D, list_to_binary(Data)}]
           end,
    Extractor = validate_extractor({{qfun, Fun1}, undefined}),
    ?assertEqual([{<<"data">>,<<"some data">>}],
                 run_extract(TestObj, <<"data">>, Extractor)).

conflict_test_object() ->
    O0 = riak_object:new(<<"b">>,<<"k">>,<<"v">>),
    riak_object:set_contents(O0, [{dict:new(), <<"some">>},
                                  {dict:new(), <<"data">>}]).
    
maybe_start_link({ok, Pid}) -> 
    Pid;
maybe_start_link({error, {already_started, _}}) ->
    undefined.
        
stop_pid(undefined) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, kill).

-endif. % TEST
