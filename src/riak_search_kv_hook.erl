%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_kv_hook).
-export([install/1,
         uninstall/1,
         precommit_def/0,
         precommit/1]).

-include("riak_search.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([run_mod_fun_extract_test_fun/3]).
-endif.

-define(DEFAULT_EXTRACTOR, {modfun, riak_search_kv_extractor, extract}).
-define(DEFAULT_ARGS,      undefined).

-type user_funterm() :: {modfun, user_modname(), user_funname()} |
                        {qfun, extract_qfun()} |
                        {jsanon, user_strorbin()} |
                        {jsanon, {user_strorbin(), user_strorbin()}} |
                        {jsfun, user_strorbin()}.
-type user_modname() :: string() | module().
-type user_funname() :: string() | atom().
-type user_strorbin() :: string() | binary().                       


-type extractdef() :: {funterm(), args()}.
-type funterm() :: {modfun, atom(), atom()} |
                   {qfun, extract_qfun()} |
                   {jsanon, binary()} |
                   {jsanon, {binary(), binary()}} |
                   {jsfun, binary()}.

-type riak_client() :: tuple(). % no good way to define riak_client
-type riak_object() :: tuple(). % no good way to define riak_object
-type search_client() :: tuple().       

-type extract_qfun() :: fun((riak_object(),any()) -> search_fields()).
-type args() :: any().

-type index() :: binary().
-type docid() :: binary().
-type idxdoc() :: tuple(). % #riak_indexed_doc{}

-type search_fields() :: [{search_field(),search_data()}].
-type search_field() :: string().
-type search_data() :: string() | binary().
    

%% Install the kv/search integration hook on the specified bucket
%% TODO: The code below can be
%% simplified. riak_core_bucket:set_bucket/2 does not require you to
%% set ALL bucket props, just the properties that have changed.
install(Bucket) -> 
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    CleanPrecommit = strip_precommit(BucketProps),
    case CleanPrecommit ++ [precommit_def()] of
        [{struct, _}]=Y ->
            UpdPrecommit=Y;
        Y ->
            UpdPrecommit=Y
    end,

    %% Update the bucket properties
    UpdBucketProps = lists:keyreplace(precommit, 1, BucketProps, 
                                      {precommit, UpdPrecommit}),
    riak_core_bucket:set_bucket(Bucket, UpdBucketProps).

%% Uninstall kv/search integration hook from specified bucket
uninstall(Bucket) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    CleanPrecommit = strip_precommit(BucketProps),

    %% Update the bucket properties
    UpdBucketProps = lists:keyreplace(precommit, 1, BucketProps, 
                                      {precommit, CleanPrecommit}),
    riak_core_bucket:set_bucket(Bucket, UpdBucketProps).

precommit_def() ->
    {struct, [{<<"mod">>,atom_to_binary(?MODULE, latin1)},
              {<<"fun">>,<<"precommit">>}]}.


%% Precommit hook for riak k/v and search integration.  Executes
%% the desired mapping on the riak object to produce a search
%% document to store in riak search.
%%
-spec precommit(riak_object()) -> {fail, any()} | riak_object().
precommit(RiakObject) ->
    Extractor = get_extractor(RiakObject),
    try
        case index_object(RiakObject, Extractor) of
            ok ->
                RiakObject;
            {error, Reason1} ->
                {fail, Reason1}
        end
    catch
        throw:Reason2 ->
            {fail, Reason2}
    end.

%% Decide if an object should be indexed, and if so the extraction function to 
%% pull out the search fields.
-spec get_extractor(riak_object()) -> {funterm(), any()}.
get_extractor(RiakObject) ->
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RiakObject)),
    validate_extractor(proplists:get_value(rs_extractfun, BucketProps, undefined)).

%% Validate the extraction function and normalize to {FunTerm, Args}
-spec validate_extractor(undefined |
                         user_funterm() |
                         {user_funterm(), args()}) -> {funterm(), args()}.
validate_extractor(undefined) ->
    {?DEFAULT_EXTRACTOR, ?DEFAULT_ARGS};
validate_extractor({struct, JsonExtractor}) ->
    Lang = proplists:get_value(<<"language">>, JsonExtractor),    
    validate_extractor(erlify_json_funterm(Lang, JsonExtractor));
validate_extractor({FunTerm, Args}) when is_tuple(FunTerm) ->
    {validate_funterm(FunTerm), Args};
validate_extractor(FunTerm) ->
    {validate_funterm(FunTerm), undefined}.

-spec validate_funterm(user_funterm()) -> funterm().
validate_funterm({modfun, Mod, Fun}) ->
    {modfun, to_modfun(Mod), to_modfun(Fun)};
validate_funterm({qfun, Fun}=FunTerm) when is_function(Fun) ->
    FunTerm;
validate_funterm({jsanon, {Bucket, Key}}) ->
    {jsanon, {to_binary(Bucket), to_binary(Key)}};
validate_funterm({jsanon, Source}) ->
    {jsanon, to_binary(Source)};
validate_funterm({jsfun, Name}) ->
    {jsfun, to_binary(Name)};
validate_funterm(FunTerm) ->
    throw({"cannot parse funterm", FunTerm}).

%% Decode a bucket property that was set using JSON/HTTP interface
erlify_json_funterm(<<"erlang">>, Props) ->
    Mod = to_modfun(proplists:get_value(<<"module">>, Props, undefined)),
    Fun = to_modfun(proplists:get_value(<<"function">>, Props, undefined)),
    Arg = proplists:get_value(<<"arg">>, Props, undefined),
    {{modfun, Mod, Fun}, Arg};
erlify_json_funterm(<<"javascript">>, Props) ->
    Source = proplists:get_value(<<"source">>, Props, undefined),
    Name = proplists:get_value(<<"name">>, Props, undefined),
    Bucket = proplists:get_value(<<"bucket">>, Props, undefined),
    Key = proplists:get_value(<<"key">>, Props, undefined),
    Arg = proplists:get_value(<<"arg">>, Props, undefined),
    case Source of
        undefined ->
            case Name of
                undefined ->
                    case (Bucket == undefined) orelse (Key == undefined) of
                        true ->
                            throw("javascript kv/search extractor must have"
                                  "name, source, or bucket and key");
                        _ ->
                            {{jsanon, {Bucket, Key}}, Arg}
                    end;
                _ ->
                    {{jsfun, Name}, Arg}
            end;
        _ ->
            {{jsanon, Source}, Arg}
    end;
erlify_json_funterm(Lang, _Props) ->
    throw({"kv/search extractors must be written in erlang or javascript", Lang}).

     

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
   
-spec to_binary(atom() | string() | binary()) -> binary().
to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin;
to_binary(Val) ->
    throw({"cannot convert to binary", Val}).

%%
%% Index the provided riak object and return ok on success.
%%
-spec index_object(riak_object(), extractdef()) -> ok.
index_object(RiakObject, Extractor) ->
    %% Set up
    {ok, RiakClient} = riak:local_client(),
    SearchClient = riak_search_client:new(RiakClient),
    Index = make_index(RiakObject),
    DocId = make_docid(RiakObject),

    %% Check the new doc is parsable and have it ready
    NewIdxDoc = make_indexed_doc(Index, DocId, RiakObject, Extractor),

    %% If all ok, remove the old entries and index the new
    remove_old_entries(RiakClient, SearchClient, Index, DocId),

    case NewIdxDoc of
        deleted ->
            riak_indexed_doc:delete(RiakClient, Index, DocId);
        _ ->
            %% Update the search index and store the indexed_doc in k/v
            Postings = riak_indexed_doc:postings(NewIdxDoc),
            SearchClient:index_terms(Postings),
            riak_indexed_doc:put(RiakClient, NewIdxDoc)
    end.

%% Remove any old index entries if they exist
-spec remove_old_entries(riak_client(), search_client(), index(), docid()) -> ok.
remove_old_entries(RiakClient, SearchClient, Index, DocId) ->
    case riak_indexed_doc:get(RiakClient, Index, DocId) of
        {error, notfound} ->
            ok;
        OldIdxDoc ->
            SearchClient:delete_doc_terms(OldIdxDoc)
    end.

%% Make an indexed document under Index/DocId from the RiakObject
-spec make_indexed_doc(index(), docid(), riak_object(), extractdef()) ->
                              idxdoc() | deleted.
make_indexed_doc(Index, DocId, RiakObject, Extractor) ->
    case riak_kv_util:is_x_deleted(RiakObject) of
        true ->
            deleted;
        false ->
            {ok, Schema} = riak_search_config:get_schema(Index),
            DefaultField = Schema:default_field(),
            Fields = run_extract(RiakObject, DefaultField, Extractor),
            IdxDoc = riak_indexed_doc:new(Index, DocId, Fields, []),
            riak_indexed_doc:analyze(IdxDoc)
    end.
 
-spec make_index(riak_object()) -> binary().
make_index(RiakObject) ->
    riak_object:bucket(RiakObject).

-spec make_docid(riak_object()) -> binary().
make_docid(RiakObject) ->
    riak_object:key(RiakObject).
    
%% Run the extraction function against the RiakObject to get a list of
%% search fields and data
-spec run_extract(riak_object(), string(), extractdef()) -> search_fields().
run_extract(RiakObject, DefaultField, {{modfun, Mod, Fun}, Arg}) ->
    Mod:Fun(RiakObject, DefaultField, Arg);
run_extract(RiakObject, DefaultField, {{qfun, Fun}, Arg}) ->
    Fun(RiakObject, DefaultField, Arg);
run_extract(RiakObject, DefaultField, {{Js, FunTerm}, Arg})
  when Js == jsanon; Js == jsfun ->
    Fun = if is_binary(FunTerm) -> FunTerm;
             is_tuple(FunTerm) ->
                  {Bucket, Key} = FunTerm,
                  {ok, Client} = riak:local_client(),
                  try 
                      {ok, JSObj} = Client:get(Bucket, Key, 1),
                      hd(riak_object:get_values(JSObj))
                  catch
                      error:{badmatch,{error,notfound}} ->
                          throw({fail, {"Extractor not found", {Bucket, Key}}})
                  end
          end,
    JsRObj = riak_object:to_json(RiakObject),
    case Arg of
        undefined ->
            JsArg = null;
        _ ->
            JsArg = Arg
    end,
    case riak_kv_js_manager:blocking_dispatch(?JSPOOL_SEARCH_EXTRACT, {{Js, Fun}, [JsRObj, DefaultField, JsArg]}, 5) of
        {ok, <<"fail">>} ->
            throw(fail);
        {ok, [{<<"fail">>, Message}]} ->
            throw({fail, Message});
        {ok, JsonFields} ->
            erlify_json_extract(JsonFields);
        {error, Error} ->
            error_logger:error_msg("Error executing kv/search hook: ~s",
                                   [Error]),
            throw({fail, Error})
    end;
run_extract(_, _, ExtractDef) ->
    throw({error, {not_implemented, ExtractDef}}).

erlify_json_extract(R) ->
    erlify_json_extract(R, []).

erlify_json_extract([], Acc) ->
    lists:reverse(Acc);
erlify_json_extract([{FieldName, FieldData} | Rest], Acc) when is_binary(FieldName),
                                                                  is_binary(FieldData) ->
    erlify_json_extract(Rest, [{FieldName, FieldData} | Acc]);
erlify_json_extract(R, _Acc) ->
    throw({fail, {bad_json_extractor, R}}).
        
%% Get the precommit hook from the bucket and strip any
%% existing index hooks.
strip_precommit(BucketProps) ->
    %% Get the current precommit hook
    case proplists:get_value(precommit, BucketProps, []) of
        X when is_list(X) ->
            CurrentPrecommit=X;
        {struct, _}=X ->
            CurrentPrecommit=[X]
    end,
    
    %% Add kv/search hook - make sure there are not duplicate entries
    CurrentPrecommit -- [precommit_def()].

-ifdef(TEST).

install_test() ->
    application:load(riak_core),
    %% Make sure the bucket proplist is not an improper list by
    %% setting the defaults, normally this would be done by starting
    %% the riak_core app.
    riak_core_bucket:append_bucket_defaults([]),
    RingEvtPid = maybe_start_link(riak_core_ring_events:start_link()),
    RingMgrPid = maybe_start_link(riak_core_ring_manager:start_link()),

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
                             {small_vclock,10},
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

run_mod_fun_extract_test() ->
    %% Try the anonymous function
    TestObj = conflict_test_object(),
    Extractor = validate_extractor({{modfun, ?MODULE, run_mod_fun_extract_test_fun}, undefined}),
    ?assertEqual([{<<"data">>,<<"some data">>}],
                 run_extract(TestObj, <<"data">>, Extractor)).
 
run_mod_fun_extract_test_fun(O, DefaultValue, _Args) ->
    StrVals = [binary_to_list(B) || B <- riak_object:get_values(O)],
    Data = string:join(StrVals, " "),
    [{DefaultValue, list_to_binary(Data)}].

run_qfun_extract_test() ->
    %% Try the anonymous function
    TestObj = conflict_test_object(),
    Fun1 = fun(O, D, _Args) ->
                   StrVals = [binary_to_list(B) || B <- riak_object:get_values(O)],
                   Data = string:join(StrVals, " "),
                   [{D, list_to_binary(Data)}]
           end,
    Extractor = validate_extractor({{qfun, Fun1}, undefined}),
    ?assertEqual([{<<"data">>,<<"some data">>}],
                 run_extract(TestObj, <<"data">>, Extractor)).
 
    

anon_js_extract_test() ->
    maybe_start_app(sasl),
    maybe_start_app(erlang_js),
    JsSup = maybe_start_link(riak_kv_js_sup:start_link()),
    JsMgr = maybe_start_link(riak_kv_js_manager:start_link(?JSPOOL_SEARCH_EXTRACT, 2)),

    %% Anonymous JSON function with default argument
    %% Join together all the values in a search field
    %% called "data" and the argument as "arg"
    JustObjectSource = "function(o, d) {
                var vals = [];
                for (var i = 0; i < o.values.length; i++) {
                  vals.push(o.values[i].data);
                }
                data = vals.join(\" \");
                return {\"data\":data};
              }",
    ObjectArgSource = "function(o,d,a) {
                var vals = [];
                for (var i = 0; i < o.values.length; i++) {
                  vals.push(o.values[i].data);
                }
                data = vals.join(\" \");
                return {\"data\":data, \"arg\":a.f};
              }",
 
    %% Try the anonymous function
    O = conflict_test_object(),
    Extractor1 = validate_extractor({{jsanon, JustObjectSource}, undefined}),
    ?assertEqual([{<<"data">>,<<"some data">>}],
                 run_extract(O, <<"data">>, Extractor1)),
                 
    %% Anonymous JSON function with provided argument
    %% Arg = {struct [{<<"f">>,<<"v">>}]},
    Arg = {struct, [{<<"f">>,<<"v">>}]},
    Extractor2 = validate_extractor({{jsanon, ObjectArgSource}, Arg}),
    ?assertEqual([{<<"data">>,<<"some data">>}, 
                  {<<"arg">>, <<"v">>}],
                 run_extract(O, <<"value">>, Extractor2)),

    stop_pid(JsMgr),
    stop_pid(JsSup),
    application:stop(erlang_js).

conflict_test_object() ->
    O0 = riak_object:new(<<"b">>,<<"k">>,<<"v">>),
    riak_object:set_contents(O0, [{dict:new(), <<"some">>},
                                  {dict:new(), <<"data">>}]).
    

maybe_start_app(App) ->
    case application:start(App) of
        {error, {already_started, _}} ->
            ok;
        ok ->
            ok
    end.

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
    
