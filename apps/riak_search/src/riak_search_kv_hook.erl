%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_kv_hook).
-export([precommit/1]).

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

-type search_fields() :: [{search_field(),search_data()}].
-type search_field() :: string().
-type search_data() :: string() | binary().
         

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

   
-spec to_modfun(list() | atom()) -> atom().
to_modfun(List) when is_list(List) ->
    %% Using list_to_atom here so that the extractor module
    %% does not need to be pre-loaded.  
    list_to_atom(List);
to_modfun(Atom) when is_list(Atom) ->
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
    Postings = riak_indexed_doc:postings(NewIdxDoc),
    SearchClient:index_terms(Postings),

    %% Store the indexed_doc for next time
    riak_indexed_doc:put(RiakClient, NewIdxDoc),
    ok.

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
-spec make_indexed_doc(index(), docid(), riak_object(), extractdef()) -> ok.
make_indexed_doc(Index, DocId, RiakObject, Extractor) ->
    Fields = run_extract(RiakObject, Extractor),
    IdxDoc0 = riak_indexed_doc:new(DocId, Fields, [], Index),
    {ok, IdxDoc} = riak_indexed_doc:analyze(IdxDoc0),
    IdxDoc.
                         
-spec make_index(riak_object()) -> binary().
make_index(RiakObject) ->
    riak_object:bucket(RiakObject).

-spec make_docid(riak_object()) -> binary().
make_docid(RiakObject) ->
    riak_object:key(RiakObject).
    
%% Run the extraction function against the RiakObject to get a list of
%% search fields and data
-spec run_extract(riak_object(), extractdef()) -> search_fields().
run_extract(RiakObject, {{modfun, Mod, Fun}, Args}) ->
    Mod:Fun(RiakObject, Args);
run_extract(_, _) ->
    throw({error, not_implemented}).
