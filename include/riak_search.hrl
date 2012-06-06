%%%===================================================================
%%% Macros
%%%===================================================================

-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.
-define(FMT(S, L), lists:flatten(io_lib:format(S, L))).

-define(INTEGER_ANALYZER,    {erlang, text_analyzers, integer_analyzer_factory}).
-define(NOOP_ANALYZER,       {erlang, text_analyzers, noop_analyzer_factory}).

-define(DEFAULT_INDEX, <<"search">>).
-define(RESULTVEC_SIZE, 1000).

-define(DEFAULT_RESULT_SIZE, 10).
-define(DEFAULT_TIMEOUT, 60000).

%%%===================================================================
%%% Types
%%%===================================================================

-type index() :: binary().
-type field() :: binary().
-type s_term() :: binary().

-type stream_ref() :: {stream_response, reference()}.

-type search_fields() :: [{search_field(),search_data()}].
-type search_field() :: string() | binary().
-type search_data() :: string() | binary().


%%%===================================================================
%%% Records
%%%===================================================================

-record(search_state, {
          parent=undefined,
          index=undefined,
          field=undefined,
          num_terms=0,
          num_docs=0,
          query_norm=0,
          filter=undefined
}).

-record(term, {
          %% A unique id used during planning.
          id=make_ref(),
          %% The term to query.
          s, 
          %% The node weights
          weights,
          %% DocFrequency,
          doc_freq=0,
          %% Boost
          boost=1
         }).

-record(node, {
          %% A unique id used during planning.
          id=make_ref(),
          %% The node to run on.
          node=node(),
          %% The child ops.
          ops=[]
         }).

-record(score, {
          boost=1,
          ops=[]
          }).

-record(proximity, {
          id=make_ref(),
          ops=[],
          proximity=999999
         }).

-record(range_sized, {
          id=make_ref(),
          from=undefined,
          to=undefined,
          size, 
          vnode
         }).

-record(range_worker, {
          id=make_ref(),
          from=undefined,
          to=undefined,
          size, 
          vnode
         }).

%% Mockterm is used for QC unit tests to test middle logic. Doesn't
%% hit a backing store.
-record(mockterm,         {results=[]}).

-record(riak_idx_doc, {index,
                       id,
                       fields=[],
                       props=[],
                       inline_fields=[],
                       analyzed_flag=false}).

-record(riak_search_ref, {id,
                          termcount,
                          inputcount,
                          querynorm}).

-record(riak_search_field, {name :: binary(),
                            aliases=[],
                            type,
                            padding_size,
                            padding_char,
                            required=false,
                            skip=false,
                            dynamic=false,
                            analyzer_factory,
                            analyzer_args,
                            inline=false}).
