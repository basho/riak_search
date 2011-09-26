-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(TIMEON, erlang:put(debug_timer, [now()|case erlang:get(debug_timer) == undefined of true -> []; false -> erlang:get(debug_timer) end])).
-define(TIMEOFF(Var), io:format("~s :: ~10.2f ms : ~p : ~p~n", [string:copies(" ", length(erlang:get(debug_timer))), (timer:now_diff(now(), hd(erlang:get(debug_timer)))/1000), ??Var, Var]), erlang:put(debug_timer, tl(erlang:get(debug_timer)))).
-endif.
-define(FMT(S, L), lists:flatten(io_lib:format(S, L))).

-define(DEFAULT_INDEX, <<"search">>).
-define(RESULTVEC_SIZE, 1000).
-define(OPKEY(Tag, Op), {Tag, element(2, Op)}).

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

-record(riak_search_field, {name,
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
