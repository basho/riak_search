-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(TIMEON, erlang:put(debug_timer, [now()|case erlang:get(debug_timer) == undefined of true -> []; false -> erlang:get(debug_timer) end])).
-define(TIMEOFF(Var), io:format("~s :: ~10.2f ms : ~p : ~p~n", [string:copies(" ", length(erlang:get(debug_timer))), (timer:now_diff(now(), hd(erlang:get(debug_timer)))/1000), ??Var, Var]), erlang:put(debug_timer, tl(erlang:get(debug_timer)))).
-endif.

-define(DEFAULT_INDEX, <<"search">>).
-define(DEFAULT_FIELD, <<"value">>).
%% -define(IS_TERM_PROHIBITED(Op), lists:member(prohibited, Op#term.options)).
%% -define(IS_TERM_REQUIRED(Op), lists:member(required, Op#term.options)).
%% -define(IS_TERM_INLINE(Op), lists:member(inline, Op#term.options)).
%% -define(IS_TERM_PROXIMITY(Op), (proplists:get_value(proximity, Op#term.options) /= undefined)).
%% -define(IS_TERM_WILDCARD_ALL(Op), lists:member({wildcard, all}, Op#term.options)).
%% -define(IS_TERM_WILDCARD_ONE(Op), lists:member({wildcard, one}, Op#term.options)).
-define(RESULTVEC_SIZE, 1000).
-define(OPKEY(Tag, Op), {Tag, element(2, Op)}).

-record(search_state, {
          index=undefined,
          field=undefined,
          num_terms=0,
          num_docs=0,
          query_norm=0,
          props=[]
}).

-record(term, {
          %% A unique id used during planning.
          id=make_ref(),
          %% The term to query.
          s, 
          %% Filter any results that return false.
          filter=fun riak_search_op_term:default_filter/2, 
          %% Transform results.
          transform=fun riak_search_op_term:default_transform/1
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

%% %% Pre-plan Operators...

%% %% Q will be normalized to {"index", "field", "term"} in
%% %% riak_search_preplan:normalize_term/2
%% -record(term,             {q, options=[]}).
%% -record(range,            {q, size, options=[]}).
%% %% Mockterm is used for QC unit tests to test middle logic. Doesn't
%% %% hit a backing store.
%% -record(mockterm,         {results=[]}).

%% %% #lnot's are collapsed down to the #term level in
%% %% riak_search_preplan:pass5/2.
%% -record(lnot,             {ops}).

%% -record(land,             {ops}).
%% -record(phrase,           {phrase, props=[]}).
%% -record(lor,              {ops}).
%% -record(group,            {ops}).
%% -record(field,            {field, ops}).
%% -record(inclusive_range,  {start_op, end_op}).
%% -record(exclusive_range,  {start_op, end_op}).
%% -record(node,             {ops, node}).
%% -record(proximity,        {ops, proximity}).

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
