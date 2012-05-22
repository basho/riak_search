-module(riak_search_pb_query).

-include_lib("riak_pb/include/riak_search_pb.hrl").
-include("riak_solr.hrl").
-include("riak_search.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-import(riak_search_utils, [to_atom/1, to_integer/1, to_binary/1, to_boolean/1, to_float/1]).
-import(riak_pb_search_codec, [encode_search_doc/1]).

-record(?MODULE, {client}).
-define(state, #?MODULE).

%% @doc init/0 callback. Returns the service internal start state.
-spec init() -> any().
init() ->
    {ok, C} = riak_search:local_client(),
    ?state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#rpbsearchqueryreq{index=Index, sort=Sort0, fl=FL0, presort=Presort0}=Msg, ?state{client=Client}=State) ->
    case riak_search_config:get_schema(Index) of
        {ok, Schema0} ->
            case parse_squery(Msg) of
                {ok, SQuery} ->
                    %% Construct schema, query, and filter
                    Schema = replace_schema_defaults(SQuery, Schema0),
                    {ok, QueryOps} = Client:parse_query(Schema, SQuery#squery.q),
                    {ok, FilterOps} = Client:parse_filter(Schema, SQuery#squery.filter),
                    %% Validate
                    DocKey = Schema:unique_key(),
                    FL = default(FL0, <<"*">>),
                    Sort = default(Sort0, <<"none">>),
                    Presort = to_atom(default(Presort0, <<"score">>)),
                    if
                        FL == [DocKey] andalso Sort /= <<"none">> ->
                            {error, riak_search_utils:err_msg(fl_id_with_sort), State};
                        true ->
                            %% Execute
                            Result = run_query(Client, Schema, SQuery, QueryOps, FilterOps, Presort, FL),
                            {reply, encode_results(Result), State}
                    end;
                {error, missing_query} ->
                    {error, "Missing query", State}
            end;
        Error ->
            {error, {format, "Could not parse schema '~s': ~p", [Index, Error]}, State}
    end.

%% @doc process_stream/3 callback. Ignored.
process_stream(_,_,State) ->
    {ignore, State}.


%% ---------------------------------
%% Internal functions
%% ---------------------------------
run_query(Client, Schema, #squery{query_start=QStart, query_rows=QRows},
          QueryOps, FilterOps, Presort, FL) ->
    UK = Schema:unique_key(),
    if
        FL == [UK] ->
            MaxScore = 0.0,
            {NumFound, Results} = Client:search(Schema, QueryOps, FilterOps,
                                                QStart, QRows, Presort, ?DEFAULT_TIMEOUT),
            Docs = [ [{UK, DocID}] || {_, DocID, _} <- Results ];
        true ->
            {NumFound, MaxScore, Results} = Client:search_doc(Schema, QueryOps, FilterOps,
                                                              QStart, QRows, Presort,
                                                              ?DEFAULT_TIMEOUT),
            Docs = [ [{UK, riak_indexed_doc:id(Doc)}|filter_fields(Doc, FL)] ||
                Doc <- Results ]
    end,
    {NumFound, MaxScore, Docs}.

encode_results({NumFound, MaxScore, Docs}) ->
    #rpbsearchqueryresp{
                docs = [ encode_search_doc(Doc) || Doc <- Docs ],
                max_score = to_float(MaxScore),
                num_found = NumFound
               }.
filter_fields(Doc, [<<"*">>]) ->
    riak_indexed_doc:fields(Doc);
filter_fields(Doc, <<"*">>) ->
    riak_indexed_doc:fields(Doc);
filter_fields(Doc, FL) ->
    [ Field || {Key, _}=Field <- riak_indexed_doc:fields(Doc),
               lists:member(to_binary(Key), FL) ].


parse_squery(#rpbsearchqueryreq{q = <<>>}) ->
    {error, missing_query};
parse_squery(#rpbsearchqueryreq{q=Query,
                                rows=Rows, start=Start,
                                filter=Filter,
                                df=DefaultField, op=DefaultOp}) ->
    {ok, #squery{q=Query,
                 filter=default(Filter, ""),
                 default_op=default(DefaultOp, undefined),
                 default_field=default(DefaultField,undefined),
                 query_start=default(Start, 0),
                 query_rows=default(Rows, ?DEFAULT_RESULT_SIZE)}}.

default(undefined, Default) ->
    Default;
default(Value, _) ->
    Value.

%% @private
%% @todo factor out of riak_solr_searcher_wm
%% Override the provided schema with a new default field, if one is
%% supplied in the query string.
replace_schema_defaults(SQuery, Schema0) ->
    Schema1 = case SQuery#squery.default_op of
                  undefined ->
                      Schema0;
                  Op ->
                      Schema0:set_default_op(Op)
              end,
    case SQuery#squery.default_field of
        undefined ->
            Schema1;
        Field ->
            Schema1:set_default_field(to_binary(Field))
    end.
