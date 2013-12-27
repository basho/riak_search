%% -------------------------------------------------------------------
%%
%% riak_search_pb_query: PB Service for Riak Search queries
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Implements a `riak_api_pb_service' for performing search
%% queries in Riak Search.
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

-import(riak_search_utils, [to_atom/1, to_binary/1, to_float/1]).
-import(riak_pb_search_codec, [encode_search_doc/1]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start state.
-spec init() -> any().
init() ->
    {ok, C} = riak_search:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(Msg, #state{client=Client}=State) ->
    case riak_core_security:is_enabled() of
        true ->
            %% we don't support link walking when security is
            %% enabled, return an error of some kind
            {error, "Riak Search 1.0 is"
                     " deprecated in Riak 2.0 and is"
                     " not compatible with security.", State};
        _ ->
            #rpbsearchqueryreq{index=Index, sort=Sort0,
                            fl=FL0, presort=Presort0}=Msg,
            {ok, Schema0} = riak_search_config:get_schema(Index),
            case parse_squery(Msg) of
                {ok, SQuery} ->
                    %% Construct schema, query, and filter
                    Schema = riak_search_utils:replace_schema_defaults(SQuery,
                                                                    Schema0),
                    {ok, QueryOps} = Client:parse_query(Schema,
                                                        SQuery#squery.q),
                    {ok, FilterOps} = Client:parse_filter(Schema,
                                                          SQuery#squery.filter),
                    %% Validate
                    UK = Schema:unique_key(),
                    FL = parse_fl(default(FL0, [<<"*">>])),
                    Sort = default(Sort0, <<"none">>),
                    Presort = to_atom(default(Presort0, <<"score">>)),
                    if
                        FL == [UK] andalso Sort /= <<"none">> ->
                            {error, riak_search_utils:err_msg(
                                    {error, fl_id_with_sort, UK}), State};
                        true ->
                            %% Execute
                            Result = run_query(Client, Schema, SQuery,
                                               QueryOps, FilterOps, Presort, FL),
                            {reply, encode_results(Result, UK, FL), State}
                    end;
                {error, missing_query} ->
                    {error, "Missing query", State}
            end
    end.

%% @doc process_stream/3 callback. Ignored.
process_stream(_,_,State) ->
    {ignore, State}.

%% ---------------------------------
%% Internal functions
%% ---------------------------------
run_query(Client, Schema, SQuery, QueryOps, FilterOps, Presort, FL) ->
    {_Time, NumFound, MaxScore, DocsOrIDs} =
        riak_search_utils:run_query(Client, Schema, SQuery, QueryOps,
                                    FilterOps, Presort, FL),
    {NumFound, MaxScore, DocsOrIDs}.

encode_results({NumFound, MaxScore, {ids, IDs}}, UK, _FL) ->
    #rpbsearchqueryresp{
                docs = [ encode_search_doc([{UK, ID}]) || ID <- IDs ],
                max_score = to_float(MaxScore),
                num_found = NumFound
                };

encode_results({NumFound, MaxScore, {docs, Docs}}, UK, FL) ->
    #rpbsearchqueryresp{
                docs = [ begin
                             Pairs = riak_indexed_doc:to_pairs(UK, Doc, FL),
                             encode_search_doc(Pairs)
                         end || Doc <- Docs ],
                max_score = to_float(MaxScore),
                num_found = NumFound
               }.

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

parse_fl([]) -> all;
parse_fl([<<"*">>]) -> all;
parse_fl(FL) -> FL.

default(undefined, Default) ->
    Default;
default(Value, _) ->
    Value.
