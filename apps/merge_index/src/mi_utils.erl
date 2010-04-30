%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at

%%   http://www.apache.org/licenses/LICENSE-2.0

%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(mi_utils).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
    ift_pack/5,
    ift_unpack/1,
    fold/3,
    read_value/1,
    write_value/2,
    file_exists/1,
    create_empty_file/1,
    now_to_timestamp/1,
    ets_next/2
]).

ift_pack(IndexID, FieldID, TermID, SubType, SubTerm) ->
    <<
        IndexID:24/integer, 
        FieldID:24/integer,
        TermID:32/integer,
        SubType:16/integer,
        SubTerm:64/integer
        >>.

ift_unpack(IFT) ->
    <<
        IndexID:24/integer, 
        FieldID:24/integer,
        TermID:32/integer,
        SubType:16/integer,
        SubTerm:64/integer
        >> = IFT,
    {IndexID, FieldID, TermID, SubType, SubTerm}.

fold(F, Acc, Resource) ->
    case F(Resource, Acc) of
        {ok, NewResource, NewAcc} -> fold(F, NewAcc, NewResource);
        {eof, NewAcc} -> {ok, NewAcc}
    end.

read_value(FH) ->
    case file:read(FH, 2) of
        {ok, <<Size:16/integer>>} ->
            {ok, B} = file:read(FH, Size),
            {ok, binary_to_term(B)};
        eof ->
            eof
    end.

write_value(FH, Term) when not is_binary(Term) ->
    B = term_to_binary(Term),
    write_value(FH, B);
write_value(FH, B) ->
    Size = size(B),
    ok = file:write(FH, <<Size:16/integer, B/binary>>),
    Size + 2.

file_exists(Filename) ->
    filelib:is_file(Filename).

create_empty_file(Filename) ->
    file:write_file(Filename, <<"">>).

now_to_timestamp({Mega, Sec, Micro}) ->
    <<TS:64/integer>> = <<Mega:16/integer, Sec:24/integer, Micro:24/integer>>,
    TS.

%% Return the next key greater than or equal to the supplied key.
ets_next(Table, Key) ->
    case Key == undefined of 
        true ->
             ets:first(Table);
        false ->
            case ets:lookup(Table, Key) of
                [{Key, _Values}] -> 
                    Key;
                [] ->
                    ets:next(Table, Key)
            end
    end.
