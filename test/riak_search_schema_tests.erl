%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(riak_search_schema_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_DISABLED_JOB_CLASSES, [
    {riak_search, query}
]).
-define(DEFAULT_ENABLED_JOB_CLASSES, [
]).
-define(JOB_CLASS_CONFIG_KEY, "riak_core.job_accept_class").

job_class_defaults_test() ->
    case riak_core_schema() of
        {true, RCSchema} when erlang:is_list(RCSchema) ->
            confirm_enabled_job_classes(RCSchema);
        Ret ->
            ?assertEqual({error, enoent}, Ret),
            ?debugMsg("Supporting riak_core components not present,"
                " skipping job_class_defaults test")
    end.

confirm_enabled_job_classes(RCSchema) ->
    Config = cuttlefish_unit:generate_templated_config(
        [RCSchema, "../priv/riak_search.schema"], [],
        riak_core_schema_tests:context() ++ context()),

    cuttlefish_unit:assert_config(
        Config, ?JOB_CLASS_CONFIG_KEY,
        lists:sort(?DEFAULT_ENABLED_JOB_CLASSES)),
    ok.

%% Mustache substitutions - return a list of values for any {{variable}} that
%% would normally be handled by rebar.
context() ->
    % the riak_search.schema file doesn't currently contain any
    [].

%% Ensure that the riak_core_schema_tests module is loaded and return the
%% path of the riak_core.schema file.
riak_core_schema() ->
    riak_core_schema(riak_core_dir()).
riak_core_schema({RCDir, Schema}) when erlang:is_list(RCDir) ->
    case code:ensure_loaded(riak_core_schema_tests) of
        {module, _} ->
            {true, Schema};
        _ ->
            Search = filename:join([RCDir, "**", "riak_core_schema_tests.beam"]),
            case filelib:wildcard(Search) of
                [Beam | _] ->
                    case code:load_abs(filename:rootname(Beam)) of
                        {module, _} ->
                            {true, Schema};
                        Error ->
                            Error
                    end;
                [] ->
                    {error, enoent}
            end
    end;
riak_core_schema(Error) ->
    Error.

riak_core_dir() ->
    % assume project base directory is ".."
    TryDeps = case os:getenv("REBAR_DEPS_DIR") of
        false ->
            ["../deps", "../.."];
        Dir ->
            [Dir, "../deps"]
    end,
    riak_core_dir(TryDeps).
riak_core_dir([Deps | TryDeps]) ->
    RCDir   = filename:join(Deps, "riak_core"),
    Schema  = filename:join([RCDir, "priv", "riak_core.schema"]),
    case filelib:is_regular(Schema) of
        true ->
            {RCDir, Schema};
        _ ->
            riak_core_dir(TryDeps)
    end;
riak_core_dir([]) ->
    {error, enoent}.
