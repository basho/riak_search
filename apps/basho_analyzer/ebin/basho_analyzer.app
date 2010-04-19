% -*- mode: erlang -*-
{application, basho_analyzer,
 [{description,  "Interface to analysis_master Java service for textual analysis"},
  {vsn,          "0.1"},
  {modules,      [analysis_pb,
                  basho_analyzer_app,
                  basho_analyzer_sup,
                  basho_analyzer]},
  {registered,   [basho_analyzer_sup,
                  basho_analyzer]},
  {applications, [kernel, stdlib, sasl]},
  {env, [{analysis_port, 6098}]},
  {mod, {basho_analyzer_app, []}}]}.
