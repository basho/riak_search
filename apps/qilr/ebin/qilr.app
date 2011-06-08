% -*- mode: erlang -*-
{application, qilr,
 [{description,  "Full text query parser and planner"},
  {vsn,          "0.14.0"},
  {modules,      [qilr,
                  qilr_analyzer,
                  text_analyzers]},
  {registered,   []},
  {applications, [kernel, stdlib, sasl]},
  {env, []},
  {mod, {qilr_app, []}}]}.
