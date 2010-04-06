% -*- mode: erlang -*-
{application, qilr,
 [{description,  "Full text query parser and planner"},
  {vsn,          "0.1"},
  {modules,      [qilr_scan, qilr_parse, qilr_repl, qilr_optimizer]},
%%   {modules,      []},
  {registered,   []},
  {applications, [kernel, stdlib, sasl]}]}.
