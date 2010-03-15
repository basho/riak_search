% -*- mode: erlang -*-
{application, qilr,
 [{description,  "Full text query parser and planner"},
  {vsn,          "0.1"},
  {modules,      [qilr_scan, qilr_parse]},
  {registered,   []},
  {applications, [kernel, stdlib, sasl]}]}.
