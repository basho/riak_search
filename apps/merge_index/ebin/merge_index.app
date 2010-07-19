{application, merge_index,
    [{description, "Merge Index"},
        {vsn, "0.12.0rc4"},
        {modules, [
            merge_index,
            mi_buffer,
            mi_incdex,
            mi_segment,
            mi_server,
            mi_utils,
            mi_locks,
            mi_scheduler,
            sync,
            test,
            basho_bench_driver_merge_index
        ]},
        {applications, [kernel,
            stdlib,
            sasl]},
        {env, []}
]}.
