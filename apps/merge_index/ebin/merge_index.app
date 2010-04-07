{application, merge_index,
    [{description, "Merge Index"},
        {vsn, "0.1"},
        {modules, [
            merge_index,
            mi_buffer,
            mi_incdex,
            mi_segment,
            mi_server,
            mi_utils,
            sync,
            test
        ]},
        {applications, [kernel,
            stdlib,
            sasl]},
        {env, []}
]}.
