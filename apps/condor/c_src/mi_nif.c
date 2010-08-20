// -------------------------------------------------------------------
//
// mi: Merge-Index Data Store
//
// Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
//
// -------------------------------------------------------------------
#include "erl_nif.h"

#include <stdint.h>
#include <string.h>

static ErlNifResourceType* mi_nif_segidx_RESOURCE;

typedef struct
{
    uint64_t ift;
    uint64_t offset;
    uint32_t count;
} __attribute__ ((__packed__)) segidx_entry;

typedef struct
{
    uint32_t count;
    segidx_entry entries[0];
} mi_nif_segidx_handle;

typedef struct
{
    uint32_t total_bytes;
    ErlNifMutex* lock;
} mi_nif_segidx_global;

// Prototypes
ERL_NIF_TERM mi_nif_segidx_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM mi_nif_segidx_lookup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM mi_nif_segidx_lookup_nearest(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM mi_nif_segidx_entry_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM mi_nif_segidx_ift_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM mi_nif_segidx_ift_range_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM mi_nif_segidx_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
{
    {"segidx_new", 1, mi_nif_segidx_new},
    {"segidx_lookup_bin", 2, mi_nif_segidx_lookup},
    {"segidx_lookup_nearest_bin", 2, mi_nif_segidx_lookup_nearest},
    {"segidx_entry_count", 1, mi_nif_segidx_entry_count},
    {"segidx_ift_count_bin", 2, mi_nif_segidx_ift_count},
    {"segidx_ift_count_bin", 3, mi_nif_segidx_ift_range_count},
    {"segidx_info", 0, mi_nif_segidx_info}
};

ERL_NIF_TERM mi_nif_segidx_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary index_bin;
    if (enif_inspect_binary(env, argv[0], &index_bin))
    {
        // Allocate the handle and enough room for the contents of
        // the binary (blech). Would be nice if the NIF API allowed us
        // to avoid copying this.
        mi_nif_segidx_handle* handle = enif_alloc_resource(env,
                                                           mi_nif_segidx_RESOURCE,
                                                           sizeof(mi_nif_segidx_handle) + index_bin.size);

        // Copy the contents of binary into our array and save the
        // number of entries
        memcpy(handle->entries, index_bin.data, index_bin.size);
        handle->count = index_bin.size / sizeof(segidx_entry);

        // Increment total_bytes on the global structure
        mi_nif_segidx_global* global = (mi_nif_segidx_global*)enif_priv_data(env);
        enif_mutex_lock(global->lock);
        global->total_bytes += sizeof(mi_nif_segidx_handle) + index_bin.size;
        enif_mutex_unlock(global->lock);

        // Hand off the resource back to the VM
        ERL_NIF_TERM result = enif_make_resource(env, handle);
        enif_release_resource(env, handle);
        return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}


static int mi_nif_segidx_compare(const void* keyPtr, const void* entry_ptr)
{
    uint64_t key = *((uint64_t*)keyPtr);
    segidx_entry* entry = (segidx_entry*)entry_ptr;
    if (key > entry->ift)
    {
        return 1;
    }
    else if (key < entry->ift)
    {
        return -1;
    }
    return 0;
}


ERL_NIF_TERM mi_nif_segidx_lookup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    mi_nif_segidx_handle* handle;
    ErlNifBinary ift_bin;
    if (enif_get_resource(env, argv[0], mi_nif_segidx_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &ift_bin))
    {
        uint64_t* key = ((uint64_t*)(ift_bin.data));
        segidx_entry* entry = bsearch(key, handle->entries, handle->count, sizeof(segidx_entry),
                                      &mi_nif_segidx_compare);
        if (entry != 0)
        {
            // Return the offset/count as a binary so as to simplify encoding/decoding
            // of the 64-bit integer.
            ErlNifBinary result_bin;
            enif_alloc_binary(env, sizeof(segidx_entry), &result_bin);
            memcpy(result_bin.data, entry, sizeof(segidx_entry));
            return enif_make_tuple2(env,
                                    enif_make_atom(env, "ok"),
                                    enif_make_binary(env, &result_bin));
        }
        else
        {
            // Didn't find any matching entries
            return enif_make_atom(env, "not_found");
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM mi_nif_segidx_lookup_nearest(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    mi_nif_segidx_handle* handle;
    ErlNifBinary ift_bin;
    if (enif_get_resource(env, argv[0], mi_nif_segidx_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &ift_bin))
    {
        uint64_t start_ift = *((uint64_t*)(ift_bin.data));

        // Find the first IFT >= to start_ift. The naive implementation here is
        // to scan the array.
        // TODO: Write our own binary search that returns nearest?!
        int i = 0;
        for (; i < handle->count; i++)
        {
            if (handle->entries[i].ift >= start_ift)
            {
                // Found it; return the whole record as a binary
                ErlNifBinary result_bin;
                enif_alloc_binary(env, sizeof(segidx_entry), &result_bin);
                memcpy(result_bin.data, handle->entries + i, sizeof(segidx_entry));
                return enif_make_tuple2(env,
                                        enif_make_atom(env, "ok"),
                                        enif_make_binary(env, &result_bin));
            }
        }

        // Didn't find any matching entries
        return enif_make_atom(env, "not_found");
    }
    else
    {
        return enif_make_badarg(env);
    }
}



ERL_NIF_TERM mi_nif_segidx_entry_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    mi_nif_segidx_handle* handle;
    if (enif_get_resource(env, argv[0], mi_nif_segidx_RESOURCE, (void**)&handle))
    {
        return enif_make_ulong(env, handle->count);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM mi_nif_segidx_ift_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    mi_nif_segidx_handle* handle;
    ErlNifBinary ift_bin;
    if (enif_get_resource(env, argv[0], mi_nif_segidx_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &ift_bin))
    {
        uint64_t* key = ((uint64_t*)ift_bin.data);

        // Looking for a specific IFT. If it's not found, just return a count of 0
        segidx_entry* entry = bsearch(key, handle->entries, handle->count, sizeof(segidx_entry),
                                      &mi_nif_segidx_compare);
        if (entry != 0)
        {
            return enif_make_ulong(env, entry->count);
        }
        else
        {
            return enif_make_ulong(env, 0);
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}


ERL_NIF_TERM mi_nif_segidx_ift_range_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    mi_nif_segidx_handle* handle;
    ErlNifBinary start_ift_bin;
    ErlNifBinary end_ift_bin;
    if (enif_get_resource(env, argv[0], mi_nif_segidx_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &start_ift_bin) &&
        enif_inspect_binary(env, argv[2], &end_ift_bin))
    {
        uint64_t start_ift = *((uint64_t*)start_ift_bin.data);
        uint64_t end_ift   = *((uint64_t*)end_ift_bin.data);
        uint64_t count     = 0;

        // The bsearch function doesn't return the closest match to start_ift,
        // so we'll have to traverse the array manually and sum up all the
        // values in the specified range.
        int i = 0;
        for (; i < handle->count; i++)
        {
            if (handle->entries[i].ift >= start_ift && handle->entries[i].ift <= end_ift)
            {
                count += handle->entries[i].count;
            }
            else if (handle->entries[i].ift > end_ift)
            {
                // Past the end IFT, no point in continuing traversal
                break;
            }
        }

        // Return the count as a binary so as to simplify encoding/decoding
        // of the 64-bit integer.
        ErlNifBinary result_bin;
        enif_alloc_binary(env, sizeof(uint64_t), &result_bin);
        memcpy(result_bin.data, &count, sizeof(uint64_t));
        return enif_make_binary(env, &result_bin);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM mi_nif_segidx_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    mi_nif_segidx_global* global = (mi_nif_segidx_global*)enif_priv_data(env);
    enif_mutex_lock(global->lock);

    ERL_NIF_TERM result = enif_make_tuple2(env,
                                           enif_make_atom(env, "total_bytes"),
                                           enif_make_ulong(env, global->total_bytes));

    enif_mutex_unlock(global->lock);
    return result;
}

static void mi_nif_segidx_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in mi_nif_handle
    mi_nif_segidx_handle* handle = (mi_nif_segidx_handle*)arg;

    // Decrement total_bytes on the global structure
    mi_nif_segidx_global* global = (mi_nif_segidx_global*)enif_priv_data(env);
    enif_mutex_lock(global->lock);
    global->total_bytes -= sizeof(mi_nif_segidx_handle) + (handle->count * sizeof(segidx_entry));
    enif_mutex_unlock(global->lock);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    mi_nif_segidx_RESOURCE = enif_open_resource_type(env, "mi_nif_segidx_resource",
                                                     &mi_nif_segidx_cleanup,
                                                     ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                     0);

    // Setup a global pointer for tracking # of bytes in all segidxs
    mi_nif_segidx_global* global = enif_alloc(env, sizeof(mi_nif_segidx_global));
    memset(global, '\0', sizeof(mi_nif_segidx_global));
    global->lock = enif_mutex_create("mi_nif_segidx_global");
    *priv_data = global;

    return 0;
}

static void on_unload(ErlNifEnv* env, void* priv_data)
{
    mi_nif_segidx_global* global = (mi_nif_segidx_global*)priv_data;
    enif_mutex_destroy(global->lock);
    enif_free(env, priv_data);
}

ERL_NIF_INIT(mi_nif, nif_funcs, &on_load, NULL, NULL, &on_unload);
