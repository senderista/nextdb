////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <unistd.h>

#include "gaia/db/db.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/debug_assert.hpp"
#include "gaia_internal/db/db.hpp"
#include "gaia_internal/db/db_object.hpp"
#include "gaia_internal/db/db_types.hpp"
#include "gaia_internal/exceptions.hpp"

#include "chunk_manager.hpp"
#include "db_hash_map.hpp"
#include "db_internal_types.hpp"
#include "db_shared_data.hpp"
#include "memory_manager.hpp"
#include "memory_types.hpp"
#include "txn_metadata.hpp"

namespace gaia
{
namespace db
{

inline void dump_system_stats()
{
    std::cerr << "Used logs: " << gaia::db::get_logs()->get_used_logs_count() << std::endl;
    std::cerr << "All used logs: " << gaia::db::get_logs()->get_all_used_logs_count() << std::endl;
    std::cerr << "Used chunks: " << gaia::db::get_memory_manager()->get_used_chunks_count() << std::endl;
    std::cerr << "All used chunks: " << gaia::db::get_memory_manager()->get_all_used_chunks_count() << std::endl;
}

inline common::gaia_id_t allocate_id()
{
    counters_t* counters = gaia::db::get_counters();
    auto last_id = (counters->last_id)++;

    ASSERT_INVARIANT(
        last_id < std::numeric_limits<common::gaia_id_t::value_type>::max(),
        "Gaia ID counter overflowed!");

    return static_cast<common::gaia_id_t::value_type>(last_id + 1);
}

inline gaia_txn_id_t allocate_txn_id()
{
    counters_t* counters = gaia::db::get_counters();
    auto last_txn_id = (counters->last_txn_id)++;
    ASSERT_INVARIANT(
        last_txn_id < std::numeric_limits<gaia_txn_id_t::value_type>::max(),
        "txn ID counter overflowed!");

    auto new_txn_id = last_txn_id + 1;
    ASSERT_INVARIANT(
        new_txn_id <= gaia::db::get_txn_metadata()->get_last_safe_unallocated_ts(),
        "Transaction timestamp entry must be allocated in unused memory!");

    // REVIEW: enable these exceptions once we handle them properly?

    // if (new_txn_id > get_last_safe_unallocated_ts())
    // {
    //     throw transaction_metadata_allocation_failure_internal();
    // }

#ifdef DUMP_STATS
    if (new_txn_id % c_dump_stats_timestamp_interval == 0)
    {
        dump_system_stats();
    }
#endif

    // A timestamp entry could be concurrently sealed between allocation of the
    // timestamp and reading the entry.
    ASSERT_INVARIANT(
        gaia::db::get_txn_metadata()->is_uninitialized_ts(new_txn_id) ||
            gaia::db::get_txn_metadata()->is_sealed_ts(new_txn_id),
        "Any newly allocated timestamp entry must be uninitialized or sealed!");

    return static_cast<gaia_txn_id_t::value_type>(new_txn_id);
}

inline gaia_locator_t allocate_locator(common::gaia_type_t type)
{
    counters_t* counters = gaia::db::get_counters();
    auto last_locator = (counters->last_locator)++;
    if (last_locator >= c_max_locators)
    {
        throw system_object_limit_exceeded_internal();
    }

    auto new_locator = static_cast<gaia_locator_t::value_type>(last_locator + 1);

    type_index_t* type_index = get_type_index();
    type_index->add_locator(type, new_locator);

    return new_locator;
}

// REVIEW: Would a relaxed store option be useful for optimizations?
inline void update_locator(gaia_locator_t locator, gaia_offset_t offset, locators_t* locators = nullptr)
{
    if (!locators)
    {
        locators = gaia::db::get_locators();
    }
    (*locators)[locator].store(offset);
}

inline gaia_locator_t get_last_locator()
{
    counters_t* counters = gaia::db::get_counters();
    auto last_locator_value = counters->last_locator.load();

    DEBUG_ASSERT_INVARIANT(
        last_locator_value <= c_max_locators,
        "Largest locator value exceeds allowed range!");

    return static_cast<gaia_locator_t::value_type>(last_locator_value);
}

// Returns true if ID was not already registered, false otherwise.
inline bool register_locator_for_id(
    common::gaia_id_t id, gaia_locator_t locator)
{
    return gaia::db::db_hash_map::insert(id, locator);
}

inline gaia_locator_t id_to_locator(common::gaia_id_t id)
{
    return id.is_valid() ? gaia::db::db_hash_map::find(id) : c_invalid_gaia_locator;
}

// REVIEW: Would a relaxed load option be useful for optimizations?
inline gaia_offset_t locator_to_offset(gaia_locator_t locator)
{
    if (!locator.is_valid())
    {
        return c_invalid_gaia_offset;
    }

    locators_t* locators = gaia::db::get_locators();
    return (*locators)[locator].load();
}

inline db_object_t* offset_to_ptr(gaia_offset_t offset)
{
    data_t* data = gaia::db::get_data();
    return (offset.is_valid())
        ? reinterpret_cast<db_object_t*>(&data->objects[offset])
        : nullptr;
}

inline db_object_t* locator_to_ptr(gaia_locator_t locator)
{
    return offset_to_ptr(locator_to_offset(locator));
}

inline bool locator_exists(gaia_locator_t locator)
{
    return (locator.is_valid())
        && (locator <= get_last_locator())
        && locator_to_offset(locator).is_valid();
}

inline db_object_t* id_to_ptr(common::gaia_id_t id)
{
    gaia_locator_t locator = id_to_locator(id);
    DEBUG_ASSERT_INVARIANT(
        locator_exists(locator),
        "An invalid locator was returned by id_to_locator()!");
    return locator_to_ptr(locator);
}

// This is only meant for "fuzzy snapshots" of the current last_txn_id; there
// are no memory barriers.
inline gaia_txn_id_t get_last_txn_id()
{
    counters_t* counters = gaia::db::get_counters();
    // A relaxed load is sufficient because stale values are acceptable.
    return static_cast<gaia_txn_id_t::value_type>(counters->last_txn_id.load(std::memory_order_relaxed));
}

inline void apply_log_to_locators(locators_t* locators, txn_log_t* txn_log,
    bool apply_new_versions = true, size_t starting_log_record_index = 0)
{
    for (size_t i = starting_log_record_index; i < txn_log->record_count; ++i)
    {
        auto log_record = &(txn_log->log_records[i]);
        auto offset_to_apply = apply_new_versions ? log_record->new_offset : log_record->old_offset;
        update_locator(log_record->locator, offset_to_apply, locators);
    }
}

inline void apply_log_from_offset(locators_t* locators, log_offset_t log_offset,
    bool apply_new_versions = true, size_t starting_log_record_index = 0)
{
    txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);
    apply_log_to_locators(locators, txn_log, apply_new_versions, starting_log_record_index);
}

// This method exists purely to isolate the chunk allocation slow path from
// allocate_object(), so that it can be more easily inlined.
inline void allocate_new_chunk(
    memory_manager::memory_manager_t* memory_manager,
    memory_manager::chunk_manager_t* chunk_manager)
{
    if (chunk_manager->initialized())
    {
        // The current chunk is out of memory, so retire it and allocate a new chunk.
        // In case it is already empty, try to deallocate it after retiring it.

        // Get the session's chunk version for safe deallocation.
        chunk_version_t version = chunk_manager->get_version();
        // Now retire the chunk.
        chunk_manager->retire_chunk(version);
        // Release ownership of the chunk.
        chunk_manager->release();
    }

    // Allocate a new chunk.
    chunk_offset_t new_chunk_offset = memory_manager->allocate_chunk();
    if (!new_chunk_offset.is_valid())
    {
        throw memory_allocation_error_internal();
    }

    // Initialize the new chunk.
    chunk_manager->initialize(new_chunk_offset);
}

// Allocate an object from the "data" shared memory segment.
// The `size` argument *does not* include the object header size!
inline void allocate_object(
    gaia_locator_t locator,
    size_t size)
{
    memory_manager::memory_manager_t* memory_manager = gaia::db::get_memory_manager();
    memory_manager::chunk_manager_t* chunk_manager = gaia::db::get_chunk_manager();

    // The allocation can fail either because there is no current chunk, or
    // because the current chunk is full.
    gaia_offset_t object_offset = chunk_manager->allocate(size + c_db_object_header_size);
    if (!object_offset.is_valid())
    {
        // Initialize the chunk manager with a new chunk.
        allocate_new_chunk(memory_manager, chunk_manager);

        // Allocate from the new chunk.
        object_offset = chunk_manager->allocate(size + c_db_object_header_size);
    }

    ASSERT_POSTCONDITION(
        object_offset.is_valid(),
        "Allocation from chunk was not expected to fail!");

    // Update locator array to point to the new offset.
    update_locator(locator, object_offset);
}

inline bool acquire_txn_log_reference(log_offset_t log_offset, gaia_txn_id_t begin_ts)
{
    txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);
    return txn_log->acquire_reference(begin_ts);
}

inline void release_txn_log_reference(log_offset_t log_offset, gaia_txn_id_t begin_ts)
{
    txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);
    txn_log->release_reference(begin_ts);
}

// This helper allocates a new begin_ts and initializes its metadata in the txn
// table.
inline gaia_txn_id_t register_begin_ts()
{
    // The newly allocated begin timestamp for the new txn.
    gaia_txn_id_t begin_ts;

    // Loop until we successfully install a newly allocated begin_ts in the txn
    // table. (We're possibly racing another beginning or committing txn that
    // could seal our begin_ts metadata entry before we install it.)
    // Technically, there is no bound on the number of iterations until success,
    // so this is not wait-free, but in practice conflicts should be very rare.
    while (true)
    {
        // Allocate a new begin timestamp.
        begin_ts = allocate_txn_id();

        // The txn metadata must be uninitialized (not sealed).
        txn_metadata_entry_t expected_value{
            txn_metadata_entry_t::uninitialized_value()};
        txn_metadata_entry_t desired_value{
            txn_metadata_entry_t::new_begin_ts_entry()};
        txn_metadata_entry_t actual_value{
            get_txn_metadata()->compare_exchange(begin_ts, expected_value, desired_value)};

        if (actual_value == expected_value)
        {
            break;
        }

        // The CAS can only fail if it returns the "sealed" value.
        ASSERT_INVARIANT(
            actual_value == txn_metadata_entry_t::sealed_value(),
            "A newly allocated timestamp cannot be concurrently initialized to any value except the sealed value!");
    }

    return begin_ts;
}

// This helper allocates a new commit_ts and initializes its metadata in the txn
// table.
inline gaia_txn_id_t register_commit_ts(gaia_txn_id_t begin_ts, db::log_offset_t log_offset)
{
    ASSERT_PRECONDITION(
        !get_txn_metadata()->is_uninitialized_ts(begin_ts),
        c_message_uninitialized_timestamp);

    // The newly allocated commit timestamp for the submitted txn.
    gaia_txn_id_t commit_ts;

    // Loop until we successfully install a newly allocated commit_ts in the txn
    // table. (We're possibly racing another beginning or committing txn that
    // could seal our commit_ts metadata entry before we install it.)
    // Technically, there is no bound on the number of iterations until success,
    // so this is not wait-free, but in practice conflicts should be very rare.
    while (true)
    {
        // Allocate a new commit timestamp.
        commit_ts = allocate_txn_id();

        // The txn metadata must be uninitialized (not sealed).
        txn_metadata_entry_t expected_value{
            txn_metadata_entry_t::uninitialized_value()};
        txn_metadata_entry_t desired_value{
            txn_metadata_entry_t::new_commit_ts_entry(commit_ts, begin_ts, log_offset)};
        txn_metadata_entry_t actual_value{
            get_txn_metadata()->compare_exchange(commit_ts, expected_value, desired_value)};

        if (actual_value == expected_value)
        {
            break;
        }

        // The CAS can only fail if it returns the "sealed" value.
        ASSERT_INVARIANT(
            actual_value == txn_metadata_entry_t::sealed_value(),
            "A newly allocated timestamp cannot be concurrently initialized to any value except the sealed value!");
    }

    return commit_ts;
}

} // namespace db
} // namespace gaia
