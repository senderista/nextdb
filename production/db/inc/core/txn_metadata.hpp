////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <cstdint>

#include <atomic>
#include <limits>
#include <optional>

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/backoff.hpp"
#include "gaia_internal/db/db.hpp"
#include "gaia_internal/db/db_types.hpp"

#include "db_internal_types.hpp"
#include "txn_metadata_entry.hpp"
#include "watermarks.hpp"

namespace gaia
{
namespace db
{
namespace transactions
{

// This class encapsulates the txn metadata array. It handles all reads, writes,
// and synchronization on the metadata array, but has no knowledge of the
// metadata format; that is the responsibility of the txn_metadata_entry_t
// class.
class txn_metadata_t
{
public:
    inline bool is_uninitialized_ts(gaia_txn_id_t ts, bool relaxed_load = false);
    inline bool is_sealed_ts(gaia_txn_id_t ts);
    inline bool is_begin_ts(gaia_txn_id_t ts);
    inline bool is_commit_ts(gaia_txn_id_t ts);
    inline bool is_txn_submitted(gaia_txn_id_t begin_ts);
    inline bool is_txn_validating(gaia_txn_id_t commit_ts);
    inline bool is_txn_decided(gaia_txn_id_t commit_ts);
    inline bool is_txn_committed(gaia_txn_id_t commit_ts);
    inline bool is_txn_aborted(gaia_txn_id_t commit_ts);
    inline bool is_txn_gc_complete(gaia_txn_id_t commit_ts);
    inline bool is_txn_durable(gaia_txn_id_t commit_ts);
    inline bool is_txn_active(gaia_txn_id_t begin_ts);
    inline bool is_txn_terminated(gaia_txn_id_t begin_ts);

    inline gaia_txn_id_t get_begin_ts_from_commit_ts(gaia_txn_id_t commit_ts);
    inline gaia_txn_id_t get_commit_ts_from_begin_ts(gaia_txn_id_t begin_ts);
    inline db::log_offset_t get_txn_log_offset_from_ts(gaia_txn_id_t commit_ts);

    inline void set_active_txn_submitted(gaia_txn_id_t begin_ts);
    inline void set_submitted_txn_commit_ts(gaia_txn_id_t begin_ts, gaia_txn_id_t commit_ts);
    inline void set_active_txn_terminated(gaia_txn_id_t begin_ts);
    inline void update_txn_decision(gaia_txn_id_t commit_ts, bool has_committed);
    inline void set_txn_durable(gaia_txn_id_t commit_ts);
    inline bool set_txn_gc_complete(gaia_txn_id_t commit_ts);

    // This is designed for implementing "fences" that can guarantee no thread can
    // ever claim a timestamp, by marking that timestamp permanently sealed. Sealing
    // can only be performed on an "uninitialized" metadata entry, not on any valid
    // metadata entry. When a session thread beginning or committing a txn finds
    // that its begin_ts or commit_ts has been sealed upon initializing the metadata
    // entry for that timestamp, it simply allocates another timestamp and retries.
    // This is possible because we never publish a newly allocated timestamp until
    // we know that its metadata entry has been successfully initialized.
    inline bool seal_uninitialized_ts(gaia_txn_id_t ts);

    // Returns the txn metadata entry at the given timestamp.
    inline txn_metadata_entry_t get_entry(gaia_txn_id_t ts, bool relaxed_load = false);

    // Sets the given txn metadata entry at the given timestamp.
    inline void set_entry(gaia_txn_id_t ts, txn_metadata_entry_t entry, bool relaxed_store = false);

    // This wrapper over std::atomic::compare_exchange_strong() returns the
    // actual value of this txn_metadata_t instance when the method was called.
    // If the returned value is not equal to the expected value, then the CAS
    // must have failed, otherwise it succeeded (compare_exchange_strong()
    // cannot fail spuriously).
    inline txn_metadata_entry_t compare_exchange(gaia_txn_id_t ts,
        txn_metadata_entry_t expected_value, txn_metadata_entry_t desired_value);

    // Spins up to a timeout (on the order of context switch latency), testing
    // the commit_ts entry at the given timestamp for TXN_DECIDED status.
    //
    // Returns true if the given commit_ts entry was TXN_DECIDED before timeout,
    // false otherwise.
    inline bool poll_for_decision(gaia_txn_id_t ts);

    // This uninitializes all entries from start_ts (inclusive) to end_ts (exclusive).
    inline void uninitialize_ts_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts);

    inline void dump_txn_metadata_at_ts(gaia_txn_id_t ts);

public:
    // REVIEW: The smallest reasonable size is double the maximum number of logs
    // (because update txns need both a begin_ts entry and a commit_ts entry).
    static constexpr size_t c_num_entries{2 * (c_max_logs + 1)};

private:
    inline size_t ts_to_buffer_index(gaia_txn_id_t ts);

private:
    // We need to alias the pre-reclaim watermark for both efficiency and
    // correctness (a simple duplicate counter that was written after the
    // watermark was updated could easily run backward under concurrency).
    //
    // We cache a pointer to this watermark instead of calling the
    // get_watermarks() accessor, to avoid taking a dependency on client/server
    // accessors for the shared-memory structures.
    //
    // This is an effectively infinite array of timestamp entries, implemented
    // as a finite circular buffer, logically indexed by the txn timestamp
    // counter and containing metadata for every txn that has been submitted to
    // the system. When a prefix of timestamp entries falls behind the
    // pre-reclaim watermark, it can be overwritten with new entries.
    //
    // Entries may be "uninitialized", "sealed" (i.e., initialized with a
    // special "junk" value and forbidden to be used afterward), or initialized
    // with txn metadata, consisting of 3 status bits, 1 bit for GC status
    // (unknown or complete), 1 bit for persistence status (unknown or
    // complete), 1 bit reserved for future use, 16 bits for a txn log offset,
    // and 42 bits for a linked timestamp (i.e., the commit timestamp of a
    // submitted txn embedded in its begin timestamp metadata, or the begin
    // timestamp of a submitted txn embedded in its commit timestamp metadata).
    // The 3 status bits use the high bit to distinguish begin timestamps from
    // commit timestamps, and 2 bits to store the state of an active,
    // terminated, or submitted txn.
    //
    // The array is always accessed without any locking, but its entries have
    // read and write barriers (via std::atomic) that ensure causal consistency
    // between any threads that read or write the same txn metadata. Any writes
    // to entries that may be written by multiple threads use CAS operations.
    //
    // REVIEW: We currently limit timestamps to 42 bits, but could extend them
    // if necessary up to 64 bits, if we replace linked timestamps in the
    // metadata word with relative offsets. If the circular buffer cannot exceed
    // 2^16 entries, the offsets could be restricted to 16 bits.

    std::atomic<uint64_t> m_txn_metadata_map[c_num_entries];

    // For buffer indexing to work correctly (and efficiently) we need the
    // buffer size to be a power of 2.
    static_assert(
        (c_num_entries & -c_num_entries) == c_num_entries,
        "The txn metadata map buffer size must be a power of 2!");
};

#include "txn_metadata.inc"

} // namespace transactions
} // namespace db
} // namespace gaia
