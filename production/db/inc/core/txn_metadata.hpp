////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <cstring>

#include <atomic>
#include <limits>
#include <optional>

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/backoff.hpp"
#include "gaia_internal/db/db.hpp"
#include "gaia_internal/db/db_types.hpp"

#include "db_internal_types.hpp"
#include "txn_metadata_entry.hpp"

namespace gaia
{
namespace db
{
namespace transactions
{

// These global timestamp variables are "watermarks" that represent the progress
// of various system functions with respect to transaction history. The
// "pre-apply" watermark represents an upper bound on the latest commit_ts whose
// txn log could have been applied to the shared locator view. A committed txn
// cannot have its txn log applied to the global snapshot until the pre-apply
// watermark has been advanced to its commit_ts. The "post-apply" watermark
// represents a lower bound on the same quantity, and also an upper bound on the
// latest commit_ts whose txn log could be eligible for GC. GC cannot be started
// for any committed txn until the post-apply watermark has advanced to its
// commit_ts. The "post-GC" watermark represents a lower bound on the latest
// commit_ts whose txn log could have had GC reclaim all its resources. The
// "pre-reclaim" watermark represents an (exclusive) upper bound on the
// timestamps whose metadata entries could have had their memory reclaimed
// (e.g., via zeroing, unmapping, or overwriting). Any timestamp whose metadata
// entry could potentially be dereferenced must be "reserved" via the "safe_ts"
// API to prevent the pre-reclaim watermark from advancing past it and allowing
// its metadata entry to be reclaimed. Finally, the "post-reclaim" watermark
// represents an (exclusive) upper bound on the timestamps whose metadata
// entries have had their memory fully reclaimed, so they are eligible for
// reuse.
//
// The pre-apply watermark must either be equal to the post-apply watermark or
// greater by 1.
//
// Schematically:
// post-reclaim watermark
//    > timestamps whose metadata entries can be safely reused
// <= pre-reclaim watermark
//    > timestamps whose metadata entries cannot be safely accessed
// <= post-GC watermark
//    >= commit timestamps of completely garbage-collected transactions,
//    < commit timestamps of transactions applied to global snapshot but possibly not garbage-collected
// <= post-apply watermark
//    >= commit timestamps of transactions fully applied to global snapshot,
//    < commit timestamps of transactions partially applied to global snapshot
// <= pre-apply watermark
//    >= commit timestamps of transactions partially applied to global snapshot,
//    < commit timestamps of transactions not applied to global snapshot.

enum class watermark_type_t
{
    pre_apply,
    post_apply,
    post_gc,
    pre_reclaim,
    post_reclaim,
    // This should always be last.
    count
};

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

    // This is designed for implementing "fences" that can guarantee no thread
    // can ever claim a timestamp, by marking that timestamp permanently sealed.
    // Sealing can only be performed on an "uninitialized" metadata entry, not
    // on any valid metadata entry. When a session thread beginning or
    // committing a txn finds that its begin_ts or commit_ts has been sealed
    // upon initializing the metadata entry for that timestamp, it simply
    // allocates another timestamp and retries. This is possible because we
    // never publish a newly allocated timestamp until we know that its metadata
    // entry has been successfully initialized.
    //
    // The motivation is to ensure that a suspended thread (suspended between
    // when it allocates a new timestamp and when it initializes that
    // timestamp's entry) cannot prevent the progress of other threads. When
    // another thread encounters the still-uninitialized timestamp, it will
    // simply seal it and continue. When the suspended thread resumes and
    // attempts to initialize its now-sealed timestamp entry, initialization
    // will fail and it must retry with a new timestamp.
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

    // Uninitializes all entries from start_ts (inclusive) to end_ts (exclusive).
    // This function must be both concurrent and idempotent.
    inline void uninitialize_ts_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts);

    // For debugging use only.
    inline void dump_txn_metadata_at_ts(gaia_txn_id_t ts);

    // This returns the oldest allocated timestamp that can be safely accessed.
    inline gaia_txn_id_t get_first_safe_allocated_ts();

    // This returns the newest unallocated timestamp that can be allocated
    // without overwriting entries that are possibly in use.
    inline gaia_txn_id_t get_last_safe_unallocated_ts();

public:
    // REVIEW: The smallest reasonable size is double the maximum number of logs
    // (because update txns need both a begin_ts entry and a commit_ts entry).
    static constexpr size_t c_num_entries{2 * (c_max_logs + 1)};

private:
    inline size_t ts_to_buffer_index(gaia_txn_id_t ts);

private:
    // This is an effectively infinite (up to 2^64) array of timestamp entries,
    // implemented as a finite circular buffer, logically indexed by the txn
    // timestamp counter and containing metadata for every txn that has been
    // submitted to the system. When a prefix of timestamp entries falls behind
    // the post-reclaim watermark, its underlying memory can be reused for new
    // entries.
    //
    // Entries may be "uninitialized", "sealed" (i.e., initialized with a
    // special "junk" value and forbidden to be used afterward), or initialized
    // with txn metadata, consisting of 3 status bits, 1 bit for GC status
    // (unknown or complete), 1 bit for persistence status (unknown or
    // complete), 1 bit reserved for future use, 16 bits for a txn log offset,
    // and 16 bits for the offset from this timestamp to a linked timestamp
    // (i.e., the commit timestamp of a submitted txn embedded in its begin
    // timestamp metadata, or the begin timestamp of a submitted txn embedded in
    // its commit timestamp metadata). The 3 status bits use the high bit to
    // distinguish begin timestamps from commit timestamps, and 2 bits to store
    // the state of an active, terminated, or submitted txn.
    //
    // The array is always accessed without any locking, but its entries have
    // read and write barriers (via std::atomic) that ensure causal consistency
    // between any threads that read or write the same txn metadata. Any writes
    // to entries that may be written by multiple threads use CAS operations.

    std::atomic<uint64_t> m_txn_metadata_map[c_num_entries];

    // For buffer indexing to work correctly (and efficiently) we need the
    // buffer size to be a power of 2.
    static_assert(
        (c_num_entries & -c_num_entries) == c_num_entries,
        "The txn metadata map buffer size must be a power of 2!");

    // Watermark-related data structures and functions.

private:
    // An array of monotonically nondecreasing timestamps, or "watermarks", that
    // represent the progress of system maintenance tasks with respect to txn
    // history. See `watermark_type_t` for a full explanation.
    //
    // We pad each entry to 64 bytes (the width of a cache line) to prevent
    // memory contention from false sharing.
    struct
    {
        alignas(c_cache_line_size_in_bytes)
        std::atomic<gaia_txn_id_t::value_type> entry;
    }
    m_watermarks[common::get_enum_value(watermark_type_t::count)];

public:
    // Returns the current value of the given watermark.
    inline gaia_txn_id_t get_watermark(watermark_type_t watermark_type, bool relaxed_load = false);

    // Atomically advances the given watermark to the given timestamp, if the
    // given timestamp is larger than the watermark's current value. It thus
    // guarantees that the watermark is monotonically nondecreasing in time.
    //
    // Returns true if the watermark was advanced to the given value, false
    // otherwise.
    inline bool advance_watermark(watermark_type_t watermark_type, gaia_txn_id_t ts);

private:
    // Returns a reference to the array entry of the given watermark.
    inline std::atomic<gaia_txn_id_t::value_type>& get_watermark_entry(watermark_type_t watermark_type);
};

#include "txn_metadata.inc"

} // namespace transactions
} // namespace db
} // namespace gaia
