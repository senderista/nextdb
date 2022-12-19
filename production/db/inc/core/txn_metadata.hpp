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

// The minimum value for the upper bound on unused pages that must exist
// before we attempt to decommit unused pages in the txn metadata map.
constexpr size_t c_min_pages_to_free = 64;

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

    inline void set_active_txn_submitted(gaia_txn_id_t begin_ts, gaia_txn_id_t commit_ts);
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

    gaia_txn_id_t register_begin_ts();
    gaia_txn_id_t register_commit_ts(gaia_txn_id_t begin_ts, db::log_offset_t log_offset);

    void dump_txn_metadata_at_ts(gaia_txn_id_t ts);

private:
    inline txn_metadata_entry_t get_entry(gaia_txn_id_t ts, bool relaxed_load = false);
    inline void set_entry(gaia_txn_id_t ts, txn_metadata_entry_t entry, bool relaxed_store = false);

    // This wrapper over std::atomic::compare_exchange_strong() returns the
    // actual value of this txn_metadata_t instance when the method was called.
    // If the returned value is not equal to the expected value, then the CAS
    // must have failed, otherwise it succeeded (compare_exchange_strong()
    // cannot fail spuriously).
    inline txn_metadata_entry_t compare_exchange(gaia_txn_id_t ts,
        txn_metadata_entry_t expected_value, txn_metadata_entry_t desired_value);

private:
    // This is an effectively infinite array of timestamp entries, indexed by
    // the txn timestamp counter and containing metadata for every txn that has
    // been submitted to the system.
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
    // The array's memory is managed via mmap(MAP_NORESERVE). We reserve 32TB of
    // virtual address space (1/8 of the total virtual address space available
    // to the process), but allocate physical pages only on first access. When a
    // range of timestamp entries falls behind the watermark, its physical pages
    // can be decommitted via madvise(MADV_DONTNEED).
    //
    // REVIEW: Because we reserve 2^45 bytes of virtual address space and each
    // array entry is 8 bytes, we can address the whole range using 2^42
    // timestamps. If we allocate 2^10 timestamps/second, we will use up all our
    // timestamps in 2^32 seconds, or about 2^7 years. If we allocate 2^20
    // timestamps/second, we will use up all our timestamps in 2^22 seconds, or
    // about a month and a half. If this is an issue, then we could treat the
    // array as a circular buffer, using a separate wraparound counter to
    // calculate the array offset from a timestamp, and we can use the 3
    // reserved bits in the txn metadata to extend our range by a factor of 8,
    // so we could allocate 2^20 timestamps/second for a full year. If we need a
    // still larger timestamp range (say 64-bit timestamps, with wraparound), we
    // could just store the difference between a commit timestamp and its txn's
    // begin timestamp, which should be possible to bound to no more than half
    // the bits we use for the full timestamp, so we would still need only 32
    // bits for a timestamp reference in the timestamp metadata. (We could store
    // the array offset instead, but that would be dangerous when we approach
    // wraparound.)
    //
    // FIXME: To prevent test failures when multiple sessions are opened in the
    // same process, we restrict timestamps to a much smaller range (2^35) than
    // would be suitable for production. This will be fixed when we transition
    // the txn metadata array to a ring buffer.
    std::atomic<uint64_t> m_txn_metadata_map[txn_metadata_entry_t::get_max_ts_count() / c_session_limit];
};

#include "txn_metadata.inc"

} // namespace transactions
} // namespace db
} // namespace gaia
