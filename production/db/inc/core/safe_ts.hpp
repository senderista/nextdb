////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include "gaia/common.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/debug_assert.hpp"
#include "gaia_internal/common/bitmap.hpp"
#include "gaia_internal/db/db.hpp"
#include "gaia_internal/db/db_types.hpp"
#include "gaia_internal/exceptions.hpp"

#include "db_internal_types.hpp"
#include "watermarks.hpp"

namespace gaia
{
namespace db
{

class safe_ts_entries_t
{
public:
    // Reserves an index into the safe_ts entries array for the current thread.
    // The entries at this index are read-write for the current thread and
    // read-only for all other threads.
    //
    // Returns the reserved index on success, the invalid index otherwise.
    inline size_t reserve_safe_ts_index();

    // Releases the given index into the safe_ts entries array, allowing the
    // entries at that index to be used by other threads.
    //
    // This method cannot fail or throw.
    inline void release_safe_ts_index(size_t& index);

    // Reserves a "safe timestamp" that protects its own and all subsequent
    // metadata entries from memory reclamation, by ensuring that the
    // pre-reclaim watermark can never advance past it until release_safe_ts()
    // is called on that timestamp.
    //
    // Any previously reserved "safe timestamp" value for this thread will be
    // atomically replaced by the new "safe timestamp" value.
    //
    // This operation can fail if post-publication validation fails. See the
    // reserve_safe_ts() implementation for details.
    //
    // Returns true if the given timestamp was successfully reserved, false
    // otherwise.
    inline bool reserve_safe_ts(size_t index, gaia_txn_id_t safe_ts, watermarks_t* watermarks);

    // Releases the "safe timestamp" previously reserved at the given index,
    // allowing the pre-reclaim watermark to advance past it, signaling to GC
    // tasks that the memory occupied by its metadata entry is eligible for
    // reclamation.
    //
    // This method cannot fail or throw.
    inline void release_safe_ts(size_t index, watermarks_t* watermarks);

    // Returns the reserved safe_ts at the given index, or the invalid timestamp
    // if no safe_ts is reserved.
    inline gaia_txn_id_t get_reserved_ts(size_t index);

    // Computes a "safe truncation timestamp", i.e., an (exclusive) upper bound
    // below which all txn metadata entries can be safely reclaimed.
    //
    // The timestamp returned is guaranteed not to exceed any "safe timestamp"
    // that was reserved before this method was called. For safety, callers must
    // avoid accessing txn metadata older than a previously reserved "safe
    // timestamp".
    inline gaia_txn_id_t get_safe_truncation_ts(watermarks_t* watermarks);

private:
    // A global array in which each session thread publishes a "safe timestamp"
    // that it needs to protect from memory reclamation. The minimum of all
    // published "safe timestamps" is an upper bound below which all txn
    // metadata entries can be safely reclaimed.
    //
    // Before using the safe_ts API, a thread must first reserve an index in
    // this array using reserve_safe_ts_index(). When a thread is no longer
    // using the safe_ts API (e.g., at session exit), it should call
    // release_safe_ts_index() to make this index available for use by other
    // threads.
    //
    // The only clients of the safe_ts API are expected to be session threads,
    // so we only allocate enough entries for the maximum allowed number of
    // session threads.
    std::atomic<gaia_txn_id_t::value_type> m_safe_ts_per_thread_entries[c_session_limit]{};

    // The reserved status of each index into `m_safe_ts_per_thread_entries` is
    // tracked in this bitmap. Before calling any safe_ts API functions, each
    // thread must reserve an index by setting a cleared bit in this bitmap.
    // When it is no longer using the safe_ts API (e.g., at session exit), each
    // thread should clear the bit that it set.
    std::atomic<uint64_t> m_safe_ts_reserved_indexes_bitmap[c_session_limit / common::c_uint64_bit_count]{};

public:
    static constexpr size_t c_invalid_safe_ts_index{std::numeric_limits<size_t>::max()};
    static constexpr size_t c_max_safe_ts_index{c_session_limit - 1};
};

#include "safe_ts.inc"

} // namespace db
} // namespace gaia
