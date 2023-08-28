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
#include "txn_metadata.hpp"

namespace gaia
{
namespace db
{

constexpr size_t c_max_session_id{c_session_limit - 1};

class safe_ts_entries_t
{
public:
    // We use an initialization method in lieu of a constructor to avoid having
    // to use placement new, since instances of this class reside in shared
    // memory.
    inline void initialize(
        watermark_type_t post_retire_watermark_type, watermark_type_t pre_reclaim_watermark_type);

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
    inline bool reserve_safe_ts(session_id_t session_id, gaia_txn_id_t safe_ts, txn_metadata_t* txn_metadata);

    // Releases the "safe timestamp" previously reserved at the given index,
    // allowing the pre-reclaim watermark to advance past it, signaling to GC
    // tasks that the memory occupied by its metadata entry is eligible for
    // reclamation.
    //
    // This method cannot fail or throw.
    inline void release_safe_ts(session_id_t session_id, txn_metadata_t* txn_metadata);

    // Unconditionally invalidates any "safe timestamp" at the given index,
    // whether it was previously reserved or not.
    inline void reinitialize_safe_ts(session_id_t session_id);

    // Returns the reserved safe_ts at the given index, or the invalid timestamp
    // if no safe_ts is reserved.
    inline gaia_txn_id_t get_reserved_ts(session_id_t session_id);

    // Computes a "maximum safe timestamp", i.e., an (exclusive) upper bound
    // below which all resources of a given type can be safely reclaimed.
    //
    // The timestamp returned is guaranteed not to exceed any "safe timestamp"
    // that was reserved before this method was called. For safety, callers must
    // avoid accessing resources older than a previously reserved "safe
    // timestamp".
    inline gaia_txn_id_t get_safe_reclamation_ts(txn_metadata_t* txn_metadata);

private:
    // This is the watermark type that serves as an upper bound for calculating
    // the "maximum safe timestamp" that must be protected from reclamation. A
    // resource cannot be reclaimed until it is retired, so the post-retire
    // watermark serves as an upper bound on the pre-reclaim watermark.
    watermark_type_t m_post_retire_watermark_type{watermark_type_t::unknown};
    // This is the watermark type we advance to the "maximum safe timestamp"
    // that must be protected from reclamation.
    watermark_type_t m_pre_reclaim_watermark_type{watermark_type_t::unknown};

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
    //
    // We pad each entry to 64 bytes (the width of a cache line) to prevent
    // memory contention from false sharing.
    struct element
    {
        alignas(c_cache_line_size_in_bytes)
        std::atomic<gaia_txn_id_t::value_type> entry;
    };

    element m_safe_ts_per_thread_entries[c_session_limit]{};


    inline std::atomic<gaia_txn_id_t::value_type>& entry_at(session_id_t session_id)
    {
        return m_safe_ts_per_thread_entries[session_id].entry;
    }
};

class session_metadata_t
{
public:
    // We use an initialization method in lieu of a constructor to avoid having
    // to use placement new, since instances of this class reside in shared
    // memory.
    inline void initialize();

    // Returns the reserved session ID on success, the invalid session ID
    // otherwise.
    inline session_id_t reserve_session_id();

    // Releases the given session ID, allowing this ID to be reused by other
    // threads.
    //
    // This method cannot fail or throw.
    inline void release_session_id(session_id_t& session_id);

    inline bool reserve_safe_ts_for_md_reclaim(session_id_t session_id, gaia_txn_id_t safe_ts, txn_metadata_t* txn_metadata);

    inline void release_safe_ts_for_md_reclaim(session_id_t session_id, txn_metadata_t* txn_metadata);

    inline gaia_txn_id_t get_reserved_ts_for_md_reclaim(session_id_t session_id);

    inline gaia_txn_id_t get_safe_md_reclaim_ts(txn_metadata_t* txn_metadata);

private:
    // The reserved status of each possible session ID is tracked in this
    // bitmap. Before publishing any session metadata, each thread must reserve
    // a session ID by setting a cleared bit in this bitmap. When it is no
    // longer using its session ID (e.g., at session exit), each thread should
    // clear the bit that it set.
    std::atomic<uint64_t> m_session_id_bitmap[c_session_limit / common::c_uint64_bit_count]{};

    safe_ts_entries_t m_md_reclaim_safe_ts_entries{};
};

#include "session_metadata.inc"

} // namespace db
} // namespace gaia
