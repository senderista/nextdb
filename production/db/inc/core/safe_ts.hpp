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
    // pre-truncate watermark can never advance past it until release_safe_ts()
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
    // allowing the pre-truncate watermark to advance past it, signaling to GC
    // tasks that the memory occupied by its metadata entry is eligible for
    // reclamation.
    //
    // This method cannot fail or throw.
    inline void release_safe_ts(size_t index, watermarks_t* watermarks);

private:
    // A global array in which each session thread publishes a "safe timestamp"
    // that it needs to protect from memory reclamation. The minimum of all
    // published "safe timestamps" is an upper bound below which all txn
    // metadata entries can be safely reclaimed.
    //
    // Each thread maintains 2 publication entries rather than 1, in order to
    // tolerate validation failures. See the reserve_safe_ts() implementation
    // for a full explanation.
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
    std::atomic<gaia_txn_id_t::value_type> m_safe_ts_per_thread_entries[c_session_limit][2]{};

    // REVIEW: Since we had to move the get_safe_truncation_ts() method into
    // safe_ts_t, the simplest way to keep it working is to give that class
    // access to m_safe_ts_per_thread_entries. This should probably be
    // refactored.
    friend class safe_ts_t;

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

class safe_ts_failure : public common::gaia_exception
{
};

// This class enables client code that is scanning a range of txn metadata
// to prevent any of that metadata's memory from being reclaimed during the
// scan. The client constructs a safe_ts_t object using the timestamp at the
// left endpoint of the scan interval, which will prevent the pre-truncate
// watermark from advancing past that timestamp until the safe_ts_t object
// exits its scope, ensuring that all txn metadata at or after that "safe
// timestamp" is protected from memory reclamation for the duration of the
// scan.
//
// The constructor adds its timestamp argument to a thread-local set
// containing the timestamps of all currently active safe_ts_t instances
// owned by this thread. The minimum timestamp in the set is recomputed, and
// if it has changed, the constructor attempts to reserve the new minimum
// timestamp for this thread. If reservation fails, then the constructor
// throws a `safe_ts_failure` exception.
//
// The destructor removes the object's timestamp from the thread-local set,
// causing the minimum timestamp in the set to be recomputed. If it has
// changed, the new minimum timestamp is reserved for this thread.
// (Reservation cannot fail, because the previous minimum timestamp must be
// smaller than the new minimum timestamp, so the pre-truncate watermark
// could not have advanced beyond the new minimum timestamp.)
class safe_ts_t
{
public:
    inline explicit safe_ts_t(gaia_txn_id_t safe_ts);
    inline ~safe_ts_t();
    safe_ts_t() = default;
    safe_ts_t(const safe_ts_t&) = delete;
    safe_ts_t& operator=(const safe_ts_t&) = delete;
    inline safe_ts_t(safe_ts_t&& other) noexcept;
    inline safe_ts_t& operator=(safe_ts_t&& other) noexcept;
    inline operator gaia_txn_id_t() const;

    // This method must be called before instantiating any safe_ts_t objects on
    // the current thread. It is intended to be called during session
    // initialization.
    static inline void initialize(safe_ts_entries_t* safe_ts_entries, watermarks_t* watermarks);

    // This method is intended to be called during session finalization. After
    // this method returns, no safe_ts objects can be instantiated on the
    // calling thread until initialize() is called again.
    static inline void finalize();

    // This method computes a "safe truncation timestamp", i.e., an (exclusive)
    // upper bound below which all txn metadata entries can be safely reclaimed.
    //
    // The timestamp returned is guaranteed not to exceed any "safe timestamp"
    // that was reserved before this method was called.
    static inline gaia_txn_id_t get_safe_truncation_ts();

private:
    // This member is logically immutable, but it cannot be `const` because
    // it needs to be set to an invalid value by the move
    // constructor/assignment operator.
    gaia_txn_id_t m_ts{c_invalid_gaia_txn_id};

    thread_local static inline size_t s_safe_ts_entries_index{safe_ts_entries_t::c_invalid_safe_ts_index};

    // We store entries in a stack to preserve RAII semantics (we push the
    // encapsulated timestamp onto the stack in the constructor and pop it from
    // the stack in the destructor).
    // This is a pointer rather than an inline object due to performance issues
    // with nontrivial TLS objects.
    thread_local static inline std::vector<gaia_txn_id_t>* s_safe_ts_values_ptr{nullptr};

    // To minimize scans for the minimum timestamp, we store the minimum
    // timestamp separately. There are no concurrency concerns since all static
    // variables of this class are thread-local.
    thread_local static inline gaia_txn_id_t s_min_safe_ts_value{c_invalid_gaia_txn_id};

    // To avoid circular dependencies, each thread must explicitly initialize
    // the class with pointers to safe_ts_entries_t and watermarks_t objects.
    thread_local static inline safe_ts_entries_t* s_safe_ts_entries_ptr{nullptr};
    thread_local static inline watermarks_t* s_watermarks_ptr{nullptr};

    // We need to give safe_watermark_t access to a thread-local watermarks_t
    // object, and this seems like the simplest way to do it.
    friend class safe_watermark_t;

private:
    // For use only in debug asserts.
    static inline bool validate_saved_minimum()
    {
        auto min_safe_ts = s_safe_ts_values_ptr->empty() ?
            c_invalid_gaia_txn_id :
            *std::min_element(
                s_safe_ts_values_ptr->begin(),
                s_safe_ts_values_ptr->end());
        return (min_safe_ts == s_min_safe_ts_value);
    }
};

// This class can be used in place of safe_ts_t, when the "safe timestamp"
// value is just the current value of a watermark. Unlike the safe_ts_t
// class, this class's constructor cannot fail, so clients do not need to
// handle exceptions from initialization failure. It should be preferred to
// safe_ts_t for protecting a range of txn metadata during a scan, whenever
// the left endpoint of the scan interval is just the current value of a
// watermark.
class safe_watermark_t
{
public:
    inline explicit safe_watermark_t(watermark_type_t watermark);
    safe_watermark_t() = delete;
    ~safe_watermark_t() = default;
    safe_watermark_t(const safe_watermark_t&) = delete;
    safe_watermark_t& operator=(const safe_watermark_t&) = delete;
    safe_watermark_t(safe_watermark_t&&) = delete;
    safe_watermark_t& operator=(safe_watermark_t&&) = delete;
    inline operator gaia_txn_id_t() const;

private:
    // This member is logically immutable, but it cannot be `const`, because
    // the constructor needs to move a local variable into it.
    safe_ts_t m_safe_ts{};
};

#include "safe_ts.inc"

} // namespace db
} // namespace gaia
