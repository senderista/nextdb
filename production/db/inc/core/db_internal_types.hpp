////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <cstddef>

#include <atomic>
#include <memory>
#include <ostream>

#include "gaia/common.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/bitmap.hpp"
#include "gaia_internal/common/mmap_helpers.hpp"
#include "gaia_internal/db/db_object.hpp"
#include "gaia_internal/db/db_types.hpp"

#include "memory_types.hpp"
#include "txn_metadata_entry.hpp"

namespace gaia
{
namespace db
{

template <typename T>
using aligned_storage_for_t =
    typename std::aligned_storage<sizeof(T), alignof(T)>::type;

enum class gaia_operation_t : uint8_t
{
    not_set = 0x0,
    create = 0x1,
    update = 0x2,
    remove = 0x3,
};

inline std::ostream& operator<<(std::ostream& os, const gaia_operation_t& o)
{
    switch (o)
    {
    case gaia_operation_t::not_set:
        os << "not_set";
        break;
    case gaia_operation_t::create:
        os << "create";
        break;
    case gaia_operation_t::update:
        os << "update";
        break;
    case gaia_operation_t::remove:
        os << "remove";
        break;
    default:
        ASSERT_UNREACHABLE("Unknown value of gaia_operation_t!");
    }
    return os;
}

constexpr char c_gaia_mem_locators_prefix[] = "gaia_mem_locators_";
constexpr char c_gaia_mem_locator_types_prefix[] = "gaia_mem_locator_types_";
constexpr char c_gaia_mem_counters_prefix[] = "gaia_mem_counters_";
constexpr char c_gaia_mem_data_prefix[] = "gaia_mem_data_";
constexpr char c_gaia_mem_logs_prefix[] = "gaia_mem_logs_";
constexpr char c_gaia_mem_id_index_prefix[] = "gaia_mem_id_index_";
constexpr char c_gaia_mem_type_index_prefix[] = "gaia_mem_type_index_";
constexpr char c_gaia_mem_txn_metadata_prefix[] = "gaia_mem_txn_metadata_";
constexpr char c_gaia_mem_watermarks_prefix[] = "gaia_mem_watermarks_";
constexpr char c_gaia_mem_session_metadata_prefix[] = "c_gaia_mem_session_metadata_";

#if __has_feature(thread_sanitizer)
// We set the maximum number of locators (including the reserved "invalid"
// value) to 2^29 in TSan builds, which reduces the data segment size from 256GB
// to 32GB. This seems small enough to avoid ENOMEM errors when mapping the data
// segment under TSan. Because our chunk address space is unchanged (still 2^16
// 4MB chunks), we could now segfault if we allocate too many chunks! However,
// given that we still have room for 1 minimum-sized (64B) object version per
// locator, this is unlikely, so it's probably acceptable for TSan builds (since
// they're not intended to be used in production). If we do encounter this
// issue, then we can add explicit checks to chunk allocation: we just need to
// define a new constant like constexpr size_t c_max_chunks = sizeof(data_t) /
// c_chunk_size_in_bytes; However, this would introduce a circular dependency
// between the memory manager headers and this header (which probably indicates
// excessive modularization).
constexpr size_t c_max_locators{(1UL << 29) - 1};
#else
// We allow as many locators as the number of 64B objects (the minimum size)
// that will fit into the data segment size of 256GB, or 2^38 / 2^6 = 2^32. The
// first entry of the locators array must be reserved for the invalid locator
// value (0), so we subtract 1.
constexpr size_t c_max_locators{(1UL << 32) - 1};
#endif

// This is the largest power of 2 that is compatible with a collision
// probability < 2^-20 for 32-bit randomized type IDs (see
// https://en.wikipedia.org/wiki/Birthday_problem#Square_approximation).
//
// REVIEW: If we move to either sequentially allocated type IDs or 64-bit or
// larger randomized type IDs, we can expand this limit.
constexpr size_t c_max_types = 64;

// With 2^32 locators, 2^26 hash buckets bounds the average hash chain length to
// 2^6. For more realistic workloads (say 2^27 locators, which bounds average
// hash chain length to 2), this gives constant-time performance. We currently
// use modulo reduction instead of a random hash function to map IDs to buckets,
// which gives excellent locality as long as IDs are sequentially allocated. If
// IDs are randomized, then locality will be poor and we may want to consider a
// more compact structure (e.g., a randomized binary search tree).
constexpr size_t c_hash_buckets{1UL << 26};
// For efficient modulo reduction, the hash bucket count should be a power of 2.
static_assert((c_hash_buckets & (c_hash_buckets - 1)) == 0, "Hash bucket count must be a power of 2!");
// We use uint32_t as the type of a hash node index.
static_assert(
    c_max_locators <= std::numeric_limits<uint32_t>::max(),
    "The index of a hash node must fit into 32 bits!");

// This is an array of offsets in the data segment corresponding to object
// versions, where each array index is referred to as a "locator."
// The first entry of the array is reserved for the invalid locator value 0.
// The elements are atomic because reads and writes to shared memory need to be
// synchronized across threads/processes.
typedef std::atomic<gaia_offset_t::value_type> locators_t[c_max_locators + 1];

// This is an array of type IDs in the data segment corresponding to locator
// types. An untyped or empty locator is mapped to c_invalid_gaia_type.
typedef std::atomic<common::gaia_type_t::value_type> locator_types_t[c_max_locators + 1];

struct hash_node_t
{
    // To enable atomic operations, we use the base integer type instead of
    // gaia_id_t and gaia_locator_t.
    std::atomic<common::gaia_id_t::value_type> id;
    std::atomic<gaia_locator_t::value_type> locator;
    std::atomic<uint32_t> next_index;
};

static_assert(decltype(hash_node_t::id)::is_always_lock_free);
static_assert(decltype(hash_node_t::locator)::is_always_lock_free);
static_assert(decltype(hash_node_t::next_index)::is_always_lock_free);

// We want to ensure that the hash node size never changes accidentally.
constexpr size_t c_hash_node_size = 16;
static_assert(c_hash_node_size == sizeof(hash_node_t), "Hash node size must be 16 bytes!");

struct log_record_t
{
    gaia_locator_t locator;
    uint16_t sequence;
    // We need 2 bytes of padding to maintain total size at 16 bytes.
    // (We place the padding here to align the two offsets on an 8-byte
    // boundary, in case we need to modify them both atomically in the future.)
    uint16_t reserved;
    gaia_offset_t old_offset;
    gaia_offset_t new_offset;

    friend std::ostream& operator<<(std::ostream& os, const log_record_t& lr)
    {
        os << "sequence: "
           << lr.sequence
           << "\tlocator: "
           << lr.locator
           << "\told_offset: "
           << lr.old_offset
           << "\tnew_offset: "
           << lr.new_offset
           << "\toperation: "
           << lr.operation()
           << std::endl;
        return os;
    }

    inline gaia_operation_t operation() const
    {
        if (old_offset.is_valid() && new_offset.is_valid())
        {
            return gaia_operation_t::update;
        }
        else if (!old_offset.is_valid() && new_offset.is_valid())
        {
            return gaia_operation_t::create;
        }
        else if (old_offset.is_valid() && !new_offset.is_valid())
        {
            return gaia_operation_t::remove;
        }
        else
        {
            ASSERT_UNREACHABLE("At least one offset in a log record must be valid!");
        }
    }
};

// We want to ensure that the txn log record size never changes accidentally.
constexpr size_t c_txn_log_record_size = 16;
static_assert(c_txn_log_record_size == sizeof(log_record_t), "Txn log record size must be 16 bytes!");

// We can reference at most 2^16 logs from the 16 bits available in a txn
// metadata entry, and we must reserve the value 0 for an invalid log offset.
constexpr size_t c_max_logs{(1UL << 16) - 1};

// The total size of a txn log in shared memory.
// We need to allow as many log records as the maximum number of live object
// versions (i.e., the maximum number of locators), and each log record occupies
// 16 bytes. We equally distribute the maximum number of live log records over
// the maximum number of live txn logs to get the maximum txn log size.
constexpr size_t c_txn_log_size{((c_max_locators + 1) * sizeof(log_record_t)) / (c_max_logs + 1)};

// We want to ensure that the txn log header size never changes accidentally.
constexpr size_t c_txn_log_header_size = 16;

// We need to ensure that the log header size is a multiple of the log record size, for alignment.
static_assert(
    (c_txn_log_header_size >= sizeof(log_record_t))
        && (c_txn_log_header_size % sizeof(log_record_t) == 0),
    "Header size must be a multiple of record size!");

// There can be at most 2^32 live versions in the data segment, and we can have
// at most 2^16 logs, so each log can contain at most 2^16 records, minus the
// number of record slots occupied by the log header.
constexpr size_t c_max_log_records{((c_max_locators + 1) / (c_max_logs + 1)) - (c_txn_log_header_size / sizeof(log_record_t))};

// The record count should fit into 16 bits.
static_assert(c_max_log_records <= std::numeric_limits<uint16_t>::max());

struct txn_log_t
{
    // This header should be a multiple of 16 bytes (the current multiple is 1).
    //
    // When a txn log becomes eligible for GC, there may still be txns in the
    // "open" phase applying this log to their snapshots, so we need to track
    // them separately via a reference count in the txn log header, and only
    // allow GC when the reference count is zero. This reference count tracks
    // outstanding readers, not owners! (The owning txn is identified by the
    // begin timestamp, or "txn ID", while the allocation status of this log
    // offset is tracked on the server, in the "allocated log offsets" bitmap.)
    // For lock-free reference count updates, we need to store the reference
    // count in the same word as the begin timestamp (to prevent changing the
    // reference count after the log offset has been reused).
    //
    // Before GC begins, the GC thread attempts to "invalidate" the txn log by
    // CAS'ing the begin_ts_with_refcount word to 0, provided that the refcount
    // is already 0 (this means that a nonzero reference count can indefinitely
    // delay GC). At the end of GC, the record_count field is zeroed, so all
    // existing log records are unreachable (i.e., garbage). Finally, the GC
    // thread clears the log offset's bit in the "allocated log offsets" bitmap,
    // to make it available for reuse.
    //
    // REVIEW: The reference count mechanism is not fault-tolerant: if a client
    // session thread applying logs to its snapshot crashes, it will never
    // decrement the reference counts for those logs and the offsets for those
    // logs (and their memory) can never be reused. The solution we adopt is to
    // only let the server modify reference counts. This means that we can only
    // GC a txn log after any txn that applied that log to its snapshot is
    // submitted, rather than as soon as that txn has finished applying the log
    // (i.e., when begin_transaction() returns). But a fortuitous benefit of
    // this solution is that if we keep shared references to all applied logs
    // for the duration of txn execution, then we can safely apply those logs to
    // a server-side local snapshot (which we currently use for table and index
    // scans). We can revisit this design (say, to have the client send another
    // message or signal an eventfd before begin_transaction() returns, to
    // notify the server that it's safe to release shared references on any logs
    // applied to the current txn's snapshot) after server-side snapshots are
    // unnecessary (i.e., when table and index scans move into shared memory).

    std::atomic<uint64_t> begin_ts_with_refcount;
    uint16_t record_count;

    // Pad the header to a multiple of 16 bytes (to align the first log record on 16 bytes).
    uint16_t reserved1;
    uint32_t reserved2;

    log_record_t log_records[c_max_log_records];

    friend std::ostream& operator<<(std::ostream& os, const txn_log_t& l)
    {
        os << "begin_ts: " << l.begin_ts() << std::endl;
        os << "record_count: " << l.record_count << std::endl;
        for (size_t i = 0; i < l.record_count; ++i)
        {
            os << l.log_records[i];
        }
        os << std::endl;
        return os;
    }

    static constexpr size_t c_txn_log_begin_ts_bit_width{48UL};
    static constexpr size_t c_txn_log_begin_ts_shift{common::c_uint64_bit_count - c_txn_log_begin_ts_bit_width};
    static constexpr size_t c_txn_log_begin_ts_mask{((1UL << c_txn_log_begin_ts_bit_width) - 1) << c_txn_log_begin_ts_shift};
    static constexpr size_t c_txn_log_refcount_bit_width{16UL};
    static constexpr uint64_t c_txn_log_refcount_shift{common::c_uint64_bit_count - c_txn_log_refcount_bit_width};
    static constexpr uint64_t c_txn_log_refcount_mask{((1UL << c_txn_log_refcount_bit_width) - 1) << c_txn_log_refcount_shift};

    inline static gaia_txn_id_t begin_ts_from_word(uint64_t word)
    {
        return gaia_txn_id_t{(word & c_txn_log_begin_ts_mask) >> c_txn_log_begin_ts_shift};
    }

    inline static size_t refcount_from_word(uint64_t word)
    {
        return static_cast<size_t>((word & c_txn_log_refcount_mask) >> c_txn_log_refcount_shift);
    }

    inline static uint64_t word_from_begin_ts_and_refcount(gaia_txn_id_t begin_ts, size_t refcount)
    {
        ASSERT_PRECONDITION(begin_ts.is_valid(), "Begin timestamp must be valid!");
        ASSERT_PRECONDITION(begin_ts.value() < (1UL << c_txn_log_begin_ts_bit_width), "Begin timestamp must fit in 48 bits!");
        ASSERT_PRECONDITION(refcount < (1UL << c_txn_log_refcount_bit_width), "Reference count must fit in 16 bits!");
        return (begin_ts << c_txn_log_begin_ts_shift) | (refcount << c_txn_log_refcount_shift);
    }

    inline gaia_txn_id_t begin_ts() const
    {
        return begin_ts_from_word(begin_ts_with_refcount);
    }

    inline size_t reference_count() const
    {
        return refcount_from_word(begin_ts_with_refcount);
    }

    // Sets the owning txn ID for this log.
    inline void set_begin_ts(gaia_txn_id_t begin_ts);

    // Returns false if the owning txn (begin_ts) changed, true otherwise.
    inline bool acquire_reference(gaia_txn_id_t begin_ts);

    // Releasing a reference must always succeed because the refcount can't be
    // zero and the txn log offset can't be reused until all references are
    // released.
    // We pass the original begin_ts only to be able to check invariants.
    inline void release_reference(gaia_txn_id_t begin_ts);

    // Returns false with no effect if begin_ts does not match the current
    // begin_ts, or it matches but the refcount is nonzero.
    // Otherwise, clears begin_ts_with_refcount and returns true.
    inline bool invalidate(gaia_txn_id_t begin_ts);
};

// The txn log header size may change in the future, but we want to explicitly
// assert that it is the expected size to catch any inadvertent changes.
static_assert(c_txn_log_header_size == offsetof(txn_log_t, log_records), "Txn log header size must be 16 bytes!");

static_assert(c_txn_log_size == sizeof(txn_log_t), "Txn log size must be 1MB!");

// To ensure the txn log record sequence number doesn't overflow,
// log_record_t::sequence must be at least as large as txn_log_t::record_count.
static_assert(sizeof(log_record_t::sequence) >= sizeof(txn_log_t::record_count));

constexpr log_offset_t c_first_log_offset{c_invalid_log_offset.value() + 1};
constexpr log_offset_t c_last_log_offset{c_max_logs};

class logs_t
{
public:
    inline void initialize();

    // Returns true if the given log offset is allocated, false otherwise.
    inline bool is_log_offset_allocated(log_offset_t offset);

    // Allocates the first unallocated log offset, returning the invalid log
    // offset if no unallocated offset is available.
    inline log_offset_t allocate_log_offset(gaia_txn_id_t begin_ts);

    // Deallocates the given log offset.
    inline void deallocate_log_offset(log_offset_t offset);

    // Returns the txn log at the given offset.
    inline txn_log_t* get_log_from_offset(log_offset_t offset);

    // Returns approximate count of log offsets currently in use.
    // For debugging purposes only.
    inline size_t get_used_logs_count();

    // Returns approximate count of all log offsets ever in use.
    // For debugging purposes only.
    inline size_t get_all_used_logs_count();

private:
    // Allocates the first used log offset.
    inline log_offset_t allocate_used_log_offset();

    // Allocates the first unused log offset.
    inline log_offset_t allocate_unused_log_offset();

private:
    // We use this offset to track the lowest-numbered log offset that has never
    // been allocated.
    // NB: We use size_t here rather than log_offset_t to avoid integer
    // overflow. A 64-bit atomically incremented counter cannot overflow in any
    // reasonable time.
    std::atomic<size_t> m_next_unused_log_offset{};

    // The allocated status of each log offset is tracked in this bitmap. When
    // opening a new txn, each session thread must allocate an offset for its txn
    // log by setting a cleared bit in this bitmap. When txn log GC completes,
    // the log's allocated bit should be cleared.
    std::atomic<uint64_t> m_allocated_log_offsets_bitmap[(c_max_logs + 1) / common::c_uint64_bit_count]{};

    // This is an array with 2^16 elements ("logs"), each holding 2^16 16-byte entries ("records").
    // The first element is unused because we need to reserve offset 0 for the "invalid log offset" value.
    // (This wastes only 1MB of virtual memory, which is inconsequential.)
    txn_log_t m_logs[c_max_logs + 1]{};
};

struct counters_t
{
    // These fields are used as cross-process atomic counters. We don't need
    // something like a cross-process mutex for this, as long as we use atomic
    // intrinsics for mutating the counters. (This is because the instructions
    // targeted by the intrinsics operate at the level of physical memory, not
    // virtual addresses.)
    //
    // NB: All these fields are initialized to 0, even though C++ doesn't
    // guarantee it, because this struct is constructed in a memory-mapped
    // shared-memory segment, and the OS automatically zeroes new pages.
    //
    // We pad all fields to 64 bytes (the width of a cache line) to prevent
    // memory contention from false sharing.
    alignas(c_cache_line_size_in_bytes)
    std::atomic<size_t> last_id;

    alignas(c_cache_line_size_in_bytes)
    std::atomic<size_t> last_txn_id;

    alignas(c_cache_line_size_in_bytes)
    std::atomic<size_t> last_locator;
};

struct data_t
{
    // We use std::aligned_storage to ensure that the backing array can only be
    // accessed at the proper alignment (64 bytes). An instance of db_object_t
    // is always aligned to the beginning of an element of the array, but it may
    // occupy multiple elements of the array.
    // The first entry of the array is reserved for the invalid offset value 0.
    aligned_storage_for_t<db_object_t> objects[c_max_locators + 1];
};

// This is a shared-memory hash table mapping gaia_id keys to locator values. We
// need a hash table node for each locator (to store the gaia_id key and the
// locator value). The hash nodes associated with each hash bucket are linked in
// a list whose head is stored in an array indexed by hash bucket
// (`list_head_index_for_bucket`). Each operation on the hash table
// (`db_hash_map::insert()`, `db_hash_map::find()`, `db_hash_map::remove()`)
// must first hash the given gaia_id to its bucket (currently using just modulo
// reduction, which works well for sequentially allocated gaia_ids), and begin
// traversing that bucket's list at the index given by the bucket's entry in
// `list_head_index_for_bucket`. If no node in the list contains a matching
// gaia_id, then find() and remove() return false, while insert() creates a new
// node, links it into the list, and returns true. If a matching node is found,
// then find() returns the locator stored in that node, insert() returns false,
// and remove() invalidates the locator stored in that node and returns true.
struct id_index_t
{
    // The index in `list_nodes` of the last allocated list node.
    // Always begins at 1, because the first list node is unused.
    std::atomic<size_t> last_allocated_list_node_index;
    // The index in `list_nodes` of the head of each bucket's linked list.
    // An empty bucket is indicated by a zero index.
    std::atomic<uint32_t> list_head_index_for_bucket[c_hash_buckets];
    // An array of list nodes storing an ID, locator, and the index of the next
    // node in its bucket's linked list.
    // Empty buckets are indicated by a zero index, so the first list node is unused.
    hash_node_t list_nodes[c_max_locators];
};

#include "db_internal_types.inc"

} // namespace db
} // namespace gaia
