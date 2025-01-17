////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <cstdint>

#include <bitset>
#include <limits>
#include <sstream>
#include <string>

#include "gaia/common.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/debug_assert.hpp"
#include "gaia_internal/db/db_types.hpp"

namespace gaia
{
namespace db
{

// This value class represents an immutable snapshot of txn metadata for a
// single timestamp, but with no association to a particular timestamp. (The
// association of a metadata entry value with a timestamp is the responsibility
// of the txn_metadata_t class.) This class implements operations (e.g.,
// bitfield manipulations) on the metadata format, so no other classes need
// knowledge of the format. It never reads or writes directly to the txn
// metadata array; that is the responsibility of the txn_metadata_t class.
class txn_metadata_entry_t
{
public:
    inline explicit txn_metadata_entry_t(uint64_t word)
        : m_word(word)
    {
    }

    txn_metadata_entry_t(const txn_metadata_entry_t&) = default;
    txn_metadata_entry_t& operator=(const txn_metadata_entry_t&) = default;

    friend inline bool operator==(txn_metadata_entry_t a, txn_metadata_entry_t b);
    friend inline bool operator!=(txn_metadata_entry_t a, txn_metadata_entry_t b);

    inline uint64_t get_word() const;

    static inline txn_metadata_entry_t uninitialized_value();
    static inline txn_metadata_entry_t sealed_value();
    static inline txn_metadata_entry_t new_begin_ts_entry();
    static inline txn_metadata_entry_t new_commit_ts_entry(
        gaia_txn_id_t commit_ts, gaia_txn_id_t begin_ts, db::log_offset_t log_offset);

    inline bool is_uninitialized() const;
    inline bool is_sealed() const;
    inline bool is_begin_ts_entry() const;
    inline bool is_commit_ts_entry() const;
    inline bool is_submitted() const;
    inline bool is_validating() const;
    inline bool is_decided() const;
    inline bool is_committed() const;
    inline bool is_aborted() const;
    inline bool is_gc_complete() const;
    inline bool is_durable() const;
    inline bool is_active() const;
    inline bool is_terminated() const;

    inline uint64_t get_status() const;
    inline gaia_txn_id_t get_timestamp(gaia_txn_id_t my_ts) const;
    inline db::log_offset_t get_log_offset() const;

    inline txn_metadata_entry_t set_submitted() const;
    inline txn_metadata_entry_t set_terminated() const;
    inline txn_metadata_entry_t set_decision(bool is_committed) const;
    inline txn_metadata_entry_t set_durable() const;
    inline txn_metadata_entry_t set_gc_complete() const;
    inline txn_metadata_entry_t set_timestamp(gaia_txn_id_t my_ts, gaia_txn_id_t linked_ts) const;

    inline const char* status_to_str() const;
    inline std::string dump_metadata() const;

private:
    // Transaction metadata constants.
    //
    // Transaction metadata format:
    // 64 bits:
    //   0-15 = linked timestamp offset
    //   16-31 = log offset
    //   32-58 = reserved
    //   59 = persistence status
    //   60 = gc status
    //   61-63 = txn status
    //
    // txn_status (3) | gc_status (1) | persistence_status (1) | reserved (27) | log_offset (16) | linked_timestamp_offset (16)

    static constexpr size_t c_txn_metadata_bit_width{common::c_uint64_bit_count};

    // Transaction status flags.
    static constexpr size_t c_txn_status_flags_bit_width{3};
    static constexpr size_t c_txn_status_flags_shift{c_txn_metadata_bit_width - c_txn_status_flags_bit_width};
    static constexpr uint64_t c_txn_status_flags_mask{
        ((1UL << c_txn_status_flags_bit_width) - 1) << c_txn_status_flags_shift};

    // These are all begin_ts status values.
    static constexpr uint64_t c_txn_status_active{0b010UL};
    static constexpr uint64_t c_txn_status_submitted{0b011UL};
    static constexpr uint64_t c_txn_status_terminated{0b001UL};

    // This is the bitwise intersection of all commit_ts status values.
    static constexpr uint64_t c_txn_status_commit_ts{0b100UL};
    static constexpr uint64_t c_txn_status_commit_mask{
        c_txn_status_commit_ts << c_txn_status_flags_shift};

    // This is the bitwise intersection of all commit_ts decided status values
    // (i.e., committed or aborted).
    static constexpr uint64_t c_txn_status_decided{0b110UL};
    static constexpr uint64_t c_txn_status_decided_mask{
        c_txn_status_decided << c_txn_status_flags_shift};

    // These are all commit_ts status values.
    static constexpr uint64_t c_txn_status_validating{0b100UL};
    static constexpr uint64_t c_txn_status_committed{0b111UL};
    static constexpr uint64_t c_txn_status_aborted{0b110UL};

    // Transaction GC status values.
    // These only apply to a commit_ts metadata entry.
    // We don't need TXN_GC_ELIGIBLE or TXN_GC_INITIATED flags, because any txn
    // behind the post-apply watermark (and with TXN_PERSISTENCE_COMPLETE set if
    // persistence is enabled) is eligible for GC, and an invalidated txn log
    // indicates that GC is in progress.
    static constexpr size_t c_txn_gc_flags_bit_width{1};
    static constexpr size_t c_txn_gc_flags_shift{
        (c_txn_metadata_bit_width - c_txn_gc_flags_bit_width) - c_txn_status_flags_bit_width};
    static constexpr uint64_t c_txn_gc_flags_mask{
        ((1UL << c_txn_gc_flags_bit_width) - 1) << c_txn_gc_flags_shift};

    // These are all commit_ts flag values.
    static constexpr uint64_t c_txn_gc_unknown{0b0UL};

    // This flag indicates that the txn log and all obsolete versions (undo
    // versions for a committed txn, redo versions for an aborted txn) have been
    // reclaimed by the system.
    static constexpr uint64_t c_txn_gc_complete{0b1UL};

    // This flag indicates whether the txn has been made externally durable
    // (i.e., persisted to the write-ahead log). It can't be combined with the
    // GC flags because a txn might be made durable before or after being
    // applied to the global snapshot, and we don't want one to block on the other.
    // However, a committed txn's redo versions cannot be reclaimed until it has
    // been marked durable (because they might be concurrently read for
    // persistence to the write-ahead log). If persistence is disabled, this
    // flag is unused.
    static constexpr size_t c_txn_persistence_flags_bit_width{1};
    static constexpr size_t c_txn_persistence_flags_shift{
        (c_txn_metadata_bit_width - c_txn_persistence_flags_bit_width)
        - (c_txn_status_flags_bit_width + c_txn_gc_flags_bit_width)};
    static constexpr uint64_t c_txn_persistence_flags_mask{
        ((1UL << c_txn_persistence_flags_bit_width) - 1) << c_txn_persistence_flags_shift};

    // These are all commit_ts flag values.
    static constexpr uint64_t c_txn_persistence_unknown{0b0UL};
    static constexpr uint64_t c_txn_persistence_complete{0b1UL};

    // This is a placeholder for the 27 (currently) reserved bits in the txn
    // metadata format.
    static constexpr size_t c_txn_reserved_flags_bit_width{27};

    // Txn log offset embedded in the txn metadata.
    // This is only present in a commit_ts metadata entry.
    // NB: we assume that any offset will be < 2^16!
    static constexpr size_t c_txn_log_offset_bit_width{16};
    static constexpr size_t c_txn_log_offset_shift{
        (c_txn_metadata_bit_width - c_txn_log_offset_bit_width)
        - (c_txn_status_flags_bit_width
           + c_txn_gc_flags_bit_width
           + c_txn_persistence_flags_bit_width
           + c_txn_reserved_flags_bit_width)};
    static constexpr uint64_t c_txn_log_offset_mask{
        ((1UL << c_txn_log_offset_bit_width) - 1) << c_txn_log_offset_shift};

    // We need the timestamp size constants to be public for now, because
    // they're used by the txn log metadata.
public:
    // 16-bit delta (from the current timestamp) of a linked txn timestamp.
    //
    // For a commit_ts entry, this is its associated begin_ts, and for a
    // begin_ts entry, this is its associated commit_ts. A commit_ts entry
    // always contains its linked begin_ts, but a begin_ts entry may not be
    // updated with its linked commit_ts until after the associated commit_ts
    // entry has been created.
    //
    // For a linked begin_ts, the delta is implicitly negative (since the
    // current timestamp is its corresponding commit_ts, so must have a larger
    // timestamp).
    //
    // NB: We must strictly enforce limits on the current range of txn
    // timestamps to prevent this delta from overflowing!

    static constexpr size_t c_txn_ts_delta_bit_width{16};
    static constexpr size_t c_txn_ts_delta_shift{0};
    static constexpr uint64_t c_txn_ts_delta_mask{((1UL << c_txn_ts_delta_bit_width) - 1) << c_txn_ts_delta_shift};

private:
    // Transaction metadata special values.

    // The first 3 bits of this value are unused for any txn state.
    static constexpr uint64_t c_value_uninitialized{0UL};

    // The first 3 bits of this value do not correspond to any valid txn status value.
    static constexpr uint64_t c_value_sealed{0b101UL << c_txn_status_flags_shift};

private:
    // Ideally this would be const, but that prevents us from reassigning a
    // variable of this type (we cannot just rebind the name, we have to invoke
    // the assignment operator, which is incompatible with immutability).
    uint64_t m_word;
};

#include "txn_metadata_entry.inc"

} // namespace db
} // namespace gaia
