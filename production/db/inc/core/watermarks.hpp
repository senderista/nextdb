////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

namespace gaia
{
namespace db
{
// These global timestamp variables are "watermarks" that represent the
// progress of various system functions with respect to transaction history.
// The "pre-apply" watermark represents an upper bound on the latest
// commit_ts whose txn log could have been applied to the shared locator
// view. A committed txn cannot have its txn log applied to the shared view
// until the pre-apply watermark has been advanced to its commit_ts. The
// "post-apply" watermark represents a lower bound on the same quantity, and
// also an upper bound on the latest commit_ts whose txn log could be
// eligible for GC. GC cannot be started for any committed txn until the
// post-apply watermark has advanced to its commit_ts. The "post-GC"
// watermark represents a lower bound on the latest commit_ts whose txn log
// could have had GC reclaim all its resources. Finally, the "pre-reclaim"
// watermark represents an (exclusive) upper bound on the timestamps whose
// metadata entries could have had their memory reclaimed (e.g., via
// zeroing, unmapping, or overwriting). Any timestamp whose metadata entry
// could potentially be dereferenced must be "reserved" via the "safe_ts"
// API to prevent the pre-reclaim watermark from advancing past it and
// allowing its metadata entry to be reclaimed.
//
// The pre-apply watermark must either be equal to the post-apply watermark or greater by 1.
//
// Schematically:
//    commit timestamps of transactions whose metadata entries have been reclaimed
//  < pre-reclaim watermark
//    <= commit timestamps of transactions completely garbage-collected
// <= post-GC watermark
//    <= commit timestamps of transactions applied to shared view
// <= post-apply watermark
//    < commit timestamp of transaction partially applied to shared view
// <= pre-apply watermark
//    < commit timestamps of transactions not applied to shared view.

enum class watermark_type_t
{
    pre_apply,
    post_apply,
    post_gc,
    pre_reclaim,
    // This should always be last.
    count
};

class watermarks_t
{
public:
    inline void clear()
    {
        for (auto& element : m_watermarks)
        {
            element.entry = {};
        }
    }

    // Returns the current value of the given watermark.
    inline gaia_txn_id_t get_watermark(watermark_type_t watermark_type, bool relaxed_load = false);

    // Atomically advances the given watermark to the given timestamp, if the
    // given timestamp is larger than the watermark's current value. It thus
    // guarantees that the watermark is monotonically nondecreasing in time.
    //
    // Returns true if the watermark was advanced to the given value, false
    // otherwise.
    inline bool advance_watermark(watermark_type_t watermark_type, gaia_txn_id_t ts);

    // Returns a reference to the array entry of the given watermark.
    inline std::atomic<gaia_txn_id_t::value_type>& get_watermark_entry(watermark_type_t watermark_type)
    {
        return m_watermarks[common::get_enum_value(watermark_type)].entry;
    }

private:
    // An array of monotonically nondecreasing timestamps, or "watermarks", that
    // represent the progress of system maintenance tasks with respect to txn
    // history. See `watermark_type_t` for a full explanation.
    //
    // We pad each entry to 64 bytes (the width of a cache line) to prevent
    // memory contention from false sharing.
    struct element
    {
        alignas(c_cache_line_size_in_bytes)
        std::atomic<gaia_txn_id_t::value_type> entry;
    };
    element m_watermarks[common::get_enum_value(watermark_type_t::count)];
};

#include "watermarks.inc"

} // namespace db
} // namespace gaia
