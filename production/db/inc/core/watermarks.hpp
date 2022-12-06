////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

// #include <cstdint>

#include "gaia/common.hpp"

// #include "gaia_internal/common/assert.hpp"
// #include "gaia_internal/common/debug_assert.hpp"
#include "gaia_internal/db/db_types.hpp"

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
// could have had GC reclaim all its resources. Finally, the "pre-truncate"
// watermark represents an (exclusive) upper bound on the timestamps whose
// metadata entries could have had their memory reclaimed (e.g., via
// zeroing, unmapping, or overwriting). Any timestamp whose metadata entry
// could potentially be dereferenced must be "reserved" via the "safe_ts"
// API to prevent the pre-truncate watermark from advancing past it and
// allowing its metadata entry to be reclaimed.
//
// The pre-apply watermark must either be equal to the post-apply watermark or greater by 1.
//
// Schematically:
//    commit timestamps of transactions whose metadata entries have been reclaimed
//  < pre-truncate watermark
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
    pre_truncate,
    // This should always be last.
    count
};

class watermarks_t
{
public:
    inline void clear()
    {
        for (auto& entry : m_watermarks)
        {
            entry = {};
        }
    }

    // Returns the current value of the given watermark.
    inline gaia_txn_id_t get_watermark(watermark_type_t watermark_type)
    {
        return m_watermarks[common::get_enum_value(watermark_type)].load();
    }

    // Atomically advances the given watermark to the given timestamp, if the
    // given timestamp is larger than the watermark's current value. It thus
    // guarantees that the watermark is monotonically nondecreasing in time.
    //
    // Returns true if the watermark was advanced to the given value, false
    // otherwise.
    inline bool advance_watermark(watermark_type_t watermark_type, gaia_txn_id_t ts);

private:
    // Returns a reference to the array entry of the given watermark.
    inline std::atomic<gaia_txn_id_t::value_type>& get_watermark_entry(watermark_type_t watermark_type)
    {
        return m_watermarks[common::get_enum_value(watermark_type)];
    }

private:
    // An array of monotonically nondecreasing timestamps, or "watermarks", that
    // represent the progress of system maintenance tasks with respect to txn
    // history. See `watermark_type_t` for a full explanation.
    std::atomic<gaia_txn_id_t::value_type> m_watermarks[common::get_enum_value(watermark_type_t::count)];
};

#include "watermarks.inc"

} // namespace db
} // namespace gaia
