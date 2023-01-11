////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

size_t safe_ts_entries_t::reserve_safe_ts_index()
{
    // Try to set the first unset bit in the "free safe_ts indexes" bitmap.
    size_t reserved_index = safe_ts_entries_t::c_invalid_safe_ts_index;
    while (true)
    {
        reserved_index = find_first_unset_bit(
            m_safe_ts_reserved_indexes_bitmap, std::size(m_safe_ts_reserved_indexes_bitmap));

        // If our scan doesn't find any unset bits, immediately return failure
        // rather than retrying the scan (otherwise this could lead to an
        // infinite loop).
        if (reserved_index == c_max_bit_index)
        {
            return safe_ts_entries_t::c_invalid_safe_ts_index;
        }

        // Restart the scan if the bit was already set when we tried to set it,
        // because that means that another thread has already reserved this
        // index. We force try_set_bit_value() to fail in this case by passing
        // fail_if_already_set=true.
        bool fail_if_already_set = true;
        if (try_set_bit_value(
                m_safe_ts_reserved_indexes_bitmap,
                std::size(m_safe_ts_reserved_indexes_bitmap),
                reserved_index, true, fail_if_already_set))
        {
            break;
        }
    }

    return reserved_index;
}

void safe_ts_entries_t::release_safe_ts_index(size_t& index)
{
    ASSERT_PRECONDITION(
        index <= safe_ts_entries_t::c_max_safe_ts_index,
        "Expected this thread's safe_ts entry index to be valid!");

    ASSERT_PRECONDITION(
        is_bit_set(
            m_safe_ts_reserved_indexes_bitmap,
            std::size(m_safe_ts_reserved_indexes_bitmap),
            index),
        "Expected the bit for this thread's safe_ts entry index to be set!");

    // Invalidate this thread's safe_ts entry.
    // REVIEW: Should we expect the caller to do this beforehand and instead
    // assert that the entry is invalid?
    m_safe_ts_per_thread_entries[index] = c_invalid_gaia_txn_id;

    // Invalidate the thread-local copy of this entry's index before marking its
    // index "free".
    size_t safe_ts_index = index;
    index = safe_ts_entries_t::c_invalid_safe_ts_index;

    // Clear the bit for this entry's index in the "free safe_ts indexes"
    // bitmap.
    safe_set_bit_value(
        m_safe_ts_reserved_indexes_bitmap,
        std::size(m_safe_ts_reserved_indexes_bitmap),
        safe_ts_index, false);
}

bool safe_ts_entries_t::reserve_safe_ts(size_t index, gaia_txn_id_t safe_ts, watermarks_t* watermarks)
{
    ASSERT_PRECONDITION(
        index <= safe_ts_entries_t::c_max_safe_ts_index,
        "Expected this thread's safe_ts entry index to be valid!");

    // The reservation of a "safe timestamp" is split into 2 steps:
    // "publication" and "validation".
    // Publication makes the reserved timestamp visible to all other threads (so
    // it will be observed by any scan that starts after publication).
    // Validation ensures that the reserving thread cannot use a timestamp that
    // was published too late to avoid being overtaken by the pre-reclaim
    // watermark.
    auto& entry = m_safe_ts_per_thread_entries[index];
    // Relaxed load is sufficient because this thread must observe its own writes.
    auto current_value = entry.load(std::memory_order_relaxed);
    ASSERT_PRECONDITION(
        current_value == c_invalid_gaia_txn_id,
        "Cannot reserve a valid safe_ts entry!");

    // Speculatively publish the new safe_ts.
    // NB: This must be a seq_cst store, since it must be immediately visible to
    // other threads (which must perform a seq_cst load to observe it).
    entry = safe_ts;

    // Validate that the new safe_ts does not lag the post-GC watermark
    // (equality is acceptable because the post-GC watermark is an inclusive
    // upper bound on the pre-reclaim watermark, and the pre-reclaim watermark
    // is an exclusive upper bound on the virtual address range eligible for
    // decommit).
    if (safe_ts < watermarks->get_watermark(watermark_type_t::post_gc))
    {
        // If validation fails, retract the published entry.
        entry = c_invalid_gaia_txn_id;
        return false;
    }

    return true;
}

void safe_ts_entries_t::release_safe_ts(size_t index, watermarks_t* watermarks)
{
    ASSERT_PRECONDITION(
        index <= safe_ts_entries_t::c_max_safe_ts_index,
        "Expected this thread's safe_ts entry index to be valid!");

    // The entry for this thread should be valid and at least as large as the
    // pre-reclaim watermark (otherwise we either failed to retract the
    // publication of a safe_ts that failed validation, or the scan algorithm is
    // incorrect).
    auto& entry = m_safe_ts_per_thread_entries[index];
    // Relaxed load is sufficient because this thread must observe its own writes.
    auto current_value = entry.load(std::memory_order_relaxed);
    ASSERT_PRECONDITION(
        current_value != c_invalid_gaia_txn_id,
        "Cannot release an invalid safe_ts entry!");
    ASSERT_PRECONDITION(
        current_value >= watermarks->get_watermark(watermark_type_t::pre_reclaim),
        "A reserved safe_ts entry cannot lag the pre-reclaim watermark!");

    // Invalidate the previously published entry.
    // REVIEW: This could be a relaxed store without loss of correctness, since
    // it would only delay advancing the pre-reclaim watermark.
    entry = c_invalid_gaia_txn_id;
}

gaia_txn_id_t safe_ts_entries_t::get_reserved_ts(size_t index)
{
    ASSERT_PRECONDITION(
        index <= safe_ts_entries_t::c_max_safe_ts_index,
        "Expected this thread's safe_ts entry index to be valid!");

    // Relaxed load is sufficient because this thread must observe its own writes.
    return m_safe_ts_per_thread_entries[index].load(std::memory_order_relaxed);
}

gaia_txn_id_t safe_ts_entries_t::get_safe_reclamation_ts(watermarks_t* watermarks)
{
    // The algorithm (loosely based on Maged Michael's "Hazard Pointers: Safe
    // Memory Reclamation for Lock-Free Objects"):
    //
    // 1. Take a snapshot of the post-GC watermark. This is an upper bound on
    //    the "safe truncation timestamp" return value, and guarantees that the
    //    "safe truncation timestamp" will not exceed any concurrently reserved
    //    timestamp (which might not be visible to the scan).
    // 2. Scan the "published timestamps" array and compute the minimum of all
    //    valid timestamps observed.
    // 3. Take the minimum of the "minimum observed published timestamp" and the
    //    pre-scan snapshot of the post-GC watermark, and return that as the
    //    "safe truncation timestamp". (Note that this timestamp may actually be
    //    behind the current value of the pre-reclaim watermark, but we won't
    //    detect that until we fail to advance the pre-reclaim watermark to
    //    this timestamp. Any published timestamp that is already behind the
    //    pre-reclaim watermark will fail validation.)
    //
    // REVIEW: Need to specify this algorithm in a model-checkable spec language
    // like TLA+.
    //
    // Proof of correctness (very informal):
    //
    // We wish to show that after reserve_safe_ts(safe_ts) has returned success,
    // the pre-reclaim watermark can never exceed safe_ts.
    //
    // First, we need to show that get_safe_reclamation_ts() cannot return a
    // timestamp greater than safe_ts. There are only two possible cases to
    // consider: either (1) the scan of published timestamps observed safe_ts,
    // or (2) it did not.
    //
    // 1. If safe_ts was observed by the scan, then we know that it is an upper
    //    bound on the return value of get_safe_reclamation_ts(), so we are done.
    //
    // 2. If safe_ts was not observed by the scan, then the pre-scan snapshot of
    //    the post-GC watermark must have been taken before the validation-time
    //    snapshot of the post-GC watermark (because validation happens after
    //    publication, and the publication evidently did not happen before the
    //    scan started). Because validation succeeded, safe_ts is at least as
    //    large as the validation-time snapshot of the post-GC watermark, so
    //    (because watermarks are monotonically nondecreasing) it must also be
    //    at least as large as the pre-scan snapshot of the post-GC watermark.
    //    Because the pre-scan snapshot of the post-GC watermark is an upper
    //    bound on the return value of get_safe_reclamation_ts(), safe_ts is as
    //    well, and we are done.
    //
    // Given that safe_ts is an upper bound on the return value of
    // get_safe_reclamation_ts(), and the pre-reclaim watermark can only be
    // advanced to a return value of get_safe_reclamation_ts(), it follows that
    // safe_ts is always an upper bound on the pre-reclaim watermark. QED.
    //
    // Schematically:
    // "pre-reclaim watermark"
    // <= "pre-scan snapshot of post-GC watermark"
    // <= "publication-time value of post-GC watermark"
    // <= "validation-time snapshot of post-GC watermark"
    // <= "published and validated safe timestamp"

    // POSTCONDITION: safe_truncation_ts is the minimum of the pre-scan snapshot
    // of the post-GC watermark and the smallest published timestamp that the
    // scan observed.

    // Take a snapshot of the post-GC watermark before the scan.
    gaia_txn_id_t pre_scan_post_gc_watermark = watermarks->get_watermark(watermark_type_t::post_gc);

    // The post-GC watermark is an upper bound on the safe truncation timestamp.
    gaia_txn_id_t safe_truncation_ts = pre_scan_post_gc_watermark;

    // Scan the "published timestamps" array and compute the minimum of all
    // published timestamps observed. Then return the minimum of that result and
    // the pre-scan snapshot of the post-GC watermark.
    //
    // Note that a published timestamp may have already fallen behind the
    // current pre-reclaim watermark (which will make it fail validation). In
    // that case, we will return a timestamp older than the current pre-reclaim
    // watermark. There is no need to try to prevent this, because
    // advance_watermark() will fail, the current GC task will abort, and
    // another thread will try to advance the pre-reclaim watermark.
    for (const auto& entry : m_safe_ts_per_thread_entries)
    {
        // This must be a seq_cst load in order to ensure that the scan observes
        // any safe_ts entry published before it started.
        auto current_value = entry.load();

        // Skip invalid entries.
        if (current_value == c_invalid_gaia_txn_id)
        {
            continue;
        }

        // Update the minimum safe_ts.
        safe_truncation_ts = std::min(safe_truncation_ts.value(), current_value);
    }

    // Return the minimum of the pre-scan snapshot of the post-GC watermark and
    // the smallest published timestamp that the scan observed.
    return safe_truncation_ts;
}
