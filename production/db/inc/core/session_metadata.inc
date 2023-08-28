////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

void session_metadata_t::initialize()
{
    m_md_reclaim_safe_ts_entries.initialize(
        watermark_type_t::post_log_gc, watermark_type_t::pre_md_reclaim);
}

session_id_t session_metadata_t::reserve_session_id()
{
    while (true)
    {
        // Try to set the first unset bit in the "free session IDs" bitmap.
        size_t reserved_index = find_first_unset_bit(
            m_session_id_bitmap, std::size(m_session_id_bitmap));

        // If our scan doesn't find any unset bits, immediately return failure
        // rather than retrying the scan (otherwise this could lead to an
        // infinite loop).
        if (reserved_index == c_max_bit_index)
        {
            return c_invalid_session_id;
        }

        // Restart the scan if the bit was already set when we tried to set it,
        // because that means that another thread has already reserved this
        // index. We force try_set_bit_value() to fail in this case by passing
        // fail_if_already_set=true.
        bool fail_if_already_set = true;
        if (try_set_bit_value(
                m_session_id_bitmap,
                std::size(m_session_id_bitmap),
                reserved_index, true, fail_if_already_set))
        {
            return static_cast<session_id_t::value_type>(reserved_index);
        }
    }
}

void session_metadata_t::release_session_id(session_id_t& session_id)
{
    ASSERT_PRECONDITION(
        session_id <= c_max_session_id,
        "Expected this thread's session ID to be valid!");

    ASSERT_PRECONDITION(
        is_bit_set(
            m_session_id_bitmap,
            std::size(m_session_id_bitmap),
            session_id),
        "Expected the bit for this thread's session ID to be set!");

    // Invalidate this thread's safe_ts entries.
    // REVIEW: Should we expect the caller to do this beforehand and instead
    // assert that the entry is invalid? What would this imply for
    // fault-tolerance (maybe the cleanup path should perform invalidation
    // before releasing the session ID)?
    m_md_reclaim_safe_ts_entries.reinitialize_safe_ts(session_id);

    // Invalidate the thread-local copy of this entry's index before marking its
    // index "free".
    session_id_t old_session_id = session_id;
    session_id = c_invalid_session_id;

    // Clear the bit for this session ID in the "free session IDs" bitmap.
    safe_set_bit_value(
        m_session_id_bitmap,
        std::size(m_session_id_bitmap),
        old_session_id, false);
}

bool session_metadata_t::reserve_safe_ts_for_md_reclaim(
    session_id_t session_id, gaia_txn_id_t safe_ts, txn_metadata_t* txn_metadata)
{
    return m_md_reclaim_safe_ts_entries.reserve_safe_ts(session_id, safe_ts, txn_metadata);
}

void session_metadata_t::release_safe_ts_for_md_reclaim(
    session_id_t session_id, txn_metadata_t* txn_metadata)
{
    m_md_reclaim_safe_ts_entries.release_safe_ts(session_id, txn_metadata);
}

gaia_txn_id_t session_metadata_t::get_reserved_ts_for_md_reclaim(session_id_t session_id)
{
    return m_md_reclaim_safe_ts_entries.get_reserved_ts(session_id);
}

gaia_txn_id_t session_metadata_t::get_safe_md_reclaim_ts(txn_metadata_t* txn_metadata)
{
    return m_md_reclaim_safe_ts_entries.get_safe_reclamation_ts(txn_metadata);
}

void safe_ts_entries_t::initialize(
    watermark_type_t post_retire_watermark_type, watermark_type_t pre_reclaim_watermark_type)
{
    m_post_retire_watermark_type = post_retire_watermark_type;
    m_pre_reclaim_watermark_type = pre_reclaim_watermark_type;
}

bool safe_ts_entries_t::reserve_safe_ts(session_id_t session_id, gaia_txn_id_t safe_ts, txn_metadata_t* txn_metadata)
{
    ASSERT_PRECONDITION(
        session_id <= c_max_session_id,
        "Expected this thread's session ID to be valid!");

    // The reservation of a "safe timestamp" is split into 2 steps:
    // "publication" and "validation".
    // Publication makes the reserved timestamp visible to all other threads (so
    // it will be observed by any scan that starts after publication).
    // Validation ensures that the reserving thread cannot use a timestamp that
    // was published too late to avoid being overtaken by the pre-reclaim
    // watermark.
    auto& entry = entry_at(session_id);
    // Relaxed load is sufficient because this thread must observe its own writes.
    auto current_value = entry.load(std::memory_order_relaxed);
    ASSERT_PRECONDITION(
        current_value == c_invalid_gaia_txn_id,
        "Cannot reserve a valid safe_ts entry!");

    // Speculatively publish the new safe_ts.
    // NB: This must be a seq_cst store, since it must be immediately visible to
    // other threads (which must perform a seq_cst load to observe it).
    entry = safe_ts;

    // Validate that the new safe_ts does not lag the "upper bound" watermark
    // (equality is acceptable because the "upper bound" watermark is an
    // inclusive upper bound on the "advancing" watermark, and the "advancing"
    // watermark is an exclusive upper bound on the timestamp range eligible for
    // reclamation).
    if (safe_ts < txn_metadata->get_watermark(m_post_retire_watermark_type))
    {
        // If validation fails, retract the published entry.
        entry = c_invalid_gaia_txn_id;
        return false;
    }

    return true;
}

void safe_ts_entries_t::release_safe_ts(session_id_t session_id, txn_metadata_t* txn_metadata)
{
    ASSERT_PRECONDITION(
        session_id <= c_max_session_id,
        "Expected this thread's session ID to be valid!");

    // The entry for this thread should be valid and at least as large as the
    // pre-reclaim watermark (otherwise we either failed to retract the
    // publication of a safe_ts that failed validation, or the scan algorithm is
    // incorrect).
    auto& entry = entry_at(session_id);
    // Relaxed load is sufficient because this thread must observe its own writes.
    auto current_value = entry.load(std::memory_order_relaxed);
    ASSERT_PRECONDITION(
        current_value != c_invalid_gaia_txn_id,
        "Cannot release an invalid safe_ts entry!");
    ASSERT_PRECONDITION(
        current_value >= txn_metadata->get_watermark(m_pre_reclaim_watermark_type),
        "A reserved safe_ts entry cannot lag the advancing watermark!");

    // Invalidate the previously published entry.
    // REVIEW: This could be a relaxed store without loss of correctness, since
    // it would only delay advancing the pre-reclaim watermark.
    entry = c_invalid_gaia_txn_id;
}

void safe_ts_entries_t::reinitialize_safe_ts(session_id_t session_id)
{
    ASSERT_PRECONDITION(
        session_id <= c_max_session_id,
        "Expected this thread's session ID to be valid!");

    entry_at(session_id) = c_invalid_gaia_txn_id;
}

gaia_txn_id_t safe_ts_entries_t::get_reserved_ts(session_id_t session_id)
{
    ASSERT_PRECONDITION(
        session_id <= c_max_session_id,
        "Expected this thread's session ID to be valid!");

    // Relaxed load is sufficient because this thread must observe its own writes.
    return entry_at(session_id).load(std::memory_order_relaxed);
}

gaia_txn_id_t safe_ts_entries_t::get_safe_reclamation_ts(txn_metadata_t* txn_metadata)
{
    // The algorithm (loosely based on Maged Michael's "Hazard Pointers: Safe
    // Memory Reclamation for Lock-Free Objects"):
    //
    // 1. Take a snapshot of the post-retire watermark. This is an upper bound
    //    on the "safe reclamation timestamp" return value, and guarantees that
    //    the "safe reclamation timestamp" will not exceed any concurrently
    //    reserved timestamp (which might not be visible to the scan).
    // 2. Scan the "published timestamps" array and compute the minimum of all
    //    valid timestamps observed.
    // 3. Take the minimum of the "minimum observed published timestamp" and the
    //    pre-scan snapshot of the post-retire watermark, and return that as the
    //    "safe reclamation timestamp". (Note that this timestamp may actually
    //    be behind the current value of the pre-reclaim watermark, but we won't
    //    detect that until we fail to advance the pre-reclaim watermark to this
    //    timestamp. Any published timestamp that is already behind the
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
    //    the post-retire watermark must have been taken before the validation-time
    //    snapshot of the post-retire watermark (because validation happens after
    //    publication, and the publication evidently did not happen before the
    //    scan started). Because validation succeeded, safe_ts is at least as
    //    large as the validation-time snapshot of the post-retire watermark, so
    //    (because watermarks are monotonically nondecreasing) it must also be
    //    at least as large as the pre-scan snapshot of the post-retire watermark.
    //    Because the pre-scan snapshot of the post-retire watermark is an upper
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
    // <= "pre-scan snapshot of post-retire watermark"
    // <= "publication-time value of post-retire watermark"
    // <= "validation-time snapshot of post-retire watermark"
    // <= "published and validated safe timestamp"

    // POSTCONDITION: safe_reclamation_ts is the minimum of the pre-scan snapshot
    // of the post-retire watermark and the smallest published timestamp that the
    // scan observed.

    // Take a snapshot of the post-retire watermark before the scan.
    gaia_txn_id_t pre_scan_post_retire_watermark = txn_metadata->get_watermark(m_post_retire_watermark_type);

    // The post-retire watermark is an upper bound on the safe reclamation timestamp.
    gaia_txn_id_t safe_reclamation_ts = pre_scan_post_retire_watermark;

    // Scan the "published timestamps" array and compute the minimum of all
    // published timestamps observed. Then return the minimum of that result and
    // the pre-scan snapshot of the post-retire watermark.
    //
    // Note that a published timestamp may have already fallen behind the
    // current pre-reclaim watermark (which will make it fail validation). In
    // that case, we will return a timestamp older than the current pre-reclaim
    // watermark. There is no need to try to prevent this, because
    // advance_watermark() will fail, the current GC task will abort, and
    // another thread will try to advance the pre-reclaim watermark.
    for (const auto& element : m_safe_ts_per_thread_entries)
    {
        // This must be a seq_cst load in order to ensure that the scan observes
        // any safe_ts entry published before it started.
        auto current_value = element.entry.load();

        // Skip invalid entries.
        if (current_value == c_invalid_gaia_txn_id)
        {
            continue;
        }

        // Update the minimum safe_ts.
        safe_reclamation_ts = std::min(safe_reclamation_ts.value(), current_value);
    }

    // Return the minimum of the pre-scan snapshot of the post-retire watermark and
    // the smallest published timestamp that the scan observed.
    return safe_reclamation_ts;
}
