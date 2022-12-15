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

        // If the CAS to set the bit fails, restart the scan, even if the bit
        // was still unset when the CAS failed (a failed CAS indicates
        // contention and we should back off by restarting the scan).
        //
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

    // Invalidate both of this thread's safe_ts entries.
    m_safe_ts_per_thread_entries[index][0] = c_invalid_gaia_txn_id;
    m_safe_ts_per_thread_entries[index][1] = c_invalid_gaia_txn_id;

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
    // "publication" and "validation". Publication makes the reserved timestamp
    // visible to all other threads (so it will be observed by a scan).
    // Validation ensures that the reserving thread cannot use a timestamp that
    // was published too late to avoid being overtaken by the pre-truncate
    // watermark.
    //
    // NB: We need to maintain visibility of this thread's previous reserved
    // timestamp in case validation fails. (We cannot just invalidate it before
    // validation and then restore it after validation fails, because then it
    // would not be visible to a concurrent scan.) We do that by maintaining
    // *two* entries for each thread, and only invalidating the previous
    // reserved timestamp after validation of the new reserved timestamp
    // succeeds. An invalid entry will be ignored by the scan algorithm in its
    // calculation of the minimum published timestamp. (If both entries are
    // valid, then the scan algorithm will just take the minimum. If the minimum
    // entry is obsolete, then the scan algorithm will just return a result that
    // is more conservative--i.e. smaller--than necessary, but still correct.)
    //
    // Find the last invalid entry. Since the current thread has exclusive write
    // access to these entries, we should not have two valid entries (any
    // obsolete entry should have been invalidated by a previous call to this
    // method).
    auto& entries = m_safe_ts_per_thread_entries[index];
    bool is_entry_valid[2]{};
    for (size_t i = 0; i < std::size(entries); ++i)
    {
        is_entry_valid[i] = (entries[i] != c_invalid_gaia_txn_id);
    }
    ASSERT_INVARIANT(
        !is_entry_valid[0] || !is_entry_valid[1],
        "At most one safe_ts entry for this thread should be valid!");

    // Is exactly one of the two entries valid?
    bool has_valid_entry = (is_entry_valid[0] ^ is_entry_valid[1]);

    // Arbitrarily pick the highest-indexed invalid entry.
    size_t invalid_entry_index = is_entry_valid[1] ? 0 : 1;

    // Speculatively publish the new safe_ts in a currently invalid entry.
    entries[invalid_entry_index] = safe_ts;

    // Validate that the new safe_ts does not lag the post-GC watermark
    // (equality is acceptable because the post-GC watermark is an inclusive
    // upper bound on the pre-truncate watermark, and the pre-truncate watermark
    // is an exclusive upper bound on the memory address range eligible for
    // reclaiming).
    //
    // An essential optimization (necessary for the safe_ts_t implementation):
    // skip validation for a safe_ts that is replacing a smaller previously
    // reserved safe_ts. Even if the new safe_ts is behind the post-GC
    // watermark, it cannot be behind the pre-truncate watermark, because the
    // smaller previously reserved safe_ts prevents the pre-truncate watermark
    // from advancing past the new safe_ts until the new safe_ts is observed by
    // a scan. If both safe_ts values are published, then a concurrent scan will
    // use the obsolete value, but that is benign: it can only cause a
    // smaller-than-necessary safe truncation timestamp to be returned.
    bool should_validate = true;
    if (has_valid_entry)
    {
        // The valid index must be the invalid index + 1 mod 2.
        size_t valid_entry_index = invalid_entry_index ^ 1;
        should_validate = (safe_ts < entries[valid_entry_index]);
    }

    if (should_validate && safe_ts < watermarks->get_watermark(watermark_type_t::post_gc))
    {
        // If validation fails, invalidate this entry to revert to the
        // previously published entry.
        entries[invalid_entry_index] = c_invalid_gaia_txn_id;
        return false;
    }

    // Invalidate any previously published entry.
    if (has_valid_entry)
    {
        // The valid index must be the invalid index + 1 mod 2.
        size_t valid_entry_index = invalid_entry_index ^ 1;
        entries[valid_entry_index] = c_invalid_gaia_txn_id;
    }

    return true;
}

void safe_ts_entries_t::release_safe_ts(size_t index, watermarks_t* watermarks)
{
    ASSERT_PRECONDITION(
        index <= safe_ts_entries_t::c_max_safe_ts_index,
        "Expected this thread's safe_ts entry index to be valid!");

    // Find the index of the last valid entry. Since the current thread has
    // exclusive write access to these entries, and the contract is that this
    // method should only be called after reserve_safe_ts() returns success, we
    // should have exactly one valid entry. Also, any valid entry should be at
    // least as large as the pre-truncate watermark (otherwise we either failed
    // to invalidate the entry of a safe_ts that failed validation, or the scan
    // algorithm is incorrect).
    auto& entries = m_safe_ts_per_thread_entries[index];
    bool is_entry_valid[2]{};
    for (size_t i = 0; i < std::size(entries); ++i)
    {
        ASSERT_INVARIANT(
            (entries[i] == c_invalid_gaia_txn_id) ||
            (entries[i] >= watermarks->get_watermark(watermark_type_t::pre_truncate)),
            "A reserved safe_ts entry cannot lag the pre-truncate watermark!");

        is_entry_valid[i] = (entries[i] != c_invalid_gaia_txn_id);
    }

    ASSERT_INVARIANT(
        (is_entry_valid[0] ^ is_entry_valid[1]),
        "Exactly one safe_ts entry for this thread should be valid!");

    // Invalidate the previously published entry.
    size_t valid_entry_index = is_entry_valid[0] ? 0 : 1;
    entries[valid_entry_index] = c_invalid_gaia_txn_id;
}

void safe_ts_t::initialize(safe_ts_entries_t* safe_ts_entries, watermarks_t* watermarks)
{
    ASSERT_PRECONDITION(s_min_safe_ts_value == c_invalid_gaia_txn_id,
        "Saved minimum timestamp should be invalid at initialization!");

    // Cache pointers to thread-local safe_ts_entries_t and watermarks_t objects.
    s_safe_ts_entries_ptr = safe_ts_entries;
    s_watermarks_ptr = watermarks;

    // Allocate stack of safe_ts values.
    ASSERT_PRECONDITION(!s_safe_ts_values_ptr,
        "Stack of safe_ts values should not be allocated!");

    s_safe_ts_values_ptr = new std::vector<gaia_txn_id_t>{};
    // REVIEW: We should log the max number of entries ever used by the
    // stack in various workloads to determine a good initial capacity.
    s_safe_ts_values_ptr->reserve(8);

    // Reserve this thread's safe_ts entries index.
    ASSERT_PRECONDITION(s_safe_ts_entries_index == safe_ts_entries_t::c_invalid_safe_ts_index,
        "safe_ts entries index should be invalid!");

    s_safe_ts_entries_index = s_safe_ts_entries_ptr->reserve_safe_ts_index();
    if (s_safe_ts_entries_index == safe_ts_entries_t::c_invalid_safe_ts_index)
    {
        // REVIEW: This is a different failure mode than other uses of this
        // exception, should we use a different exception?
        throw safe_ts_failure();
    }
}

void safe_ts_t::finalize()
{
    ASSERT_PRECONDITION(s_safe_ts_entries_ptr,
        "Pointer to safe_ts entries should be set!");

    // Deallocate stack of safe_ts values.
    ASSERT_PRECONDITION(s_safe_ts_values_ptr,
        "Stack of safe_ts values should be allocated!");

    delete s_safe_ts_values_ptr;
    s_safe_ts_values_ptr = nullptr;
    s_min_safe_ts_value = c_invalid_gaia_txn_id;

    // Release this thread's safe_ts entries index.
    ASSERT_PRECONDITION(s_safe_ts_entries_index <= safe_ts_entries_t::c_max_safe_ts_index,
        "safe_ts entries index should be valid!");

    // This automatically invalidates s_safe_ts_entries_index.
    s_safe_ts_entries_ptr->release_safe_ts_index(s_safe_ts_entries_index);

    // We don't own these objects, so just clear their pointers.
    s_safe_ts_entries_ptr = nullptr;
    s_watermarks_ptr = nullptr;
}

gaia_txn_id_t safe_ts_t::get_safe_truncation_ts()
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
    //    behind the current value of the pre-truncate watermark, but we won't
    //    detect that until we fail to advance the pre-truncate watermark to
    //    this timestamp. Any published timestamp that is already behind the
    //    pre-truncate watermark will fail validation.)
    //
    // REVIEW: Need to specify this algorithm in a model-checkable spec language
    // like TLA+.
    //
    // Proof of correctness (very informal):
    //
    // We wish to show that after reserve_safe_ts(safe_ts) has returned success,
    // the pre-truncate watermark can never exceed safe_ts.
    //
    // First, we need to show that get_safe_truncation_ts() cannot return a
    // timestamp greater than safe_ts. There are only two possible cases to
    // consider: either (1) the scan of published timestamps observed safe_ts,
    // or (2) it did not.
    //
    // 1. If safe_ts was observed by the scan, then we know that it is an upper
    //    bound on the return value of get_safe_truncation_ts(), so we are done.
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
    //    bound on the return value of get_safe_truncation_ts(), safe_ts is as
    //    well, and we are done.
    //
    // Given that safe_ts is an upper bound on the return value of
    // get_safe_truncation_ts(), and the pre-truncate watermark can only be
    // advanced to a return value of get_safe_truncation_ts(), it follows that
    // safe_ts is always an upper bound on the pre-truncate watermark. QED.
    //
    // Schematically:
    // "pre-truncate watermark"
    // <= "pre-scan snapshot of post-GC watermark"
    // <= "publication-time value of post-GC watermark"
    // <= "validation-time snapshot of post-GC watermark"
    // <= "published and validated safe timestamp"

    // Take a snapshot of the post-GC watermark before the scan.
    gaia_txn_id_t pre_scan_post_gc_watermark = s_watermarks_ptr->get_watermark(watermark_type_t::post_gc);

    // The post-GC watermark is an upper bound on the safe truncation timestamp.
    gaia_txn_id_t safe_truncation_ts = pre_scan_post_gc_watermark;

    // Scan the "published timestamps" array and compute the minimum of all
    // published timestamps observed. Then return the minimum of that result and
    // the pre-scan snapshot of the post-GC watermark.
    //
    // Note that a published timestamp may have already fallen behind the
    // current pre-truncate watermark (which will make it fail validation). In
    // that case, we will return a timestamp older than the current pre-truncate
    // watermark. There is no need to try to prevent this, because
    // advance_watermark() will fail, the current GC task will abort, and
    // another thread will try to advance the pre-truncate watermark.
    for (const auto& per_thread_entries : s_safe_ts_entries_ptr->m_safe_ts_per_thread_entries)
    {
        for (const auto& per_thread_entry : per_thread_entries)
        {
            // Skip invalid entries.
            if (per_thread_entry == c_invalid_gaia_txn_id)
            {
                continue;
            }

            // Update the minimum safe_ts.
            safe_truncation_ts = std::min(safe_truncation_ts.value(), per_thread_entry.load());
        }
    }

    // Return the minimum of the pre-scan snapshot of the post-GC watermark and
    // the smallest published timestamp that the scan observed.
    return safe_truncation_ts;
}

safe_ts_t::safe_ts_t(gaia_txn_id_t safe_ts)
    : m_ts(safe_ts)
{
    ASSERT_PRECONDITION(s_safe_ts_entries_ptr, "Expected safe_ts entries to be allocated!");
    ASSERT_PRECONDITION(s_safe_ts_values_ptr, "Expected safe_ts stack to be allocated!");

    // To simplify the safe_watermark_t implementation, we allow safe_ts to be
    // invalid. The destructor will be a no-op in this case.
    if (!safe_ts.is_valid())
    {
        return;
    }

    DEBUG_ASSERT_INVARIANT(validate_saved_minimum(), "Failed to validate saved minimum at start of constructor!");

    // If safe_ts is the minimum element in the safe_ts stack, then
    // replace this thread's reserved safe_ts entry.
    ASSERT_INVARIANT(s_safe_ts_values_ptr->empty() || s_min_safe_ts_value.is_valid(),
        "Minimum timestamp must be valid if stack is non-empty!");
    if (!s_min_safe_ts_value.is_valid() || safe_ts < s_min_safe_ts_value)
    {
        if (!s_safe_ts_entries_ptr->reserve_safe_ts(s_safe_ts_entries_index, safe_ts, s_watermarks_ptr))
        {
            // Force the client to handle the validation failure.
            throw safe_ts_failure();
        }
        s_min_safe_ts_value = safe_ts;
    }

    // We have successfully reserved the new safe_ts if necessary, so push the
    // new safe_ts onto the stack of safe_ts values.
    s_safe_ts_values_ptr->push_back(safe_ts);

    DEBUG_ASSERT_INVARIANT(validate_saved_minimum(), "Failed to validate saved minimum at end of constructor!");
}

safe_ts_t::safe_ts_t(safe_ts_t&& other) noexcept
    : m_ts(std::exchange(other.m_ts, c_invalid_gaia_txn_id))
{
}

safe_ts_t& safe_ts_t::operator=(safe_ts_t&& other) noexcept
{
    this->m_ts = std::exchange(other.m_ts, c_invalid_gaia_txn_id);
    return *this;
}

safe_ts_t::operator gaia_txn_id_t() const
{
    return m_ts;
}

safe_ts_t::~safe_ts_t()
{
    // A moved-from object has an invalid timestamp.
    if (!m_ts.is_valid())
    {
        return;
    }

    DEBUG_ASSERT_INVARIANT(validate_saved_minimum(), "Failed to validate saved minimum at start of destructor!");

    // The destructor cannot fail, so we need to ensure that all
    // consistency conditions we already satisfied still hold.
    ASSERT_INVARIANT(s_safe_ts_entries_ptr, "Expected pointer to safe_ts entries to be set!");
    ASSERT_INVARIANT(s_safe_ts_values_ptr, "Expected safe_ts stack to be allocated!");
    ASSERT_INVARIANT(!s_safe_ts_values_ptr->empty(), "Expected safe_ts stack to be non-empty!");

    // Pop the safe_ts from the safe_ts stack.
    ASSERT_INVARIANT(m_ts == s_safe_ts_values_ptr->back(), "Timestamp must be at the top of the stack!");
    s_safe_ts_values_ptr->pop_back();

    // First handle the case where our safe_ts was the only value in the
    // safe_ts stack.
    if (s_safe_ts_values_ptr->empty())
    {
        ASSERT_INVARIANT(m_ts == s_min_safe_ts_value,
            "Timestamp must be the minimum if it is the only timestamp in the stack!");
        // Invalidate the saved minimum.
        s_min_safe_ts_value = c_invalid_gaia_txn_id;
        // Release this thread's reserved safe_ts value.
        s_safe_ts_entries_ptr->release_safe_ts(s_safe_ts_entries_index, s_watermarks_ptr);
        return;
    }

    // If our safe_ts was the minimum value in the stack, then calculate the new
    // minimum and replace the published safe_ts value if the minimum changed.
    ASSERT_INVARIANT(m_ts >= s_min_safe_ts_value, "Timestamp cannot be smaller than the minimum timestamp!");
    if (m_ts == s_min_safe_ts_value)
    {
        auto min_safe_ts = *std::min_element(s_safe_ts_values_ptr->begin(), s_safe_ts_values_ptr->end());
        ASSERT_INVARIANT(min_safe_ts >= s_min_safe_ts_value,
            "The new minimum timestamp cannot be smaller than the previous minimum timestamp!");
        if (min_safe_ts > s_min_safe_ts_value)
        {
            // Update the saved minimum timestamp.
            s_min_safe_ts_value = min_safe_ts;

            // Reserve the new minimum timestamp.
            bool reservation_succeeded = s_safe_ts_entries_ptr->reserve_safe_ts(
                s_safe_ts_entries_index, min_safe_ts, s_watermarks_ptr);
            // The new minimum safe_ts was protected by the previous minimum
            // safe_ts from falling behind the pre-truncate watermark, so
            // its reservation must succeed.
            ASSERT_INVARIANT(reservation_succeeded, "Reservation of new minimum safe_ts cannot fail!");
        }
    }

    DEBUG_ASSERT_INVARIANT(validate_saved_minimum(), "Failed to validate saved minimum at end of destructor!");
}

// The constructor reads the current value of the given watermark, and
// initializes a safe_ts_t object to that value. If initialization fails, then
// it retries after reading the value of the given watermark again.
safe_watermark_t::safe_watermark_t(watermark_type_t watermark_type)
{
    ASSERT_PRECONDITION(safe_ts_t::s_watermarks_ptr, "Expected watermarks pointer to be set!");

    // Retry until we have a valid safe_ts for the current value of
    // the watermark.
    while (true)
    {
        gaia_txn_id_t watermark_ts = safe_ts_t::s_watermarks_ptr->get_watermark(watermark_type);
        try
        {
            // First try to obtain a local safe_ts for the current value of the
            // watermark. If the local safe_ts is successfully initialized, use
            // it to initialize our member safe_ts. (Initialization cannot fail
            // because the move assignment operator does not release the "safe
            // timestamp" that was reserved in the local safe_ts's constructor.)
            safe_ts_t safe_ts(watermark_ts);
            m_safe_ts = std::move(safe_ts);
            // NB: It is legal to call any method without preconditions on a
            // moved-from safe_ts, because the standard specifies that
            // "Moved-from objects shall be placed in a valid but unspecified
            // state."
            ASSERT_POSTCONDITION(
                static_cast<gaia_txn_id_t>(safe_ts).is_valid() == false,
                "safe_ts should contain an invalid timestamp after moving!");
            break;
        }
        catch (const safe_ts_failure&)
        {
            continue;
        }
    }
}

safe_watermark_t::operator gaia_txn_id_t() const
{
    return gaia_txn_id_t(m_safe_ts);
}
