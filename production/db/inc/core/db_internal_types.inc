////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

using namespace gaia::common::bitmap;

void txn_log_t::set_begin_ts(gaia_txn_id_t begin_ts)
{
    // We should only call this on a newly allocated log offset.
    uint64_t old_begin_ts_with_refcount = begin_ts_with_refcount;
    ASSERT_PRECONDITION(
        old_begin_ts_with_refcount == 0,
        "Cannot call set_begin_ts() on an initialized txn log header!");

    uint64_t new_begin_ts_with_refcount = word_from_begin_ts_and_refcount(begin_ts, 0);
    bool has_set_value = begin_ts_with_refcount.compare_exchange_strong(old_begin_ts_with_refcount, new_begin_ts_with_refcount);
    ASSERT_POSTCONDITION(has_set_value, "set_begin_ts() should only be called when txn log offset is exclusively owned!");
}

bool txn_log_t::acquire_reference(gaia_txn_id_t begin_ts)
{
    ASSERT_PRECONDITION(begin_ts.is_valid(), "acquire_reference() must be called with a valid begin_ts!");
    while (true)
    {
        uint64_t old_begin_ts_with_refcount = begin_ts_with_refcount;

        // Return failure if ownership of this txn log has changed.
        if (begin_ts != begin_ts_from_word(old_begin_ts_with_refcount))
        {
            return false;
        }

        size_t refcount = refcount_from_word(old_begin_ts_with_refcount);
        uint64_t new_begin_ts_with_refcount = word_from_begin_ts_and_refcount(begin_ts, ++refcount);

        if (begin_ts_with_refcount.compare_exchange_strong(old_begin_ts_with_refcount, new_begin_ts_with_refcount))
        {
            break;
        }
    }

    return true;
}

// Releasing a reference must always succeed because the refcount can't be
// zero and the txn log offset can't be reused until all references are
// released.
// We pass the original begin_ts only to be able to check invariants.
void txn_log_t::release_reference(gaia_txn_id_t begin_ts)
{
    ASSERT_PRECONDITION(begin_ts.is_valid(), "release_reference() must be called with a valid begin_ts!");
    while (true)
    {
        uint64_t old_begin_ts_with_refcount = begin_ts_with_refcount;
        gaia_txn_id_t begin_ts = begin_ts_from_word(old_begin_ts_with_refcount);

        ASSERT_INVARIANT(
            begin_ts == begin_ts_from_word(old_begin_ts_with_refcount),
            "Cannot change ownership of a txn log with outstanding references!");

        size_t refcount = refcount_from_word(old_begin_ts_with_refcount);
        ASSERT_PRECONDITION(refcount > 0, "Cannot release a reference when refcount is zero!");
        uint64_t new_begin_ts_with_refcount = word_from_begin_ts_and_refcount(begin_ts, --refcount);

        if (begin_ts_with_refcount.compare_exchange_strong(old_begin_ts_with_refcount, new_begin_ts_with_refcount))
        {
            break;
        }
    }
}

// Returns false with no effect if begin_ts does not match the current
// begin_ts, or it matches but the refcount is nonzero.
// Otherwise, clears begin_ts_with_refcount and returns true.
bool txn_log_t::invalidate(gaia_txn_id_t begin_ts)
{
    uint64_t old_begin_ts_with_refcount = begin_ts_with_refcount;
    gaia_txn_id_t old_begin_ts = begin_ts_from_word(old_begin_ts_with_refcount);
    size_t old_refcount = refcount_from_word(old_begin_ts_with_refcount);

    // Invalidation should fail if either invalidation has already occurred
    // (with possible reuse), or there are outstanding shared references.
    if (old_begin_ts != begin_ts || old_refcount > 0)
    {
        return false;
    }

    uint64_t new_begin_ts_with_refcount = 0;
    if (begin_ts_with_refcount.compare_exchange_strong(old_begin_ts_with_refcount, new_begin_ts_with_refcount))
    {
        return true;
    }

    return false;
}

void logs_t::initialize()
{
    // Start log allocations at the first valid offset.
    m_next_unused_log_offset = c_first_log_offset;
    // Mark all log offsets as initially unallocated.
    std::fill(std::begin(m_allocated_log_offsets_bitmap), std::end(m_allocated_log_offsets_bitmap), 0);
    // Mark the invalid offset as allocated.
    safe_set_bit_value(m_allocated_log_offsets_bitmap, std::size(m_allocated_log_offsets_bitmap), c_invalid_log_offset, true);
}

bool logs_t::is_log_offset_allocated(log_offset_t offset)
{
    return is_bit_set(
        m_allocated_log_offsets_bitmap,
        std::size(m_allocated_log_offsets_bitmap),
        static_cast<size_t>(offset));
}

log_offset_t logs_t::allocate_used_log_offset()
{
    // Starting from the first valid offset, scan for the first unallocated
    // offset, up to a snapshot of m_next_unused_log_offset. If we fail to claim
    // an unallocated offset, restart the scan. (If we fail to find or claim any
    // reused offsets, then the caller can allocate a new offset from unused
    // memory.) Since m_next_unused_log_offset can be concurrently advanced, and
    // offsets can also be deallocated behind our scan pointer, this search is
    // best-effort; we could miss an offset deallocated concurrently with our
    // scan.
    size_t first_unused_offset = m_next_unused_log_offset;

    // If we're out of unused offsets, set the exclusive upper bound of the
    // bitmap scan to just past the end of the bitmap.
    if (first_unused_offset > c_last_log_offset)
    {
        first_unused_offset = c_last_log_offset + 1;
    }

    // Try to set the first unset bit in the "allocated log offsets" bitmap.
    log_offset_t allocated_offset = c_invalid_log_offset;
    while (true)
    {
        size_t first_unallocated_index = find_first_unset_bit(
            m_allocated_log_offsets_bitmap,
            std::size(m_allocated_log_offsets_bitmap),
            first_unused_offset);

        // If our scan doesn't find any unset bits, immediately return failure
        // rather than retrying the scan (otherwise this could lead to an
        // infinite loop).
        if (first_unallocated_index == c_max_bit_index)
        {
            break;
        }

        ASSERT_INVARIANT(
            first_unallocated_index >= c_first_log_offset
                && first_unallocated_index <= c_last_log_offset,
            "Index returned by find_first_unset_bit() is outside expected range!");

        // If the CAS to set the bit fails, restart the scan, even if the bit
        // was still unset when the CAS failed (a failed CAS indicates
        // contention and we should back off by restarting the scan).
        //
        // Restart the scan if the bit was already set when we tried to set it,
        // because that means that another thread has already allocated this
        // offset. We force try_set_bit_value() to fail in this case by passing
        // fail_if_already_set=true.
        bool fail_if_already_set = true;
        if (try_set_bit_value(
                m_allocated_log_offsets_bitmap,
                std::size(m_allocated_log_offsets_bitmap),
                first_unallocated_index, true, fail_if_already_set))
        {
            allocated_offset = static_cast<log_offset_t>(first_unallocated_index);
            break;
        }
    }

    return allocated_offset;
}

log_offset_t logs_t::allocate_unused_log_offset()
{
    // We claim the next available unused offset, and keep trying until we succeed.
    // (This is not wait-free, but conflicts should be rare.)
    while (true)
    {
        // Get the next available unused offset.
        size_t next_offset = m_next_unused_log_offset++;

        // If we've run out of log space, return the invalid offset.
        if (next_offset > c_last_log_offset)
        {
            return c_invalid_log_offset;
        }

        // At this point, we know that next_offset is a valid log_offset_t.
        ASSERT_INVARIANT(
            next_offset >= c_first_log_offset
                && next_offset <= c_last_log_offset,
            "next_offset is out of range!");

        // If the CAS to set the bit fails, try the next available unused
        // offset, even if the bit was still unset when the CAS failed (a failed
        // CAS indicates contention and we should back off by restarting the
        // scan).
        //
        // Retry if the bit was already set when we tried to set it,
        // because that means that another thread has already allocated this
        // offset. We force try_set_bit_value() to fail in this case by passing
        // fail_if_already_set=true.
        bool fail_if_already_set = true;
        if (try_set_bit_value(
                m_allocated_log_offsets_bitmap,
                std::size(m_allocated_log_offsets_bitmap),
                next_offset, true, fail_if_already_set))
        {
            return static_cast<log_offset_t>(next_offset);
        }
    }
}

// REVIEW: Under most workloads, only a few txn log offsets will ever be in use,
// so concurrent log offset allocations will contend on just 1 or 2 words in the
// allocated log offset bitmap. We could reduce contention by forcing
// allocations to use more of the available offset space. OTOH, the current
// implementation ensures that readers only need to scan a few words to find an
// unused offset, and it minimizes minor page faults and TLB misses. We could
// re-evaluate this reader/writer tradeoff if contention proves to be an issue.
log_offset_t logs_t::allocate_log_offset(gaia_txn_id_t begin_ts)
{
    // First try to reuse a deallocated offset.
    log_offset_t allocated_offset = allocate_used_log_offset();

    // If no deallocated offset is available, then allocate the next unused offset.
    if (!allocated_offset.is_valid())
    {
        allocated_offset = allocate_unused_log_offset();
    }

    // At this point, we must either have a valid offset, or we have run out of log space.
    ASSERT_INVARIANT(
        allocated_offset.is_valid() || (m_next_unused_log_offset > c_last_log_offset),
        "Log offset allocation cannot fail unless log space is exhausted!");

    // Initialize txn log metadata.
    // REVIEW: We could move this initialization logic into a wrapping function,
    // so this function is only responsible for allocating the offset.
    if (allocated_offset.is_valid())
    {
        ASSERT_INVARIANT(begin_ts.is_valid(), "Cannot allocate a txn log without a valid txn ID!");
        txn_log_t* txn_log = get_log_from_offset(allocated_offset);
        // If we allocated an unallocated or deallocated offset, its log header must be uninitialized.
        ASSERT_INVARIANT(txn_log->begin_ts() == 0, "Cannot allocate a txn log with a valid txn ID!");
        ASSERT_INVARIANT(txn_log->reference_count() == 0, "Cannot allocate a txn log with a nonzero reference count!");
        // Update the log header with the given begin timestamp.
        // (We don't initialize the refcount because that only tracks readers, not owners.)
        txn_log->set_begin_ts(begin_ts);
    }

    return allocated_offset;
}

void logs_t::deallocate_log_offset(log_offset_t offset)
{
    ASSERT_PRECONDITION(
        is_log_offset_allocated(offset), "Cannot deallocate unallocated log offset!");

    // The txn log header at this offset must have an invalid txn ID and zero refcount.
    // REVIEW: these asserts require access to shared memory, so could be debug-only.
    txn_log_t* txn_log = get_log_from_offset(offset);
    ASSERT_PRECONDITION(!txn_log->begin_ts().is_valid(), "Cannot deallocate a txn log with a valid txn ID!");
    ASSERT_PRECONDITION(txn_log->reference_count() == 0, "Cannot deallocate a txn log with a nonzero reference count!");

    safe_set_bit_value(
        m_allocated_log_offsets_bitmap,
        std::size(m_allocated_log_offsets_bitmap),
        static_cast<size_t>(offset), false);
}

txn_log_t* logs_t::get_log_from_offset(log_offset_t offset)
{
    ASSERT_PRECONDITION(offset.is_valid(), "Log offset is invalid!");

    return &(m_logs[static_cast<size_t>(offset)]);
}

size_t logs_t::get_used_logs_count()
{
    return count_set_bits(m_allocated_log_offsets_bitmap, std::size(m_allocated_log_offsets_bitmap));
}
