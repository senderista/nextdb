////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

constexpr char c_message_uninitialized_timestamp[] = "Uninitialized timestamp!";
constexpr char c_message_not_a_begin_timestamp[] = "Not a begin timestamp!";
constexpr char c_message_not_a_commit_timestamp[] = "Not a commit timestamp!";
constexpr char c_message_not_an_active_transaction[] = "Not an active transaction!";
constexpr char c_message_not_a_submitted_transaction[] = "Not a submitted transaction!";

bool txn_metadata_t::is_uninitialized_ts(gaia_txn_id_t ts, bool relaxed_load)
{
    return get_entry(ts, relaxed_load).is_uninitialized();
}

bool txn_metadata_t::is_sealed_ts(gaia_txn_id_t ts)
{
    return get_entry(ts).is_sealed();
}

bool txn_metadata_t::is_begin_ts(gaia_txn_id_t ts)
{
    return get_entry(ts).is_begin_ts_entry();
}

bool txn_metadata_t::is_commit_ts(gaia_txn_id_t ts)
{
    return get_entry(ts).is_commit_ts_entry();
}

bool txn_metadata_t::is_txn_submitted(gaia_txn_id_t begin_ts)
{
    return get_entry(begin_ts).is_submitted();
}

bool txn_metadata_t::is_txn_validating(gaia_txn_id_t commit_ts)
{
    return get_entry(commit_ts).is_validating();
}

bool txn_metadata_t::is_txn_decided(gaia_txn_id_t commit_ts)
{
    return get_entry(commit_ts).is_decided();
}

bool txn_metadata_t::is_txn_committed(gaia_txn_id_t commit_ts)
{
    return get_entry(commit_ts).is_committed();
}

bool txn_metadata_t::is_txn_aborted(gaia_txn_id_t commit_ts)
{
    return get_entry(commit_ts).is_aborted();
}

bool txn_metadata_t::is_txn_gc_complete(gaia_txn_id_t commit_ts)
{
    return get_entry(commit_ts).is_gc_complete();
}

bool txn_metadata_t::is_txn_durable(gaia_txn_id_t commit_ts)
{
    return get_entry(commit_ts).is_durable();
}

bool txn_metadata_t::is_txn_active(gaia_txn_id_t begin_ts)
{
    return get_entry(begin_ts).is_active();
}

bool txn_metadata_t::is_txn_terminated(gaia_txn_id_t begin_ts)
{
    return get_entry(begin_ts).is_terminated();
}

gaia_txn_id_t txn_metadata_t::get_begin_ts_from_commit_ts(gaia_txn_id_t commit_ts)
{
    ASSERT_PRECONDITION(is_commit_ts(commit_ts), c_message_not_a_commit_timestamp);
    return get_entry(commit_ts).get_timestamp();
}

gaia_txn_id_t txn_metadata_t::get_commit_ts_from_begin_ts(gaia_txn_id_t begin_ts)
{
    ASSERT_PRECONDITION(is_begin_ts(begin_ts), c_message_not_a_begin_timestamp);
    return get_entry(begin_ts).get_timestamp();
}

db::log_offset_t txn_metadata_t::get_txn_log_offset_from_ts(gaia_txn_id_t commit_ts)
{
    return get_entry(commit_ts).get_log_offset();
}

bool txn_metadata_t::seal_uninitialized_ts(gaia_txn_id_t ts)
{
    // If the metadata is not uninitialized, we can't seal it.
    // Use relaxed load because if the read is stale we will only incur an
    // unnecessary CAS.
    bool relaxed_load = true;
    if (!is_uninitialized_ts(ts, relaxed_load))
    {
        return false;
    }

    txn_metadata_entry_t expected_value{
        txn_metadata_entry_t::uninitialized_value()};
    txn_metadata_entry_t desired_value{
        txn_metadata_entry_t::sealed_value()};
    txn_metadata_entry_t actual_value{
        compare_exchange(ts, expected_value, desired_value)};

    if (actual_value != expected_value)
    {
        // We don't consider TXN_SUBMITTED or TXN_TERMINATED to be valid prior
        // states, because only the submitting thread can transition the txn to
        // these states.
        ASSERT_INVARIANT(
            actual_value != txn_metadata_entry_t::uninitialized_value(),
            "An uninitialized txn metadata entry cannot fail to be sealed!");
        return false;
    }

    return true;
}

void txn_metadata_t::set_active_txn_submitted(gaia_txn_id_t begin_ts)
{
    // Only an active txn can be submitted.
    ASSERT_PRECONDITION(is_txn_active(begin_ts), c_message_not_an_active_transaction);

    // We don't need a CAS here because only the session thread can submit or terminate a txn,
    // and an active txn cannot be sealed.
    auto new_entry = get_entry(begin_ts).set_submitted();
    set_entry(begin_ts, new_entry);
}

void txn_metadata_t::set_active_txn_terminated(gaia_txn_id_t begin_ts)
{
    // Only an active txn can be terminated.
    ASSERT_PRECONDITION(is_txn_active(begin_ts), c_message_not_an_active_transaction);

    // We don't need a CAS here because only the session thread can submit or terminate a txn,
    // and an active txn cannot be sealed.
    auto new_entry = get_entry(begin_ts).set_terminated();
    set_entry(begin_ts, new_entry);
}

void txn_metadata_t::set_submitted_txn_commit_ts(gaia_txn_id_t begin_ts, gaia_txn_id_t commit_ts)
{
    ASSERT_PRECONDITION(is_txn_submitted(begin_ts), c_message_not_a_submitted_transaction);
    ASSERT_PRECONDITION(is_commit_ts(commit_ts), c_message_not_a_commit_timestamp);

    // This can be a relaxed load because a stale read will just make us
    // redundantly set the same commit_ts.
    bool relaxed_load = true;
    auto old_entry = get_entry(begin_ts, relaxed_load);
    auto old_commit_ts = old_entry.get_timestamp();
    ASSERT_INVARIANT(!old_commit_ts.is_valid() || old_commit_ts == commit_ts,
        "A begin_ts entry can only contain one commit_ts value!");
    if (!old_commit_ts.is_valid())
    {
        // We don't need a CAS here because setting the commit_ts is idempotent.
        auto new_entry = old_entry.set_timestamp(commit_ts);
        // A relaxed store is safe because all current callers handle the case
        // of reading an invalid commit_ts from a submitted begin_ts entry.
        bool relaxed_store = true;
        set_entry(begin_ts, new_entry, relaxed_store);
    }
}

void txn_metadata_t::update_txn_decision(gaia_txn_id_t commit_ts, bool has_committed)
{
    txn_metadata_entry_t expected_value{
        get_entry(commit_ts)};
    txn_metadata_entry_t desired_value{
        expected_value.set_decision(has_committed)};
    txn_metadata_entry_t actual_value{
            compare_exchange(commit_ts, expected_value, desired_value)};

    if (actual_value != expected_value)
    {
        // The only state transition allowed from TXN_VALIDATING is to TXN_DECIDED.
        ASSERT_POSTCONDITION(
            actual_value.is_decided(),
            "commit_ts metadata in validating state can only transition to a decided state!");

        // If another txn validated before us, they should have reached the same decision.
        ASSERT_POSTCONDITION(
            actual_value.is_committed() == has_committed,
            "Inconsistent txn decision detected!");
    }
}

void txn_metadata_t::set_txn_durable(gaia_txn_id_t commit_ts)
{
    while (true)
    {
        txn_metadata_entry_t expected_value{
            get_entry(commit_ts)};
        txn_metadata_entry_t desired_value{
            expected_value.set_durable()};
        txn_metadata_entry_t actual_value{
            compare_exchange(commit_ts, expected_value, desired_value)};

        if (actual_value == expected_value)
        {
            break;
        }
    }
}

bool txn_metadata_t::set_txn_gc_complete(gaia_txn_id_t commit_ts)
{
    txn_metadata_entry_t expected_value{
        get_entry(commit_ts)};
    txn_metadata_entry_t desired_value{
        expected_value.set_gc_complete()};
    txn_metadata_entry_t actual_value{
        compare_exchange(commit_ts, expected_value, desired_value)};

    return (actual_value == expected_value);
}

size_t txn_metadata_t::ts_to_buffer_index(gaia_txn_id_t ts)
{
    // FIXME: We can't do this right now without introducing a circular
    // dependency between txn_metadata.hpp and watermarks.hpp. Best solution
    // might be to just absorb watermarks_t within txn_metadata_t.
    // DEBUG_ASSERT_PRECONDITION(ts >= get_first_safe_allocated_ts(), "Timestamp
    // metadata entry may have been reclaimed!");

    // The size is guaranteed to be a power of 2, so we can do modulo reduction
    // with bitwise operations.
    constexpr uint64_t remainder_mask{txn_metadata_t::c_num_entries - 1};
    return static_cast<size_t>(ts & remainder_mask);
}

txn_metadata_entry_t txn_metadata_t::get_entry(gaia_txn_id_t ts, bool relaxed_load)
{
    std::memory_order order = relaxed_load ? std::memory_order_relaxed : std::memory_order_seq_cst;
    return txn_metadata_entry_t{m_txn_metadata_map[ts_to_buffer_index(ts)].load(order)};
}

void txn_metadata_t::set_entry(gaia_txn_id_t ts, txn_metadata_entry_t entry, bool relaxed_store)
{
    std::memory_order order = relaxed_store ? std::memory_order_relaxed : std::memory_order_seq_cst;
    m_txn_metadata_map[ts_to_buffer_index(ts)].store(entry.get_word(), order);
}

txn_metadata_entry_t txn_metadata_t::compare_exchange(gaia_txn_id_t ts,
    txn_metadata_entry_t expected_value, txn_metadata_entry_t desired_value)
{
    auto expected_word = expected_value.get_word();
    auto desired_word = desired_value.get_word();
    // compare_exchange_strong() cannot spuriously fail, so if
    // actual_value == expected_value, then we know that
    // desired_value must have been stored at the address.
    m_txn_metadata_map[ts_to_buffer_index(ts)].compare_exchange_strong(expected_word, desired_word);
    // expected_word is passed by reference, and on exit holds the initial word at the address.
    txn_metadata_entry_t actual_value{expected_word};
    return actual_value;
}

inline bool txn_metadata_t::poll_for_decision(gaia_txn_id_t ts)
{
    ASSERT_PRECONDITION(is_commit_ts(ts), "Expected a commit_ts!");
    auto& entry = m_txn_metadata_map[ts_to_buffer_index(ts)];

    // We bound spinning by context switch latency, conservatively 5us.
    constexpr size_t c_iterations = 5 * common::backoff::c_pause_iterations_per_us;
    for (size_t i = 0; i < c_iterations; ++i)
    {
        txn_metadata_entry_t current_value{entry.load(std::memory_order_relaxed)};
        if (current_value.is_decided())
        {
            return true;
        }

        common::backoff::pause();
    }

    return false;
}

void txn_metadata_t::uninitialize_ts_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts)
{
    // We could just call set_entry(uninitialized_value()) for the whole range,
    // but that would be much less efficient than directly calling memset() on
    // the underlying memory.
    //
    // This is a circular buffer, so the logical timestamp range could span up
    // to two distinct physical entry ranges, one starting at the beginning of
    // the buffer and one ending at the end of the buffer.
    ASSERT_PRECONDITION(
        start_ts < end_ts,
        "The start timestamp must be older than the end timestamp!");

    ASSERT_PRECONDITION(
        end_ts - start_ts <= c_num_entries,
        "The timestamp range cannot be larger than the buffer size!");

    auto start_buffer_index = ts_to_buffer_index(start_ts);
    auto end_buffer_index = ts_to_buffer_index(end_ts);

    // If the physical range is contiguous, we don't need to split it.
    if (end_buffer_index > start_buffer_index)
    {
        void* range_start_ptr = m_txn_metadata_map + start_buffer_index;
        size_t range_size_bytes = (end_buffer_index - start_buffer_index) * sizeof(*m_txn_metadata_map);
        std::memset(range_start_ptr, 0, range_size_bytes);
        return;
    }

    // Otherwise, split the logical range into two physical ranges and zero each separately.
    ASSERT_INVARIANT(end_buffer_index <= start_buffer_index, "Expected physical range to be split!");

    void* first_range_start_ptr = m_txn_metadata_map;
    size_t first_range_size_bytes = end_buffer_index * sizeof(*m_txn_metadata_map);
    std::memset(first_range_start_ptr, 0, first_range_size_bytes);

    void* second_range_start_ptr = m_txn_metadata_map + start_buffer_index;
    size_t second_range_size_bytes = (c_num_entries - start_buffer_index) * sizeof(*m_txn_metadata_map);
    std::memset(second_range_start_ptr, 0, second_range_size_bytes);
}

void txn_metadata_t::dump_txn_metadata_at_ts(gaia_txn_id_t ts)
{
    std::cerr << get_entry(ts).dump_metadata();

    if (is_commit_ts(ts))
    {
        gaia_txn_id_t begin_ts = get_begin_ts_from_commit_ts(ts);
        std::cerr
            << "Metadata for commit_ts `" << ts << "` metadata's begin_ts `"
            << begin_ts << "`: " << std::endl
            << get_entry(begin_ts).dump_metadata();
    }

    if (is_begin_ts(ts))
    {
        // A begin_ts may not have a linked commit_ts, either because it is not
        // submitted or because it has not yet been updated with its commit_ts.
        gaia_txn_id_t commit_ts = get_commit_ts_from_begin_ts(ts);
        if (commit_ts.is_valid())
        {
            std::cerr
                << "Metadata for begin_ts `" << ts << "` metadata's commit_ts `"
                << commit_ts << "`: " << std::endl
                << get_entry(commit_ts).dump_metadata();
        }
    }
}
