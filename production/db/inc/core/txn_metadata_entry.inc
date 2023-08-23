////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

bool operator==(txn_metadata_entry_t a, txn_metadata_entry_t b)
{
    return (a.m_word == b.m_word);
}

bool operator!=(txn_metadata_entry_t a, txn_metadata_entry_t b)
{
    return (a.m_word != b.m_word);
}

uint64_t txn_metadata_entry_t::get_word() const
{
    return m_word;
}

txn_metadata_entry_t txn_metadata_entry_t::uninitialized_value()
{
    return txn_metadata_entry_t{c_value_uninitialized};
}

txn_metadata_entry_t txn_metadata_entry_t::sealed_value()
{
    return txn_metadata_entry_t{c_value_sealed};
}

txn_metadata_entry_t txn_metadata_entry_t::new_begin_ts_entry()
{
    // Any initial begin_ts metadata entry must have its status initialized to TXN_ACTIVE.
    // All other bits should be 0.
    return txn_metadata_entry_t{c_txn_status_active << c_txn_status_flags_shift};
}

txn_metadata_entry_t txn_metadata_entry_t::new_commit_ts_entry(
    gaia_txn_id_t commit_ts, gaia_txn_id_t begin_ts, db::log_offset_t log_offset)
{
    ASSERT_PRECONDITION(
        begin_ts < commit_ts && commit_ts - begin_ts < (1UL << c_txn_ts_delta_bit_width),
        "commit_ts cannot exceed begin_ts by more than 2^16!");

    // We construct the commit_ts metadata by concatenating required bits.
    uint64_t shifted_log_offset = static_cast<uint64_t>(log_offset) << c_txn_log_offset_shift;
    constexpr uint64_t c_shifted_status_flags{
        c_txn_status_validating << c_txn_status_flags_shift};
    return txn_metadata_entry_t{c_shifted_status_flags | shifted_log_offset}
        .set_timestamp(commit_ts, begin_ts);
}

bool txn_metadata_entry_t::is_uninitialized() const
{
    return (m_word == c_value_uninitialized);
}

bool txn_metadata_entry_t::is_sealed() const
{
    return (m_word == c_value_sealed);
}

bool txn_metadata_entry_t::is_begin_ts_entry() const
{
    // "Uninitialized" values also have the commit bit unset, but they're not considered to be begin timestamps.
    return (!is_uninitialized() && ((m_word & c_txn_status_commit_mask) == 0));
}

bool txn_metadata_entry_t::is_commit_ts_entry() const
{
    // "Sealed" values also have the commit bit set, but they're not considered to be commit timestamps.
    return (!is_sealed() && ((m_word & c_txn_status_commit_mask) == c_txn_status_commit_mask));
}

bool txn_metadata_entry_t::is_submitted() const
{
    ASSERT_PRECONDITION(is_begin_ts_entry(), "Not a begin timestamp!");
    return (get_status() == c_txn_status_submitted);
}

bool txn_metadata_entry_t::is_validating() const
{
    ASSERT_PRECONDITION(is_commit_ts_entry(), "Not a commit timestamp!");
    return (get_status() == c_txn_status_validating);
}

bool txn_metadata_entry_t::is_decided() const
{
    ASSERT_PRECONDITION(is_commit_ts_entry(), "Not a commit timestamp!");
    constexpr uint64_t c_decided_mask = c_txn_status_decided << c_txn_status_flags_shift;
    return ((m_word & c_decided_mask) == c_decided_mask);
}

bool txn_metadata_entry_t::is_committed() const
{
    ASSERT_PRECONDITION(is_commit_ts_entry(), "Not a commit timestamp!");
    return (get_status() == c_txn_status_committed);
}

bool txn_metadata_entry_t::is_aborted() const
{
    ASSERT_PRECONDITION(is_commit_ts_entry(), "Not a commit timestamp!");
    return (get_status() == c_txn_status_aborted);
}

bool txn_metadata_entry_t::is_gc_complete() const
{
    ASSERT_PRECONDITION(is_commit_ts_entry(), "Not a commit timestamp!");
    uint64_t gc_flags = (m_word & c_txn_gc_flags_mask) >> c_txn_gc_flags_shift;
    return (gc_flags == c_txn_gc_complete);
}

bool txn_metadata_entry_t::is_durable() const
{
    ASSERT_PRECONDITION(is_commit_ts_entry(), "Not a commit timestamp!");
    uint64_t persistence_flags = (m_word & c_txn_persistence_flags_mask) >> c_txn_persistence_flags_shift;
    return (persistence_flags == c_txn_persistence_complete);
}

bool txn_metadata_entry_t::is_active() const
{
    ASSERT_PRECONDITION(is_begin_ts_entry(), "Not a begin timestamp!");
    return (get_status() == c_txn_status_active);
}

bool txn_metadata_entry_t::is_terminated() const
{
    ASSERT_PRECONDITION(is_begin_ts_entry(), "Not a begin timestamp!");
    return (get_status() == c_txn_status_terminated);
}

uint64_t txn_metadata_entry_t::get_status() const
{
    return ((m_word & c_txn_status_flags_mask) >> c_txn_status_flags_shift);
}

gaia_txn_id_t txn_metadata_entry_t::get_timestamp(gaia_txn_id_t my_ts) const
{
    ASSERT_PRECONDITION(is_begin_ts_entry() || is_commit_ts_entry(),
        "Can only get linked timestamp from a begin_ts or commit_ts entry!");

    size_t ts_delta = (m_word & c_txn_ts_delta_mask) >> c_txn_ts_delta_shift;

    // If the delta is 0, it means there is no linked timestamp present.
    if (ts_delta == 0)
    {
        return c_invalid_gaia_txn_id;
    }

    if (is_begin_ts_entry())
    {
        ASSERT_INVARIANT(std::numeric_limits<uint64_t>::max() - my_ts.value() >= ts_delta,
            "commit_ts value calculated from offset to begin_ts value would overflow!");
    }
    else
    {
        ASSERT_INVARIANT(my_ts.value() >= ts_delta,
            "begin_ts value calculated from offset to commit_ts value would underflow!");
    }

    return is_begin_ts_entry() ? (my_ts + ts_delta) : (my_ts - ts_delta);
}

db::log_offset_t txn_metadata_entry_t::get_log_offset() const
{
    ASSERT_PRECONDITION(is_commit_ts_entry(), "Not a commit timestamp!");

    // The txn log offset is the 16 bits of the ts metadata after the 3 status bits.
    return (m_word & c_txn_log_offset_mask) >> c_txn_log_offset_shift;
}

txn_metadata_entry_t txn_metadata_entry_t::set_submitted() const
{
    ASSERT_PRECONDITION(is_active(), "Not an active transaction!");

    // Transition the begin_ts metadata to the TXN_SUBMITTED state.
    constexpr uint64_t c_submitted_flags
        = c_txn_status_submitted << c_txn_status_flags_shift;

    return txn_metadata_entry_t{m_word | c_submitted_flags};
}

txn_metadata_entry_t txn_metadata_entry_t::set_terminated() const
{
    ASSERT_PRECONDITION(is_active(), "Not an active transaction!");

    constexpr uint64_t c_terminated_flags = c_txn_status_terminated << c_txn_status_flags_shift;
    return txn_metadata_entry_t{
        c_terminated_flags | (m_word & ~c_txn_status_flags_mask)};
}

txn_metadata_entry_t txn_metadata_entry_t::set_decision(bool has_committed) const
{
    // The commit_ts metadata must be in state TXN_VALIDATING or TXN_DECIDED.
    // (We allow the latter to enable idempotent concurrent validation.)
    ASSERT_PRECONDITION(
        is_validating() || is_decided(),
        "commit_ts metadata must be in validating or decided state!");

    uint64_t decided_status_flags{has_committed ? c_txn_status_committed : c_txn_status_aborted};

    // This masks out all bits except the commit_ts flag bits.
    constexpr uint64_t c_commit_flags_mask = ~c_txn_status_commit_ts << c_txn_status_flags_shift;

    // Turn off all commit flag bits, then set the decision flags.
    return txn_metadata_entry_t{
        (m_word & ~c_commit_flags_mask) | (decided_status_flags << c_txn_status_flags_shift)};
}

txn_metadata_entry_t txn_metadata_entry_t::set_durable() const
{
    ASSERT_PRECONDITION(is_decided(), "Not a decided transaction!");

    // Set persistence status to TXN_DURABLE.
    return txn_metadata_entry_t{
        m_word | (c_txn_persistence_complete << c_txn_persistence_flags_shift)};
}

txn_metadata_entry_t txn_metadata_entry_t::set_gc_complete() const
{
    ASSERT_PRECONDITION(is_decided(), "Not a decided transaction!");

    // Set GC status to TXN_GC_COMPLETE.
    return txn_metadata_entry_t{
        m_word | (c_txn_gc_complete << c_txn_gc_flags_shift)};
}

txn_metadata_entry_t txn_metadata_entry_t::set_timestamp(gaia_txn_id_t my_ts, gaia_txn_id_t linked_ts) const
{
    ASSERT_PRECONDITION(is_begin_ts_entry() || is_commit_ts_entry(),
        "Can only set linked timestamp on a begin_ts or commit_ts entry!");
    ASSERT_PRECONDITION(is_begin_ts_entry() ? (my_ts < linked_ts) : (linked_ts < my_ts),
        "A begin_ts must always precede its matching commit_ts!");

    // A begin_ts always precedes the matching commit_ts, so we ensure the sign
    // of the difference is always positive.
    uint64_t ts_delta = is_begin_ts_entry() ? (linked_ts - my_ts) : (my_ts - linked_ts);
    ASSERT_PRECONDITION(ts_delta < (1UL << c_txn_ts_delta_bit_width),
        "commit_ts cannot exceed begin_ts by more than 2^16!");

    return txn_metadata_entry_t{
        m_word | (ts_delta << c_txn_ts_delta_shift)};
}

const char* txn_metadata_entry_t::status_to_str() const
{
    ASSERT_PRECONDITION(
        !is_uninitialized() && !is_sealed(),
        "Not a valid txn metadata entry!");

    uint64_t status = get_status();
    switch (status)
    {
    case c_txn_status_active:
        return "ACTIVE";
    case c_txn_status_submitted:
        return "SUBMITTED";
    case c_txn_status_terminated:
        return "TERMINATED";
    case c_txn_status_validating:
        return "VALIDATING";
    case c_txn_status_committed:
        return "COMMITTED";
    case c_txn_status_aborted:
        return "ABORTED";
    default:
        ASSERT_UNREACHABLE("Unexpected txn metadata status flags!");
    }

    return nullptr;
}

std::string txn_metadata_entry_t::dump_metadata() const
{
    std::stringstream str;
    std::bitset<c_txn_metadata_bit_width> metadata_bits{m_word};

    str << "Transaction metadata bits: " << metadata_bits << std::endl;

    if (is_uninitialized())
    {
        str << "UNINITIALIZED" << std::endl;
        return str.str();
    }

    if (is_sealed())
    {
        str << "SEALED" << std::endl;
        return str.str();
    }

    str << "Type: " << (is_commit_ts_entry() ? "COMMIT_TIMESTAMP" : "BEGIN_TIMESTAMP") << std::endl;
    str << "Status: " << status_to_str() << std::endl;

    if (is_commit_ts_entry())
    {
        str << "Log offset for commit_ts metadata: " << get_log_offset() << std::endl;
    }
    return str.str();
}
