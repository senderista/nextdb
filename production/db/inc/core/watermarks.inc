////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

gaia_txn_id_t watermarks_t::get_watermark(watermark_type_t watermark_type, bool relaxed_load)
{
    std::memory_order order = relaxed_load ? std::memory_order_relaxed : std::memory_order_seq_cst;
    return m_watermarks[common::get_enum_value(watermark_type)].load(order);
}

// This advances the given global timestamp counter to the given timestamp
// value, unless it has already advanced beyond the given value due to a
// concurrent update.
//
// NB: we use compare_exchange_weak() for the global update because we need to
// retry anyway on concurrent updates, so tolerating spurious failures
// requires no additional logic.
bool watermarks_t::advance_watermark(watermark_type_t watermark_type, gaia_txn_id_t ts)
{
    // The initial load can be relaxed, because a stale read will just cause the
    // CAS to fail and retry. If the given timestamp is less than or equal to a
    // stale value of the watermark, it will transitively be less than the
    // current value of the watermark, so the early return logic is still
    // correct.
    bool relaxed_load = true;
    gaia_txn_id_t last_watermark_ts = get_watermark(watermark_type, relaxed_load);
    do
    {
        // NB: last_watermark_ts is an inout argument holding the previous value
        // on failure!
        if (ts <= last_watermark_ts)
        {
            return false;
        }

    } while (!get_watermark_entry(watermark_type).compare_exchange_weak(last_watermark_ts.value_ref(), ts.value()));

    return true;
}
