////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

// This advances the given global timestamp counter to the given timestamp
// value, unless it has already advanced beyond the given value due to a
// concurrent update.
//
// NB: we use compare_exchange_weak() for the global update because we need to
// retry anyway on concurrent updates, so tolerating spurious failures
// requires no additional logic.
bool watermarks_t::advance_watermark(watermark_type_t watermark_type, gaia_txn_id_t ts)
{
    gaia_txn_id_t last_watermark_ts = get_watermark(watermark_type);
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
