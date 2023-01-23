////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include "gaia_internal/db/db_types.hpp"

namespace gaia
{
namespace db
{

/**
 * @brief Internal API for getting the begin_ts of the current txn.
 */
gaia_txn_id_t get_current_txn_id();

// The name of the SE server binary.
constexpr char c_db_server_exec_name[] = "gaia_db_server";

// The name of the default gaia instance.
constexpr char c_default_instance_name[] = "gaia_default_instance";

// The default location of the data directory.
constexpr char c_default_data_dir[] = "/var/lib/gaia/db";

// The customer facing name of the DB server.
constexpr char c_db_server_name[] = "Gaia Database Server";

// This is necessary to avoid VM exhaustion in the worst case where all
// sessions are opened from a single process (we remap the 256GB data
// segment for each session, so the 128TB of VM available to each process
// would be exhausted by 512 sessions opened in a single process, but we
// also create other large per-session mappings, so we need a large margin
// of error, hence the choice of 128 for the session limit).
// REVIEW: How much could we relax this limit if we revert to per-process
// mappings of the data segment?
constexpr size_t c_session_limit{1UL << 7};

// This is arbitrary but seems like a reasonable starting point (pending benchmarks).
constexpr size_t c_stream_batch_size{1UL << 10};

// We use this as the dummy payload of a session reply message from the server.
constexpr uint64_t c_session_magic{0xcafebabedecafbad};

// Time to spend backing off in a spin loop after contention was detected during GC.
// This should be comparable to (or less than) context switch latency (~5us).
constexpr size_t c_contention_backoff_us{3};

// Time to spend backing off in a spin loop before remapping the global snapshot.
// 90us was empirically determined to maximize throughput.
constexpr size_t c_remap_backoff_us{90};

// Timestamp interval between dumping system statistics to console.
constexpr size_t c_dump_stats_timestamp_interval{1UL << 20};

// The number of timestamp entries by which the pre-reclaim watermark can lag the
// post-GC watermark before we attempt to advance the pre-reclaim watermark.
constexpr size_t c_txn_metadata_reclaim_threshold{1024};

} // namespace db
} // namespace gaia
