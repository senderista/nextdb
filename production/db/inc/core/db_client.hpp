////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <sys/socket.h>

#include "gaia/db/db.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/debug_assert.hpp"
#include "gaia_internal/common/generator_iterator.hpp"
#include "gaia_internal/common/mmap_helpers.hpp"
#include "gaia_internal/common/socket_helpers.hpp"
#include "gaia_internal/db/gaia_ptr.hpp"
#include "gaia_internal/exceptions.hpp"

#include "client_contexts.hpp"
#include "safe_ts.hpp"

namespace gaia
{
namespace db
{

// For declarations of friend functions.
#include "db_shared_data_interface.inc"

class client_t
{
    /**
     * @throws no_open_transaction_internal if there is no open transaction.
     */
    friend gaia::db::locators_t* gaia::db::get_locators();

    /**
     * @throws no_open_session_internal if there is no open session.
     */
    friend gaia::db::counters_t* gaia::db::get_counters();
    friend gaia::db::data_t* gaia::db::get_data();
    friend gaia::db::logs_t* gaia::db::get_logs();
    friend gaia::db::id_index_t* gaia::db::get_id_index();
    friend gaia::db::type_index_t* gaia::db::get_type_index();
    friend gaia::db::transactions::txn_metadata_t* get_txn_metadata();
    friend gaia::db::watermarks_t* get_watermarks();
    friend gaia::db::safe_ts_entries_t* get_safe_ts_entries();
    friend gaia::db::memory_manager::memory_manager_t* gaia::db::get_memory_manager();
    friend gaia::db::memory_manager::chunk_manager_t* gaia::db::get_chunk_manager();

public:
    // These functions are exported from gaia_internal/db/db.hpp.
    static inline gaia_txn_id_t get_current_txn_id();

    // These functions are exported from and documented in gaia/db/db.hpp.
    static inline bool is_session_open();
    static inline bool is_transaction_open();
    static void end_session();
    static void begin_transaction();
    static void rollback_transaction();
    static void commit_transaction();

    // These functions are consumed by the gaia_ptr_t class.
    static void log_txn_operation(
        gaia_locator_t locator,
        gaia_offset_t old_offset,
        gaia_offset_t new_offset);

    // Internal version of begin_session(), called by public interface in db.hpp.
    static void begin_session();

private:
    // Called by internal code to verify preconditions.
    static inline void verify_txn_active();
    static inline void verify_no_txn();

    static inline void verify_session_active();
    static inline void verify_no_session();

    // Context getters.
    static inline gaia_txn_id_t txn_id();
    static inline log_offset_t txn_log_offset();
    static inline txn_log_t* txn_log();

    static inline chunk_offset_t chunk_offset();
    static inline int session_socket();
    static inline mapped_data_t<locators_t>& private_locators();
    static inline mapped_data_t<locators_t>& shared_locators();
    static inline mapped_data_t<data_t>& shared_data();
    static inline std::vector<data_mapping_t>& data_mappings();
    static inline std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_logs_for_snapshot();
    static inline std::vector<std::pair<chunk_offset_t, chunk_version_t>>& map_gc_chunks_to_versions();
    static inline gaia_txn_id_t latest_applied_commit_ts();

private:
    // We don't use unique_ptr because its destructor is "non-trivial"
    // and that would add overhead to the TLS implementation.
    thread_local static inline client_session_context_t* s_session_context{nullptr};

private:
    static void init_memory_manager();

    static void txn_cleanup();

    static void commit_chunk_manager_allocations();
    static void rollback_chunk_manager_allocations();

    static void apply_txn_log(log_offset_t offset);

    static int get_session_socket(const std::string& socket_name);

    static void validate_txns_in_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts);

    static bool get_txn_log_offsets_in_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts,
        std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_ids_with_log_offsets);

    static void get_txn_log_offsets_for_snapshot(
        gaia_txn_id_t begin_ts, std::vector<std::pair<gaia_txn_id_t,
        log_offset_t>>& txn_ids_with_log_offsets_for_snapshot);

    // This is only intended for debug asserts.
    static bool is_log_sorted(txn_log_t* txn_log);
    static void sort_log(txn_log_t* txn_log);
    static gaia_txn_id_t submit_txn(gaia_txn_id_t begin_ts, log_offset_t log_offset);
    static bool validate_txn(gaia_txn_id_t commit_ts, bool is_committing_session = true);
    static bool txn_logs_conflict(log_offset_t offset1, log_offset_t offset2);

    static bool perform_maintenance();
    static bool apply_txn_logs_to_shared_view();
    static bool gc_applied_txn_logs();
    static bool update_post_gc_watermark();
    static bool truncate_txn_table();
    static char* get_txn_metadata_page_address_from_ts(gaia_txn_id_t ts);
    static size_t get_txn_metadata_page_count_from_ts_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts);

    static void apply_txn_log_from_ts(gaia_txn_id_t commit_ts);
    static void gc_txn_log_from_offset(log_offset_t log_offset, bool is_committed);
    static void deallocate_object(gaia_offset_t offset);
    static void deallocate_txn_log(txn_log_t* txn_log, bool is_committed);

    static bool acquire_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts);
    static void release_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts);
};

#include "db_client.inc"

} // namespace db
} // namespace gaia
