////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <vector>
#include <unordered_map>

#include "gaia_internal/db/db_types.hpp"

#include "chunk_manager.hpp"
#include "db_internal_types.hpp"
#include "mapped_data.hpp"
#include "memory_manager.hpp"
#include "safe_ts.hpp"
#include "txn_metadata.hpp"
#include "type_index.hpp"
#include "watermarks.hpp"

namespace gaia
{
namespace db
{

struct client_transaction_context_t
{
    gaia_txn_id_t txn_id{c_invalid_gaia_txn_id};
    log_offset_t txn_log_offset{c_invalid_log_offset};

    inline bool initialized()
    {
        return txn_id.is_valid() && txn_log_offset.is_valid();
    }

    inline void clear()
    {
        txn_id = c_invalid_gaia_txn_id;
        txn_log_offset = c_invalid_log_offset;
    }

    inline ~client_transaction_context_t()
    {
        clear();
    }
};

struct client_session_context_t
{
    // The transaction context.
    std::unique_ptr<client_transaction_context_t> txn_context{};

    memory_manager::memory_manager_t memory_manager{};
    memory_manager::chunk_manager_t chunk_manager{};

    int fd_locators{-1};
    int session_socket{-1};

    // The client's memory mappings.
    //
    // TODO: Consider moving locators segment into transaction context.
    // Unlike the other mappings, the locators segment is re-mapped for each transaction.
    mapped_data_t<locators_t> private_locators{};
    mapped_data_t<locators_t> shared_locators{};
    mapped_data_t<counters_t> shared_counters{};
    mapped_data_t<data_t> shared_data{};
    mapped_data_t<logs_t> shared_logs{};
    mapped_data_t<id_index_t> shared_id_index{};
    mapped_data_t<type_index_t> shared_type_index{};
    mapped_data_t<transactions::txn_metadata_t> shared_txn_metadata{};
    mapped_data_t<watermarks_t> shared_watermarks{};
    mapped_data_t<safe_ts_entries_t> shared_safe_ts_entries{};

    // The list of data mappings that we manage together.
    // The order of declarations must be the order of data_mapping_t::index_t values!
    std::vector<data_mapping_t> data_mappings{};

    // This is used by GC tasks on a session thread to cache chunk IDs for empty chunk deallocation.
    std::unordered_map<chunk_offset_t, chunk_version_t> map_gc_chunks_to_versions{};

    // REVIEW [GAIAPLAT-2068]: When we enable snapshot reuse across txns (by
    // applying the undo log from the previous txn to the existing snapshot and
    // then applying redo logs from txns committed after the last shared
    // locators view update), we need to track the last commit_ts whose log was
    // applied to the snapshot, so we can ignore any logs committed at or before
    // that commit_ts.
    gaia_txn_id_t latest_applied_commit_ts{c_invalid_gaia_txn_id};

    // The current thread's safe_ts_entries index.
    size_t safe_ts_index{safe_ts_entries_t::c_invalid_safe_ts_index};

    void clear();

    client_session_context_t();

    inline ~client_session_context_t()
    {
        clear();
    }
};

} // namespace db
} // namespace gaia
