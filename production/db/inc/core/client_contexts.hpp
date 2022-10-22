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

#include "gaia_internal/db/db_types.hpp"

#include "chunk_manager.hpp"
#include "db_internal_types.hpp"
#include "mapped_data.hpp"
#include "memory_manager.hpp"
#include "type_index.hpp"

namespace gaia
{
namespace db
{

struct client_transaction_context_t
{
    gaia_txn_id_t txn_id;
    log_offset_t txn_log_offset;

public:
    inline ~client_transaction_context_t();

    inline void clear();
    inline bool initialized();
};

struct client_session_context_t
{
    // The transaction context.
    std::shared_ptr<client_transaction_context_t> txn_context;

    memory_manager::memory_manager_t memory_manager;
    memory_manager::chunk_manager_t chunk_manager;

    int fd_locators{-1};
    int session_socket{-1};

    // The client's memory mappings.
    //
    // TODO: Consider moving locators segment into transaction context.
    // Unlike the other mappings, the locators segment is re-mapped for each transaction.
    mapped_data_t<locators_t> private_locators;
    mapped_data_t<counters_t> shared_counters;
    mapped_data_t<data_t> shared_data;
    mapped_data_t<logs_t> shared_logs;
    mapped_data_t<id_index_t> shared_id_index;
    mapped_data_t<type_index_t> shared_type_index;

    // The list of data mappings that we manage together.
    // The order of declarations must be the order of data_mapping_t::index_t values!
    std::vector<data_mapping_t> data_mappings;

    // REVIEW [GAIAPLAT-2068]: When we enable snapshot reuse across txns (by
    // applying the undo log from the previous txn to the existing snapshot and
    // then applying redo logs from txns committed after the last shared
    // locators view update), we need to track the last commit_ts whose log was
    // applied to the snapshot, so we can ignore any logs committed at or before
    // that commit_ts.
    gaia_txn_id_t latest_applied_commit_ts;

public:
    client_session_context_t();
    inline ~client_session_context_t();

    void clear();
};

#include "client_contexts.inc"

} // namespace db
} // namespace gaia
