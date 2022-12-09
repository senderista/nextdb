////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <thread>
#include <unordered_map>

#include "gaia_internal/db/db.hpp"
#include "gaia_internal/db/db_types.hpp"

#include "chunk_manager.hpp"
#include "db_internal_types.hpp"
#include "mapped_data.hpp"
#include "memory_manager.hpp"
#include "messages_generated.h"
#include "safe_ts.hpp"

namespace gaia
{
namespace db
{

struct server_transaction_context_t
{
    gaia_txn_id_t txn_id;
    log_offset_t txn_log_offset;

    std::vector<std::pair<gaia_txn_id_t, log_offset_t>> txn_logs_for_snapshot;

public:
    inline ~server_transaction_context_t();

    void clear();
};

struct server_session_context_t
{
    // The transaction context.
    std::shared_ptr<server_transaction_context_t> txn_context;

    int session_socket{-1};
    messages::session_state_t session_state{messages::session_state_t::DISCONNECTED};
    bool session_shutdown{false};
    int session_shutdown_eventfd{-1};

    gaia::db::memory_manager::memory_manager_t memory_manager;
    gaia::db::memory_manager::chunk_manager_t chunk_manager;

    std::string error_message;

    // These thread objects are owned by the session thread that created them.
    std::vector<std::thread> session_owned_threads;

    // This is used by GC tasks on a session thread to cache chunk IDs for empty chunk deallocation.
    std::unordered_map<chunk_offset_t, chunk_version_t> map_gc_chunks_to_versions;

    // The current thread's safe_ts_entries index.
    size_t safe_ts_index{safe_ts_entries_t::c_invalid_safe_ts_index};

public:
    inline ~server_session_context_t();

    void clear();
};

#include "server_contexts.inc"

} // namespace db
} // namespace gaia
