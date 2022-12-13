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

namespace gaia
{
namespace db
{

struct server_session_context_t
{
    int session_socket{-1};
    messages::session_state_t session_state{messages::session_state_t::DISCONNECTED};
    bool session_shutdown{false};
    int session_shutdown_eventfd{-1};

    gaia::db::memory_manager::memory_manager_t memory_manager;
    gaia::db::memory_manager::chunk_manager_t chunk_manager;

    std::string error_message;

    // These thread objects are owned by the session thread that created them.
    std::vector<std::thread> session_owned_threads;

public:
    inline ~server_session_context_t()
    {
        clear();
    }

    void clear();
};

} // namespace db
} // namespace gaia
