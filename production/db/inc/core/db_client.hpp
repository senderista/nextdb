////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <mutex>
#include <optional>

#include <sys/socket.h>

#include "gaia/db/db.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/debug_assert.hpp"
#include "gaia_internal/common/generator_iterator.hpp"
#include "gaia_internal/common/mmap_helpers.hpp"
#include "gaia_internal/db/gaia_ptr.hpp"
#include "gaia_internal/exceptions.hpp"

#include "client_contexts.hpp"
#include "client_messenger.hpp"
#include "messages_generated.h"

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
    friend gaia::db::txn_log_t* gaia::db::get_txn_log();

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
    friend gaia::db::memory_manager::memory_manager_t* gaia::db::get_memory_manager();
    friend gaia::db::memory_manager::chunk_manager_t* gaia::db::get_chunk_manager();
    friend gaia::db::gaia_txn_id_t gaia::db::get_txn_id();

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

    // Internal version of begin_session(), called by public interface in db.hpp.
    static void begin_session();

    static inline int get_session_socket_for_txn();

    // This returns a generator object for locators of a given type.
    static std::shared_ptr<common::iterators::generator_t<gaia_locator_t>>
    get_locator_generator_for_type(common::gaia_type_t type);

    // This is a helper for higher-level methods that use
    // this generator to build a range or iterator object.
    template <typename T_element_type>
    static std::function<std::optional<T_element_type>()>
    get_stream_generator_for_socket(std::shared_ptr<int> stream_socket_ptr);

    // Called by internal code to verify preconditions.
    static inline void verify_txn_active();
    static inline void verify_no_txn();

    static inline void verify_session_active();
    static inline void verify_no_session();

private:
    // Context getters.
    static inline gaia_txn_id_t txn_id();
    static inline log_offset_t txn_log_offset();

    static inline int session_socket();
    static inline mapped_data_t<locators_t>& private_locators();
    static inline mapped_data_t<data_t>& shared_data();
    static inline std::vector<data_mapping_t>& data_mappings();

private:
    // We don't use an auto-pointer because its destructor is "non-trivial"
    // and that would add overhead to the TLS implementation.
    thread_local static inline client_session_context_t* s_session_context{nullptr};

private:
    static void init_memory_manager();

    static void txn_cleanup();

    static void commit_chunk_manager_allocations();
    static void rollback_chunk_manager_allocations();

    static void apply_txn_log(log_offset_t offset);

    static int get_session_socket(const std::string& socket_name);
};

#include "db_client.inc"

} // namespace db
} // namespace gaia
