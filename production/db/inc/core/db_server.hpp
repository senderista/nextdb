////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <csignal>

#include <atomic>
#include <functional>
#include <utility>

#include "gaia/exception.hpp"

#include "gaia_internal/common/generator_iterator.hpp"

#include "chunk_manager.hpp"
#include "mapped_data.hpp"
#include "memory_manager.hpp"
#include "session_metadata.hpp"
#include "txn_metadata.hpp"
#include "type_index.hpp"

namespace gaia
{
namespace db
{

/**
 * Encapsulates the server_t configuration.
 */
class server_config_t
{
    friend class server_t;

public:
    enum class persistence_mode_t : uint8_t
    {
        e_enabled,
        e_disabled,
        e_disabled_after_recovery,
        e_reinitialized_on_startup,
    };

    static constexpr persistence_mode_t c_default_persistence_mode = persistence_mode_t::e_enabled;

public:
    server_config_t(server_config_t::persistence_mode_t persistence_mode, std::string instance_name, std::string data_dir)
        : m_persistence_mode(persistence_mode), m_instance_name(std::move(instance_name)), m_data_dir(std::move(data_dir))
    {
    }

    inline persistence_mode_t persistence_mode()
    {
        return m_persistence_mode;
    }

    inline const std::string& instance_name()
    {
        return m_instance_name;
    }

    inline const std::string& data_dir()
    {
        return m_data_dir;
    }

    inline bool is_persistence_enabled()
    {
        return (m_persistence_mode != persistence_mode_t::e_disabled)
            && (m_persistence_mode != persistence_mode_t::e_disabled_after_recovery);
    }

private:
    // Dummy constructor to allow server_t initialization.
    server_config_t()
        : m_persistence_mode(c_default_persistence_mode)
    {
    }

private:
    persistence_mode_t m_persistence_mode;
    std::string m_instance_name;
    std::string m_data_dir;
};

// For declarations of friend functions.
#include "db_shared_data_interface.inc"

class server_t
{
    friend class gaia_ptr_t;

    friend gaia::db::locators_t* gaia::db::get_locators();
    friend gaia::db::counters_t* gaia::db::get_counters();
    friend gaia::db::data_t* gaia::db::get_data();
    friend gaia::db::logs_t* gaia::db::get_logs();
    friend gaia::db::id_index_t* gaia::db::get_id_index();
    friend gaia::db::type_index_t* gaia::db::get_type_index();
    friend gaia::db::txn_metadata_t* gaia::db::get_txn_metadata();
    friend gaia::db::session_metadata_t* gaia::db::get_session_metadata();
    friend gaia::db::memory_manager::memory_manager_t* gaia::db::get_memory_manager();
    friend gaia::db::memory_manager::chunk_manager_t* gaia::db::get_chunk_manager();

public:
    static void run(server_config_t server_conf);

private:
    static inline server_config_t s_server_conf{};

    static inline int s_server_shutdown_eventfd{-1};
    static inline std::atomic<size_t> s_session_count{0};

    static inline mapped_data_t<locators_t> s_shared_locators{};
    static inline mapped_data_t<counters_t> s_shared_counters{};
    static inline mapped_data_t<data_t> s_shared_data{};
    static inline mapped_data_t<logs_t> s_shared_logs{};
    static inline mapped_data_t<id_index_t> s_shared_id_index{};
    static inline mapped_data_t<type_index_t> s_shared_type_index{};
    static inline mapped_data_t<txn_metadata_t> s_shared_txn_metadata{};
    static inline mapped_data_t<session_metadata_t> s_shared_session_metadata{};

private:
    // A list of data mappings that we manage together.
    // The order of declarations must be the order of data_mapping_t::index_t values!
    static inline constexpr data_mapping_t c_data_mappings[] = {
        {data_mapping_t::index_t::locators, &s_shared_locators, c_gaia_mem_locators_prefix},
        {data_mapping_t::index_t::counters, &s_shared_counters, c_gaia_mem_counters_prefix},
        {data_mapping_t::index_t::data, &s_shared_data, c_gaia_mem_data_prefix},
        {data_mapping_t::index_t::logs, &s_shared_logs, c_gaia_mem_logs_prefix},
        {data_mapping_t::index_t::id_index, &s_shared_id_index, c_gaia_mem_id_index_prefix},
        {data_mapping_t::index_t::type_index, &s_shared_type_index, c_gaia_mem_type_index_prefix},
        {data_mapping_t::index_t::txn_metadata, &s_shared_txn_metadata, c_gaia_mem_txn_metadata_prefix},
        {data_mapping_t::index_t::session_metadata, &s_shared_session_metadata, c_gaia_mem_session_metadata_prefix},
    };

    static void clear_server_state();

    static void init_shared_memory();

    static void init_txn_history();

    static sigset_t get_masked_signals();

    static void signal_handler(sigset_t sigset, int& signum);

    static int get_listening_socket(const std::string& socket_name);

    static bool authenticate_client_socket(int socket);

    static bool can_start_session(int socket_fd);

    static void client_dispatch_handler(const std::string& socket_name, int session_listener_epoll_fd);

    static void session_listener_handler(int session_listener_epoll_fd);
};

} // namespace db
} // namespace gaia
