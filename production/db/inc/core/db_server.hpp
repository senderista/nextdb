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
#include <optional>
#include <shared_mutex>
#include <utility>

#include <flatbuffers/flatbuffers.h>

#include "gaia/exception.hpp"

#include "gaia_internal/common/generator_iterator.hpp"

#include "safe_ts.hpp"
#include "server_contexts.hpp"
#include "txn_metadata.hpp"
#include "type_index.hpp"

namespace gaia
{
namespace db
{

class invalid_session_transition : public common::gaia_exception
{
public:
    explicit invalid_session_transition(const std::string& message)
        : gaia_exception(message)
    {
    }
};

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

    inline persistence_mode_t persistence_mode();
    inline const std::string& instance_name();
    inline const std::string& data_dir();
    inline bool is_persistence_enabled();

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
    friend gaia::db::transactions::txn_metadata_t* get_txn_metadata();
    friend gaia::db::watermarks_t* get_watermarks();
    friend gaia::db::safe_ts_entries_t* get_safe_ts_entries();
    friend gaia::db::txn_log_t* gaia::db::get_txn_log();
    friend gaia::db::memory_manager::memory_manager_t* gaia::db::get_memory_manager();
    friend gaia::db::memory_manager::chunk_manager_t* gaia::db::get_chunk_manager();
    friend gaia::db::gaia_txn_id_t gaia::db::get_txn_id();

public:
    static void run(server_config_t server_conf);

private:
    // Context getters.
    static inline gaia_txn_id_t txn_id();
    static inline log_offset_t txn_log_offset();
    static inline std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_logs_for_snapshot();

    static inline int session_socket();
    static inline std::vector<std::thread>& session_owned_threads();
    static inline std::unordered_map<chunk_offset_t, chunk_version_t>& map_gc_chunks_to_versions();

private:
    // We don't use an auto-pointer because its destructor is "non-trivial"
    // and that would add overhead to the TLS implementation.
    thread_local static inline server_session_context_t* s_session_context{nullptr};

    static inline server_config_t s_server_conf{};

    static inline int s_server_shutdown_eventfd = -1;
    static inline int s_listening_socket = -1;

    // These thread objects are owned by the client dispatch thread.
    static inline std::vector<std::thread> s_session_threads{};

    static inline mapped_data_t<locators_t> s_shared_locators{};
    static inline mapped_data_t<counters_t> s_shared_counters{};
    static inline mapped_data_t<data_t> s_shared_data{};
    static inline mapped_data_t<logs_t> s_shared_logs{};
    static inline mapped_data_t<id_index_t> s_shared_id_index{};
    static inline mapped_data_t<type_index_t> s_shared_type_index{};
    static inline mapped_data_t<transactions::txn_metadata_t> s_shared_txn_metadata{};
    static inline mapped_data_t<watermarks_t> s_shared_watermarks{};
    static inline mapped_data_t<safe_ts_entries_t> s_shared_safe_ts_entries{};

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
        {data_mapping_t::index_t::watermarks, &s_shared_watermarks, c_gaia_mem_watermarks_prefix},
        {data_mapping_t::index_t::safe_ts_entries, &s_shared_safe_ts_entries, c_gaia_mem_safe_ts_entries_prefix},
    };

    // Function pointer type that executes side effects of a session state transition.
    // REVIEW: replace void* with std::any?
    typedef void (*transition_handler_fn)(
        messages::session_event_t event,
        const void* event_data,
        messages::session_state_t old_state,
        messages::session_state_t new_state);

    // Session state transition handler functions.
    static void handle_connect(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);
    static void handle_begin_txn(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);
    static void handle_rollback_txn(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);
    static void handle_commit_txn(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);
    static void handle_decide_txn(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);
    static void handle_client_shutdown(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);
    static void handle_server_shutdown(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);
    static void handle_request_stream(messages::session_event_t, const void*, messages::session_state_t, messages::session_state_t);

    struct transition_t
    {
        messages::session_state_t new_state;
        transition_handler_fn handler;
    };

    struct valid_transition_t
    {
        messages::session_state_t state;
        messages::session_event_t event;
        transition_t transition;
    };

    // "Wildcard" transitions (current state = session_state_t::ANY) must be listed after
    // non-wildcard transitions with the same event, or the latter will never be applied.
    static inline constexpr valid_transition_t c_valid_transitions[] = {
        {messages::session_state_t::DISCONNECTED, messages::session_event_t::CONNECT, {messages::session_state_t::CONNECTED, handle_connect}},
        {messages::session_state_t::ANY, messages::session_event_t::CLIENT_SHUTDOWN, {messages::session_state_t::DISCONNECTED, handle_client_shutdown}},
        {messages::session_state_t::CONNECTED, messages::session_event_t::BEGIN_TXN, {messages::session_state_t::TXN_IN_PROGRESS, handle_begin_txn}},
        {messages::session_state_t::TXN_IN_PROGRESS, messages::session_event_t::ROLLBACK_TXN, {messages::session_state_t::CONNECTED, handle_rollback_txn}},
        {messages::session_state_t::TXN_IN_PROGRESS, messages::session_event_t::COMMIT_TXN, {messages::session_state_t::TXN_COMMITTING, handle_commit_txn}},
        {messages::session_state_t::TXN_COMMITTING, messages::session_event_t::DECIDE_TXN_COMMIT, {messages::session_state_t::CONNECTED, handle_decide_txn}},
        {messages::session_state_t::TXN_COMMITTING, messages::session_event_t::DECIDE_TXN_ABORT, {messages::session_state_t::CONNECTED, handle_decide_txn}},
        {messages::session_state_t::ANY, messages::session_event_t::SERVER_SHUTDOWN, {messages::session_state_t::DISCONNECTED, handle_server_shutdown}},
        {messages::session_state_t::ANY, messages::session_event_t::REQUEST_STREAM, {messages::session_state_t::ANY, handle_request_stream}},
    };

    static void apply_transition(messages::session_event_t event, const void* event_data);

    static void build_server_reply_info(
        flatbuffers::FlatBufferBuilder& builder,
        messages::session_event_t event,
        messages::session_state_t old_state,
        messages::session_state_t new_state,
        gaia_txn_id_t txn_id = c_invalid_gaia_txn_id,
        log_offset_t txn_log_offset = c_invalid_log_offset,
        const std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_logs_to_apply = {});

    static void clear_server_state();

    static void init_memory_manager(bool initializing);

    static void init_shared_memory();

    static sigset_t get_masked_signals();

    static void signal_handler(sigset_t sigset, int& signum);

    static void init_listening_socket(const std::string& socket_name);

    static bool authenticate_client_socket(int socket);

    static bool can_start_session(int socket_fd);

    static void client_dispatch_handler(const std::string& socket_name);

    static void session_handler(int session_socket);

    static std::pair<int, int> get_stream_socket_pair();

    template <typename T_element>
    static void stream_producer_handler(
        int stream_socket,
        int cancel_eventfd,
        std::shared_ptr<common::iterators::generator_t<T_element>> generator_fn);

    template <typename T_element>
    static void start_stream_producer(
        int stream_socket,
        std::shared_ptr<common::iterators::generator_t<T_element>> generator);

    static void get_txn_log_offsets_for_snapshot(
        gaia_txn_id_t begin_ts,
        std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_ids_with_log_offsets_for_snapshot);

    static void release_txn_log_offsets_for_snapshot();

    static void release_transaction_resources();

    static void txn_begin();

    static void txn_rollback(bool client_disconnected = false);

    static bool txn_commit();

    static void perform_maintenance();

    static void apply_txn_logs_to_shared_view();

    static void gc_applied_txn_logs();

    static void update_post_gc_watermark();

    static void truncate_txn_table();

    static gaia_txn_id_t submit_txn(gaia_txn_id_t begin_ts, log_offset_t log_offset);

    static bool txn_logs_conflict(log_offset_t offset_1, log_offset_t offset_2);

    static void perform_pre_commit_work_for_txn();

    static bool validate_txn(gaia_txn_id_t commit_ts);

    static void validate_txns_in_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts);

    static void apply_txn_log_from_ts(gaia_txn_id_t commit_ts);

    static void gc_txn_log_from_offset(log_offset_t offset, bool is_committed);

    static void deallocate_txn_log(txn_log_t* txn_log, bool deallocate_new_offsets);

    static void sort_log();

    static void deallocate_object(gaia_offset_t offset);

    static char* get_txn_metadata_page_address_from_ts(gaia_txn_id_t ts);

    static inline bool acquire_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts);

    static inline void release_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts);
};

#include "db_server.inc"

} // namespace db
} // namespace gaia
