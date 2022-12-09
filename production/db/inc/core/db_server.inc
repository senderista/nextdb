////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

server_config_t::persistence_mode_t server_config_t::persistence_mode()
{
    return m_persistence_mode;
}

bool server_config_t::is_persistence_enabled()
{
    return (m_persistence_mode != persistence_mode_t::e_disabled)
        && (m_persistence_mode != persistence_mode_t::e_disabled_after_recovery);
}

const std::string& server_config_t::data_dir()
{
    return m_data_dir;
}

const std::string& server_config_t::instance_name()
{
    return m_instance_name;
}

gaia_txn_id_t server_t::txn_id()
{
    return s_session_context->txn_context->txn_id;
}

log_offset_t server_t::txn_log_offset()
{
    return s_session_context->txn_context->txn_log_offset;
}

std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& server_t::txn_logs_for_snapshot()
{
    return s_session_context->txn_context->txn_logs_for_snapshot;
}

int server_t::session_socket()
{
    return s_session_context->session_socket;
}

std::vector<std::thread>& server_t::session_owned_threads()
{
    return s_session_context->session_owned_threads;
}

std::unordered_map<chunk_offset_t, chunk_version_t>& server_t::map_gc_chunks_to_versions()
{
    return s_session_context->map_gc_chunks_to_versions;
}
