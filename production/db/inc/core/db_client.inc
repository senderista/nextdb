////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

bool client_t::is_session_open()
{
    return (s_session_context != nullptr && session_socket() != -1);
}

bool client_t::is_transaction_open()
{
    return (s_session_context && s_session_context->txn_context.txn_id.is_valid());
}

gaia_txn_id_t client_t::get_current_txn_id()
{
    return txn_id();
}

void client_t::verify_txn_active()
{
    if (!is_transaction_open())
    {
        throw no_open_transaction_internal();
    }
}

void client_t::verify_no_txn()
{
    if (is_transaction_open())
    {
        throw transaction_in_progress_internal();
    }
}

void client_t::verify_session_active()
{
    if (s_session_context == nullptr || session_socket() == -1)
    {
        throw no_open_session_internal();
    }
}

void client_t::verify_no_session()
{
    if (s_session_context != nullptr && session_socket() != -1)
    {
        throw session_exists_internal();
    }
}

gaia_txn_id_t client_t::txn_id()
{
    return s_session_context->txn_context.txn_id;
}

log_offset_t client_t::txn_log_offset()
{
    return s_session_context->txn_context.txn_log_offset;
}

txn_log_t* client_t::txn_log()
{
    DEBUG_ASSERT_PRECONDITION(
        s_session_context->txn_context.txn_log_offset.is_valid(),
        "Invalid txn log offset!");

    return s_session_context->shared_logs.data()->get_log_from_offset(s_session_context->txn_context.txn_log_offset);
}

chunk_offset_t client_t::chunk_offset()
{
    return s_session_context->chunk_manager.chunk_offset();
}

int client_t::session_socket()
{
    return s_session_context->session_socket;
}

mapped_data_t<locators_t>& client_t::private_locators()
{
    return s_session_context->private_locators;
}

mapped_data_t<locators_t>& client_t::shared_locators()
{
    return s_session_context->shared_locators;
}

mapped_data_t<data_t>& client_t::shared_data()
{
    return s_session_context->shared_data;
}

std::vector<data_mapping_t>& client_t::data_mappings()
{
    return s_session_context->data_mappings;
}

std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& client_t::txn_logs_for_snapshot()
{
    return s_session_context->txn_logs_for_snapshot;
}

std::vector<std::pair<chunk_offset_t, chunk_version_t>>& client_t::map_gc_chunks_to_versions()
{
    return s_session_context->map_gc_chunks_to_versions;
}

gaia_txn_id_t client_t::latest_applied_commit_ts_lower_bound()
{
    return s_session_context->latest_applied_commit_ts_lower_bound;
}

session_id_t client_t::session_id()
{
    return s_session_context->session_id;
}
