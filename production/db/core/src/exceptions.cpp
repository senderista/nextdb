////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include "gaia_internal/exceptions.hpp"

namespace gaia
{
namespace db
{

server_connection_failed_internal::server_connection_failed_internal(const char* error_message, int error_number)
    : m_error_number(error_number)
{
    std::stringstream message;
    message
        << "Client failed to connect to server! System error: '"
        << error_message << "'.";
    m_message = message.str();
}

int server_connection_failed_internal::get_errno()
{
    return m_error_number;
}

session_exists_internal::session_exists_internal()
{
    m_message = "Close the current session before opening a new one.";
}

no_open_session_internal::no_open_session_internal()
{
    m_message = "Open a session before performing data access.";
}

transaction_in_progress_internal::transaction_in_progress_internal()
{
    m_message = "Commit or rollback the current transaction before opening a new transaction.";
}

no_open_transaction_internal::no_open_transaction_internal()
{
    m_message = "Open a transaction before performing data access.";
}

transaction_update_conflict_internal::transaction_update_conflict_internal()
{
    m_message = "Transaction was aborted due to a conflict with another transaction.";
}

transaction_object_limit_exceeded_internal::transaction_object_limit_exceeded_internal()
{
    m_message = "Transaction attempted to update too many objects.";
}

transaction_log_allocation_failure_internal::transaction_log_allocation_failure_internal()
{
    m_message = "Unable to allocate a log for this transaction.";
}

duplicate_object_id_internal::duplicate_object_id_internal(common::gaia_id_t id)
{
    std::stringstream message;
    message << "An object with the same ID '" << id << "' already exists.";
    m_message = message.str();
}

out_of_memory_internal::out_of_memory_internal()
{
    m_message = "Out of memory.";
}

system_object_limit_exceeded_internal::system_object_limit_exceeded_internal()
{
    m_message = "System object limit exceeded.";
}

invalid_object_id_internal::invalid_object_id_internal(common::gaia_id_t id)
{
    std::stringstream message;
    message << "Cannot find an object with ID '" << id << "'.";
    m_message = message.str();
}

object_too_large_internal::object_too_large_internal(size_t total_len, uint16_t max_len)
{
    std::stringstream message;
    message << "Object size " << total_len << " exceeds maximum size " << max_len << ".";
    m_message = message.str();
}

invalid_object_type_internal::invalid_object_type_internal(common::gaia_type_t type)
{
    std::stringstream message;
    message << "The type '" << type << "' does not exist in the catalog.";
    m_message = message.str();
}

invalid_object_type_internal::invalid_object_type_internal(common::gaia_id_t id, common::gaia_type_t type)
{
    std::stringstream message;
    message
        << "Cannot create object with ID '" << id << "' and type '" << type
        << "'. The type does not exist in the catalog.";
    m_message = message.str();
}

invalid_object_type_internal::invalid_object_type_internal(
    common::gaia_id_t id,
    common::gaia_type_t expected_type,
    const char* expected_typename,
    common::gaia_type_t actual_type)
{
    std::stringstream message;
    message
        << "Requesting Gaia type '" << expected_typename << "'('" << expected_type
        << "'), but object identified by '" << id << "' is of type '" << actual_type << "'.";
    m_message = message.str();
}

type_limit_exceeded_internal::type_limit_exceeded_internal()
{
    m_message = "Registered type limit exceeded.";
}

session_limit_exceeded_internal::session_limit_exceeded_internal()
{
    m_message = "Server session limit exceeded.";
}

memory_allocation_error_internal::memory_allocation_error_internal()
{
    m_message = "The Gaia database ran out of memory.";
}

} // namespace db
} // namespace gaia
