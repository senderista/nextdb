////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include "gaia/exceptions.hpp"

namespace gaia
{

namespace db
{

class server_connection_failed_internal : public server_connection_failed
{
public:
    explicit server_connection_failed_internal(const char* error_message, int error_number = 0);

    int get_errno();

protected:
    int m_error_number;
};

class session_exists_internal : public session_exists
{
public:
    session_exists_internal();
};

class no_open_session_internal : public no_open_session
{
public:
    no_open_session_internal();
};

class transaction_in_progress_internal : public transaction_in_progress
{
public:
    transaction_in_progress_internal();
};

class no_open_transaction_internal : public no_open_transaction
{
public:
    no_open_transaction_internal();
};

class transaction_update_conflict_internal : public transaction_update_conflict
{
public:
    transaction_update_conflict_internal();
};

class transaction_object_limit_exceeded_internal : public transaction_object_limit_exceeded
{
public:
    transaction_object_limit_exceeded_internal();
};

class transaction_log_allocation_failure_internal : public transaction_log_allocation_failure
{
public:
    transaction_log_allocation_failure_internal();
};

class duplicate_object_id_internal : public duplicate_object_id
{
public:
    explicit duplicate_object_id_internal(common::gaia_id_t id);
};

class out_of_memory_internal : public out_of_memory
{
public:
    out_of_memory_internal();
};

class system_object_limit_exceeded_internal : public system_object_limit_exceeded
{
public:
    system_object_limit_exceeded_internal();
};

class invalid_object_id_internal : public invalid_object_id
{
public:
    explicit invalid_object_id_internal(common::gaia_id_t id);
};

class object_too_large_internal : public object_too_large
{
public:
    object_too_large_internal(size_t total_len, uint16_t max_len);
};

class invalid_object_type_internal : public invalid_object_type
{
public:
    explicit invalid_object_type_internal(common::gaia_type_t type);

    invalid_object_type_internal(common::gaia_id_t id, common::gaia_type_t type);

    invalid_object_type_internal(
        common::gaia_id_t id,
        common::gaia_type_t expected_type,
        const char* expected_typename,
        common::gaia_type_t actual_type);
};

class type_limit_exceeded_internal : public type_limit_exceeded
{
public:
    type_limit_exceeded_internal();
};

class session_limit_exceeded_internal : public session_limit_exceeded
{
public:
    session_limit_exceeded_internal();
};

class memory_allocation_error_internal : public memory_allocation_error
{
public:
    explicit memory_allocation_error_internal();
};

} // namespace db

} // namespace gaia
