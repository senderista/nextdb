////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <cstring>

#include <sstream>

#include "gaia/common.hpp"
#include "gaia/exception.hpp"

// Export all symbols declared in this file.
#pragma GCC visibility push(default)

/**
 * @defgroup catalog catalog
 * @ingroup gaia
 * Catalog namespace
 */

/**
 * @defgroup index index
 * @ingroup db
 * Index namespace
 */

namespace gaia
{
/**
 * @addtogroup gaia
 * @{
 */

namespace db
{
/**
 * @addtogroup db
 * @{
 */

/**
 * @brief Client failed to connect to the server.
 *
 * Server needs to be running.
 */
class server_connection_failed : public common::gaia_exception
{
};

/**
 * @brief A session already exists on this thread.
 *
 * Only one session at a time can exist on a thread.
 */
class session_exists : public common::gaia_exception
{
};

/**
 * @brief No session exists on this thread.
 *
 * A transaction can only be opened from a thread with an open session.
 */
class no_open_session : public common::gaia_exception
{
};

/**
 * @brief A transaction is already in progress in this session.
 *
 * Only one transaction at a time can exist within a session.
 */
class transaction_in_progress : public common::gaia_exception
{
};

/**
 * @brief No transaction is open in this session.
 *
 * Data can only be accessed from an open transaction.
 */
class no_open_transaction : public common::gaia_exception
{
};

/**
 * @brief The transaction conflicts with another transaction.
 *
 * If two transactions modify the same data at the same time, one of them must abort.
 */
class transaction_update_conflict : public common::gaia_exception
{
};

/**
 * @brief The transaction tried to update too many objects.
 *
 * A transaction can create, update, or delete at most 2^16 objects.
 */
class transaction_object_limit_exceeded : public common::gaia_exception
{
};

/**
 * @brief Unable to allocate a log for this transaction.
 *
 * The system can allocate at most 2^16 transaction logs at one time.
 */
class transaction_log_allocation_failure : public common::gaia_exception
{
};

/**
 * @brief The transaction tried to create an object with an existing ID.
 *
 * A transaction must create a new object using an ID that has not been assigned to another object.
 */
class duplicate_object_id : public common::gaia_exception
{
};

/**
 * @brief The transaction tried to create more objects than fit into memory.
 *
 * The memory used to store objects cannot exceed the configured physical memory limit.
 */
class out_of_memory : public common::gaia_exception
{
};

/**
 * @brief The transaction tried to create more objects than are permitted in the system.
 *
 * The system cannot contain more than 2^32 objects.
 */
class system_object_limit_exceeded : public common::gaia_exception
{
};

/**
 * @brief The transaction referenced an object ID that does not exist.
 *
 * An object can only reference existing objects.
 */
class invalid_object_id : public common::gaia_exception
{
};

/**
 * @brief The transaction tried to create or update an object that is too large.
 *
 * An object cannot be larger than 64 KB.
 */
class object_too_large : public common::gaia_exception
{
};

/**
 * @brief The transaction referenced an object type that does not exist
 * or that does not match the expected type.
 *
 * An object's type must exist in the catalog or must match the expected object type.
 */
class invalid_object_type : public common::gaia_exception
{
};

/**
 * @brief The registered type limit has been exceeded.
 */
class type_limit_exceeded : public common::gaia_exception
{
};

/**
 * @brief The Gaia server session limit has been exceeded.
 */
class session_limit_exceeded : public common::gaia_exception
{
};

/**
 * @brief Failed to allocate more memory.
 */
class memory_allocation_error : public common::gaia_exception
{
};

/**@}*/
} // namespace db

/**@}*/
} // namespace gaia

// Restore default hidden visibility for all symbols.
#pragma GCC visibility pop
