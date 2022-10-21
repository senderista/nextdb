////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include "gaia_internal/db/db_types.hpp"

namespace gaia
{
namespace db
{

/**
 * @brief Internal API for getting the begin_ts of the current txn.
 */
gaia_txn_id_t get_current_txn_id();

// The name of the SE server binary.
constexpr char c_db_server_exec_name[] = "gaia_db_server";

// The name of the default gaia instance.
constexpr char c_default_instance_name[] = "gaia_default_instance";

// The default location of the data directory.
constexpr char c_default_data_dir[] = "/var/lib/gaia/db";

// The customer facing name of the DB server.
constexpr char c_db_server_name[] = "Gaia Database Server";

} // namespace db
} // namespace gaia
