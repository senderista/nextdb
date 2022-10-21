////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include "gaia_internal/db/gaia_ptr_api.hpp"

#include "gaia/common.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/db/gaia_ptr.hpp"
#include "gaia_internal/exceptions.hpp"

#include "db_client.hpp"
#include "db_helpers.hpp"

using namespace gaia::common;
using namespace gaia::db;

namespace gaia
{
namespace db
{

namespace gaia_ptr
{

gaia_ptr_t create(
    common::gaia_type_t type,
    size_t data_size,
    const void* data)
{
    gaia_id_t id = gaia_ptr_t::generate_id();
    return create(id, type, data_size, data);
}

gaia_ptr_t create(
    common::gaia_id_t id,
    common::gaia_type_t type,
    size_t data_size,
    const void* data)
{
    gaia_ptr_t obj = gaia_ptr_t::create_no_txn(id, type, data_size, data);
    obj.finalize_create();
    return obj;
}

void update_payload(gaia_id_t id, size_t data_size, const void* data)
{
    gaia_ptr_t obj = gaia_ptr_t::from_gaia_id(id);
    if (!obj)
    {
        throw invalid_object_id_internal(id);
    }
    obj.update_payload(data_size, data);
}

void remove(gaia_ptr_t& object)
{
    if (!object)
    {
        return;
    }

    object.reset();
}

} // namespace gaia_ptr

} // namespace db
} // namespace gaia
