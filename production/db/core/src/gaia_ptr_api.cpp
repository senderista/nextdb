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
    const type_metadata_t& metadata = type_registry_t::instance().get(type);
    reference_offset_t references_count = metadata.references_count();

    gaia_ptr_t obj = gaia_ptr_t::create_no_txn(id, type, references_count, data_size, data);

    if (metadata.has_value_linked_relationship())
    {
        db_object_t* obj_ptr = obj.to_ptr();
        auto_connect(
            id,
            type,
            // NOLINTNEXTLINE: cppcoreguidelines-pro-type-const-cast
            const_cast<gaia_id_t*>(obj_ptr->references()),
            reinterpret_cast<const uint8_t*>(obj_ptr->data()));
    }

    obj.finalize_create();

    if (client_t::is_valid_event(type))
    {
        client_t::log_event(triggers::event_type_t::row_insert, type, id, triggers::c_empty_position_list);
    }
    return obj;
}

void update_payload(gaia_id_t id, size_t data_size, const void* data)
{
    gaia_ptr_t obj = gaia_ptr_t::from_gaia_id(id);
    if (!obj)
    {
        throw invalid_object_id_internal(id);
    }
    update_payload(obj, data_size, data);
}

void update_payload(gaia_ptr_t& obj, size_t data_size, const void* data)
{
    db_object_t* old_this = obj.to_ptr();
    gaia_offset_t old_offset = obj.to_offset();

    obj.update_payload_no_txn(data_size, data);
    obj.finalize_update(old_offset);
}

void remove(gaia_ptr_t& object, bool force)
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
