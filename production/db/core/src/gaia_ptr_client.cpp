////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include "gaia/common.hpp"
#include "gaia/exceptions.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/debug_assert.hpp"
#include "gaia_internal/db/gaia_ptr.hpp"
#include "gaia_internal/exceptions.hpp"

#include "db_client.hpp"
#include "db_helpers.hpp"

#ifdef DEBUG
#include "memory_helpers.hpp"
#define WRITE_PROTECT(o) write_protect_allocation_page_for_offset((o))
#else
#define WRITE_PROTECT(o) ((void)0)
#endif

using namespace gaia::common;
using namespace gaia::common::iterators;
using namespace gaia::db;

namespace gaia
{
namespace db
{

/*
 * Client-side implementation of gaia_ptr_t here.
 */

void gaia_ptr_t::reset()
{
    locators_t* locators = get_locators();
    log_txn_operation(m_locator, to_offset(), c_invalid_gaia_offset);

    (*locators)[m_locator] = c_invalid_gaia_offset;
    m_locator = c_invalid_gaia_locator;
}

db_object_t* gaia_ptr_t::to_ptr() const
{
    return locator_to_ptr(m_locator);
}

gaia_offset_t gaia_ptr_t::to_offset() const
{
    return locator_to_offset(m_locator);
}

void gaia_ptr_t::finalize_create()
{
    WRITE_PROTECT(to_offset());
    log_txn_operation(m_locator, c_invalid_gaia_offset, to_offset());
}

void gaia_ptr_t::finalize_update(gaia_offset_t old_offset)
{
    WRITE_PROTECT(to_offset());
    log_txn_operation(m_locator, old_offset, to_offset());
}

void gaia_ptr_t::update_payload(size_t data_size, const void* data)
{
    gaia_offset_t old_offset = to_offset();
    update_payload_no_txn(data_size, data);
    finalize_update(old_offset);
}

gaia_ptr_t gaia_ptr_t::create_no_txn(gaia_id_t id, gaia_type_t type, size_t data_size, const void* data)
{
    if (data_size > c_db_object_max_payload_size)
    {
        throw object_too_large_internal(data_size, c_db_object_max_payload_size);
    }

    // TODO: this constructor allows creating a gaia_ptr_t in an invalid state;
    //  the db_object_t should either be initialized before and passed in
    //  or it should be initialized inside the constructor.
    gaia_locator_t locator = allocate_locator(type);
    // register_locator_for_id() returns false if the ID was already present in
    // the map.
    if (!register_locator_for_id(id, locator))
    {
        throw duplicate_object_id_internal(id);
    }

    DEBUG_ASSERT_INVARIANT(id_to_locator(id) == locator, "Cannot find locator for just-inserted ID!");

    allocate_object(locator, data_size);
    gaia_ptr_t obj(locator);
    db_object_t* obj_ptr = obj.to_ptr();
    obj_ptr->id = id;
    obj_ptr->type = type;
    obj_ptr->payload_size = data_size;
    if (data)
    {
        memcpy(obj_ptr->payload, data, data_size);
    }
    else
    {
        ASSERT_INVARIANT(data_size == 0, "Null payload with non-zero payload size!");
    }

    return obj;
}

void gaia_ptr_t::clone_no_txn()
{
    db_object_t* old_this = to_ptr();
    size_t data_size = old_this->payload_size;
    size_t total_object_size = c_db_object_header_size + data_size;
    allocate_object(m_locator, data_size);
    db_object_t* new_this = to_ptr();
    memcpy(new_this, old_this, total_object_size);
}

void gaia_ptr_t::update_payload_no_txn(size_t data_size, const void* data)
{
    db_object_t* old_this = to_ptr();

    if (data_size > c_db_object_max_payload_size)
    {
        throw object_too_large_internal(data_size, c_db_object_max_payload_size);
    }

    // Updates m_locator to point to the new object.
    allocate_object(m_locator, data_size);

    db_object_t* new_this = to_ptr();

    memcpy(new_this, old_this, c_db_object_header_size);
    new_this->payload_size = data_size;
    memcpy(new_this->payload, data, data_size);
}

} // namespace db
} // namespace gaia
