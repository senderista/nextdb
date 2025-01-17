////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <memory>

#include "gaia/common.hpp"

namespace gaia
{
namespace db
{

class gaia_ptr_t;

/*
 * This namespace provides 'gaia_ptr_t' operations that include higher level
 * functionality like referential integrity checks and value-linked relationship
 * auto-connection. The low-level 'gaia_ptr_t' API excludes all usage of catalog
 * and type metadata.
 */
namespace gaia_ptr
{

gaia_ptr_t create(
    common::gaia_type_t type,
    size_t data_size,
    const void* data);

gaia_ptr_t create(
    common::gaia_id_t id,
    common::gaia_type_t type,
    size_t data_size,
    const void* data);

void remove(gaia_ptr_t& object, bool force = false);

void update_payload(common::gaia_id_t id, size_t data_size, const void* data);
void update_payload(gaia_ptr_t& obj, size_t data_size, const void* data);

} // namespace gaia_ptr
} // namespace db
} // namespace gaia
