////////////////////////////////////////////////////
// Copyright (c) Tobin Baker
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <typeinfo>
#include <type_traits>

#include "gaia/common.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/hash.hpp"
#include "gaia_internal/db/gaia_ptr.hpp"

namespace gaia
{
namespace db
{

/**
 * This "smart pointer" class wraps an instance of a user-provided type T,
 * allowing the current version of the instance to be retrieved from the
 * database and a new version to be written to the database.
 *
 * Currently, T is restricted to be a trivial type, so it can be trivially
 * serialized and deserialized. Later, serialization hooks will be provided to
 * support nontrivial types.
 */
template<typename T>
class gaia_var_t
{
    static_assert(std::is_trivial_v<T> == true, "gaia_var_t only accepts trivial types!");

public:
    gaia_var_t() = default;
    inline static gaia_var_t<T> create(const T& value);

    inline T get() const;
    inline const T* get_ptr() const;
    inline void set(const T& value) const;

    inline bool operator==(const gaia_var_t<T>& other) const;
    inline bool operator!=(const gaia_var_t<T>& other) const;
    inline bool operator==(const std::nullptr_t) const;
    inline bool operator!=(const std::nullptr_t) const;
    inline explicit operator bool() const;

private:
    gaia_ptr_t m_ptr{c_invalid_gaia_locator};

private:
    inline explicit gaia_var_t(gaia_ptr_t ptr)
        : m_ptr(ptr)
    {
    }
};


#include "gaia_var.inc"

} // namespace db
} // namespace gaia
