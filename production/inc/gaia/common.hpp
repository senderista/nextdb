////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <cstddef>
#include <cstdint>

#include <limits>
#include <vector>

#include "gaia/int_type.hpp"

// Export all symbols declared in this file.
#pragma GCC visibility push(default)

/**
 * @defgroup gaia gaia
 * Gaia global namespace
 */

/**
 * @defgroup common common
 * @ingroup gaia
 * Common namespace
 */

namespace gaia
{
/**
 * @addtogroup gaia
 * @{
 */
namespace common
{
/**
 * @addtogroup common
 * @{
 */

constexpr char c_empty_string[] = "";
constexpr char c_whitespace_chars[] = " \n\r\t\f\v";
constexpr size_t c_uint64_bit_count = std::numeric_limits<uint64_t>::digits;

/**
 * The type of a Gaia object identifier.
 */
class gaia_id_t : public int_type_t<uint64_t, 0>
{
public:
    // By default, we should initialize to an invalid value.
    constexpr gaia_id_t()
        : int_type_t<uint64_t, 0>()
    {
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr gaia_id_t(uint64_t value)
        : int_type_t<uint64_t, 0>(value)
    {
    }
};

static_assert(
    sizeof(gaia_id_t) == sizeof(gaia_id_t::value_type),
    "gaia_id_t has a different size than its underlying integer type!");

/**
 * The value of an invalid gaia_id_t.
 */
constexpr gaia_id_t c_invalid_gaia_id;

// This assertion ensures that the default type initialization
// matches the value of the invalid constant.
static_assert(
    c_invalid_gaia_id.value() == gaia_id_t::c_default_invalid_value,
    "Invalid c_invalid_gaia_id initialization!");

/**
 * The type of a Gaia type identifier.
 */
class gaia_type_t : public int_type_t<uint32_t, 0>
{
public:
    // By default, we should initialize to an invalid value.
    constexpr gaia_type_t()
        : int_type_t<uint32_t, 0>()
    {
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr gaia_type_t(uint32_t value)
        : int_type_t<uint32_t, 0>(value)
    {
    }
};

static_assert(
    sizeof(gaia_type_t) == sizeof(gaia_type_t::value_type),
    "gaia_type_t has a different size than its underlying integer type!");

/**
 * The value of an invalid gaia_type_t.
 */
constexpr gaia_type_t c_invalid_gaia_type;

// This assertion ensures that the default type initialization
// matches the value of the invalid constant.
static_assert(
    c_invalid_gaia_type.value() == gaia_type_t::c_default_invalid_value,
    "Invalid c_invalid_gaia_type initialization!");

/**
 * Opaque handle to a gaia record.
 */
class gaia_handle_t : public int_type_t<uint32_t, 0>
{
public:
    // By default, we should initialize to an invalid value.
    constexpr gaia_handle_t()
        : int_type_t<uint32_t, 0>()
    {
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr gaia_handle_t(uint32_t value)
        : int_type_t<uint32_t, 0>(value)
    {
    }
};

static_assert(
    sizeof(gaia_handle_t) == sizeof(gaia_handle_t::value_type),
    "gaia_handle_t has a different size than its underlying integer type!");

template <typename T>
constexpr std::underlying_type_t<T> get_enum_value(T val)
{
    return static_cast<std::underlying_type_t<T>>(val);
}

/**@}*/
} // namespace common
/**@}*/
} // namespace gaia

#include "gaia/internal/common_std.inc"

// Restore default hidden visibility for all symbols.
#pragma GCC visibility pop
