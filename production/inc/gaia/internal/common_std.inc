////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

namespace std
{

// This enables gaia_id_t to be hashed and used as a key in maps.
template <>
struct hash<gaia::common::gaia_id_t>
{
    size_t operator()(const gaia::common::gaia_id_t& gaia_id) const noexcept
    {
        return std::hash<gaia::common::gaia_id_t::value_type>()(gaia_id.value());
    }
};

// This enables gaia_type_t to be hashed and used as a key in maps.
template <>
struct hash<gaia::common::gaia_type_t>
{
    size_t operator()(const gaia::common::gaia_type_t& gaia_type) const noexcept
    {
        return std::hash<gaia::common::gaia_type_t::value_type>()(gaia_type.value());
    }
};

// This enables gaia_handle_t to be hashed and used as a key in maps.
template <>
struct hash<gaia::common::gaia_handle_t>
{
    size_t operator()(const gaia::common::gaia_handle_t& gaia_handle) const noexcept
    {
        return std::hash<gaia::common::gaia_handle_t::value_type>()(gaia_handle.value());
    }
};

} // namespace std
