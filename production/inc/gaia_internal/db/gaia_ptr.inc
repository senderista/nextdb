////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

bool gaia_ptr_t::operator==(const gaia_ptr_t& other) const
{
    return m_locator == other.m_locator;
}

bool gaia_ptr_t::operator!=(const gaia_ptr_t& other) const
{
    return m_locator != other.m_locator;
}

bool gaia_ptr_t::operator==(const std::nullptr_t) const
{
    return !to_ptr();
}

bool gaia_ptr_t::operator!=(const std::nullptr_t) const
{
    return to_ptr();
}

gaia_ptr_t::operator bool() const
{
    return to_ptr();
}

bool gaia_ptr_t::is_null() const
{
    return !m_locator.is_valid();
}

common::gaia_id_t gaia_ptr_t::id() const
{
    return to_ptr() ? to_ptr()->id : common::c_invalid_gaia_id;
}

char* gaia_ptr_t::data() const
{
    return data_size() ? const_cast<char*>(to_ptr()->data()) : nullptr;
}

size_t gaia_ptr_t::data_size() const
{
    return to_ptr()->data_size();
}

bool gaia_ptr_t::is(common::gaia_type_t type) const
{
    return to_ptr() && (this->type() == type);
}
