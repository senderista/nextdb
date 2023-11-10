////////////////////////////////////////////////////
// Copyright (c) Tobin Baker
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

template<typename T>
gaia_var_t<T> gaia_var_t<T>::create(const T& value)
{
    static_assert(
        sizeof(::gaia::db::gaia_var_t<T>) == sizeof(gaia_ptr_t),
        "gaia_var_t shouldn't contain more than a gaia_ptr_t value!");

    const char* type_name = typeid(T).name();
    auto ptr = gaia_ptr_t::create(
        gaia_ptr_t::generate_id(),
        common::gaia_type_t(
            common::hash::murmur3_32(
                type_name, ::strlen(type_name))),
        sizeof(T),
        &value);
    return gaia_var_t(ptr);
}

template<typename T>
const T* gaia_var_t<T>::get_ptr() const
{
    ASSERT_INVARIANT(m_ptr, "gaia_ptr must be set!");
    ASSERT_INVARIANT(m_ptr.data_size() == sizeof(T), "gaia_ptr payload must have size of wrapped type!");
    return reinterpret_cast<const T*>(m_ptr.data());
}

template<typename T>
T gaia_var_t<T>::get() const
{
    return *(get_ptr());
}

template<typename T>
void gaia_var_t<T>::set(const T& value) const
{
    ASSERT_INVARIANT(m_ptr, "gaia_ptr must be set!");
    m_ptr.update_payload(sizeof(T), &value);
}

template<typename T>
bool gaia_var_t<T>::operator==(const gaia_var_t<T>& other) const
{
    return m_ptr == other.m_ptr;
}

template<typename T>
bool gaia_var_t<T>::operator!=(const gaia_var_t<T>& other) const
{
    return m_ptr != other.m_ptr;
}

template<typename T>
bool gaia_var_t<T>::operator==(const std::nullptr_t) const
{
    return !m_ptr;
}

template<typename T>
bool gaia_var_t<T>::operator!=(const std::nullptr_t) const
{
    return !!m_ptr;
}

template<typename T>
gaia_var_t<T>::operator bool() const
{
    return !!m_ptr;
}
