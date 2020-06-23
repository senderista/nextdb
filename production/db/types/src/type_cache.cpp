/////////////////////////////////////////////
// Copyright (c) Gaia Platform LLC
// All rights reserved.
/////////////////////////////////////////////

#include <type_cache.hpp>

#include <retail_assert.hpp>

using namespace std;
using namespace gaia::common;
using namespace gaia::db::types;

type_cache_t type_cache_t::s_type_cache;

const reflection::Field* field_cache_t::get_field(uint16_t field_id) const
{
    field_map_t::const_iterator iterator = m_field_map.find(field_id);
    return (iterator == m_field_map.end()) ? nullptr : iterator->second;
}

void field_cache_t::set_field(uint16_t field_id, const reflection::Field* field)
{
    retail_assert(field != nullptr, "field_cache_t::set_field() should not be called with a null field value!");

    m_field_map.insert(make_pair(field_id, field));
}

size_t field_cache_t::size()
{
    return m_field_map.size();
}

type_cache_t* type_cache_t::get_type_cache()
{
    return &s_type_cache;
}

void type_cache_t::get_field_cache(uint64_t type_id, auto_field_cache_t& auto_field_cache)
{
    // We keep a shared lock while the field_cache is in use,
    // to ensure that its information is not being updated by another thread.
    m_lock.lock_shared();

    type_map_t::const_iterator iterator = m_type_map.find(type_id);

    if (iterator == m_type_map.end())
    {
        m_lock.unlock_shared();
    }
    else
    {
        auto_field_cache.set(iterator->second);
    }
}

bool type_cache_t::remove_field_cache(uint64_t type_id)
{
    bool removed_field_cache = false;

    auto_lock_t auto_lock(m_lock);

    type_map_t::const_iterator iterator = m_type_map.find(type_id);
    if (iterator != m_type_map.end())
    {
        const field_cache_t* field_cache = iterator->second;
        m_type_map.erase(iterator);
        delete field_cache;
        removed_field_cache = true;
    }

    return removed_field_cache;
}

bool type_cache_t::set_field_cache(uint64_t type_id, const field_cache_t* field_cache)
{
    retail_assert(field_cache != nullptr, "type_cache_t::set_field_cache() should not be called with a null cache!");

    bool inserted_field_cache = false;

    auto_lock_t auto_lock(m_lock);

    type_map_t::const_iterator iterator = m_type_map.find(type_id);
    if (iterator == m_type_map.end())
    {
        m_type_map.insert(make_pair(type_id, field_cache));
        inserted_field_cache = true;
    }

    return inserted_field_cache;
}

size_t type_cache_t::size()
{
    return m_type_map.size();
}

auto_field_cache_t::auto_field_cache_t()
{
    m_field_cache = nullptr;
}

auto_field_cache_t::~auto_field_cache_t()
{
    if (m_field_cache != nullptr)
    {
        type_cache_t::get_type_cache()->m_lock.unlock_shared();
    }
}

const field_cache_t* auto_field_cache_t::get()
{
    return m_field_cache;
}

void auto_field_cache_t::set(const field_cache_t* field_cache)
{
    retail_assert(
        m_field_cache == nullptr,
        "auto_field_cache_t::set() was called on an already set instance!");

    m_field_cache = field_cache;
}
