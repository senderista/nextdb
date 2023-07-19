////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

static const bool c_is_running_on_server = true;
static const bool c_is_running_on_client = false;

static gaia::db::locators_t* get_locators()
{
    // The shared locator segment should always be mapped whenever any callers
    // of this method are able to observe it.
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_locators.is_set(), "Invalid local snapshot!");
    return gaia::db::server_t::s_shared_locators.data();
}

static gaia::db::counters_t* get_counters()
{
    // Since we don't use this accessor in the server itself, we can assert that
    // it is always non-null (since callers should never be able to see it in
    // its null state, i.e., with the counters segment unmapped).
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_counters.is_set(), "Server counters segment is unmapped!");
    return gaia::db::server_t::s_shared_counters.data();
}

static gaia::db::data_t* get_data()
{
    // Since we don't use this accessor in the server itself, we can assert that
    // it is always non-null (since callers should never be able to see it in
    // its null state, i.e., with the data segment unmapped).
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_data.is_set(), "Server data segment is unmapped!");
    return gaia::db::server_t::s_shared_data.data();
}

static gaia::db::logs_t* get_logs()
{
    // Since we don't use this accessor in the server itself, we can assert that
    // it is always non-null (since callers should never be able to see it in
    // its null state, i.e., with the data segment unmapped).
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_logs.is_set(), "Server logs segment is unmapped!");
    return gaia::db::server_t::s_shared_logs.data();
}

static gaia::db::id_index_t* get_id_index()
{
    // Since we don't use this accessor in the server itself, we can assert that
    // it is always non-null (since callers should never be able to see it in
    // its null state, i.e., with the id_index segment unmapped).
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_id_index.is_set(), "Server id_index segment is unmapped!");
    return gaia::db::server_t::s_shared_id_index.data();
}

static gaia::db::type_index_t* get_type_index()
{
    // Since we don't use this accessor in the server itself, we can assert that
    // it is always non-null (since callers should never be able to see it in
    // its null state, i.e., with the type_index segment unmapped).
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_type_index.is_set(), "Server type_index segment is unmapped!");
    return gaia::db::server_t::s_shared_type_index.data();
}

static gaia::db::transactions::txn_metadata_t* get_txn_metadata()
{
    // The txn_metadata segment should always be mapped whenever any callers
    // of this method are able to observe it.
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_txn_metadata.is_set(), "Server txn_metadata segment is unmapped!");
    return gaia::db::server_t::s_shared_txn_metadata.data();
}

static gaia::db::safe_ts_entries_t* get_safe_ts_entries()
{
    // The safe_ts_entries segment should always be mapped whenever any callers
    // of this method are able to observe it.
    DEBUG_ASSERT_PRECONDITION(gaia::db::server_t::s_shared_safe_ts_entries.is_set(), "Server safe_ts_entries segment is unmapped!");
    return gaia::db::server_t::s_shared_safe_ts_entries.data();
}

// This is currently never used, so we just return nullptr to avoid changing the interface.
static gaia::db::memory_manager::memory_manager_t* get_memory_manager()
{
    return nullptr;
}

// This is currently never used, so we just return nullptr to avoid changing the interface.
static gaia::db::memory_manager::chunk_manager_t* get_chunk_manager()
{
    return nullptr;
}
