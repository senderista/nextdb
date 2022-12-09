////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

// Returns a pointer to a mapping of the "locators" shared memory segment.
static inline gaia::db::locators_t* get_locators();

// Returns a pointer to a mapping of the "counters" shared memory segment.
static inline gaia::db::counters_t* get_counters();

// Returns a pointer to a mapping of the "data" shared memory segment.
static inline gaia::db::data_t* get_data();

// Returns a pointer to a mapping of the "logs" shared memory segment.
static inline gaia::db::logs_t* get_logs();

// Returns a pointer to a mapping of the "id_index" shared memory segment.
static inline gaia::db::id_index_t* get_id_index();

// Returns a pointer to a mapping of the "type_index" shared memory segment.
static inline gaia::db::type_index_t* get_type_index();

// Returns a pointer to a mapping of the "txn_metadata" shared memory segment.
static inline gaia::db::transactions::txn_metadata_t* get_txn_metadata();

// Returns a pointer to a mapping of the "watermarks" shared memory segment.
static inline gaia::db::watermarks_t* get_watermarks();

// Returns a pointer to a mapping of the "safe_ts_entries" shared memory segment.
static inline gaia::db::safe_ts_entries_t* get_safe_ts_entries();

// Gets the memory manager instance for the current thread or process.
static inline gaia::db::memory_manager::memory_manager_t* get_memory_manager();

// Gets the chunk manager instance for the current thread or process.
static inline gaia::db::memory_manager::chunk_manager_t* get_chunk_manager();

// Gets the transaction log for the current session thread.
static inline gaia::db::txn_log_t* get_txn_log();

// Gets the transaction ID for the current session thread.
static inline gaia::db::gaia_txn_id_t get_txn_id();
