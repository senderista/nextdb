////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

client_transaction_context_t::~client_transaction_context_t()
{
    clear();
}

client_session_context_t::~client_session_context_t()
{
    clear();
}

void client_transaction_context_t::clear()
{
    txn_id = c_invalid_gaia_txn_id;
    txn_log_offset = c_invalid_log_offset;
}

bool client_transaction_context_t::initialized()
{
    return txn_id.is_valid() && txn_log_offset.is_valid();
}
