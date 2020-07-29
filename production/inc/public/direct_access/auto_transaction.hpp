/////////////////////////////////////////////
// Copyright (c) Gaia Platform LLC
// All rights reserved.
/////////////////////////////////////////////

#pragma once

#include <map>

#include "gaia_db.hpp"

using namespace std;
using namespace gaia::db;

namespace gaia {

/**
 * \addtogroup Gaia
 * @{
 */

namespace direct_access {

/**
 * \addtogroup Direct
 * @{
 * 
 * Provides a RAII object for exception safe handling of transactions.
 * 
 */

/**
 * The auto transaction object begins a transaction on construction and 
 * does a rollback on destruction.  The user must explicitly call commit()
 */
class auto_transaction_t
{
public:
    auto_transaction_t(): auto_transaction_t(true) {}
    auto_transaction_t(bool auto_begin);

    void commit();

    ~auto_transaction_t();
private:
    bool m_auto_begin;
};

/*@}*/
}
/*@}*/
}