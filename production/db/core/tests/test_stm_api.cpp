////////////////////////////////////////////////////
// Copyright (c) Tobin Baker
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include "gaia/db/db.hpp"
#include "gaia/db/gaia_var.hpp"
#include "gaia/exceptions.hpp"

#include "db_test_base.hpp"

using namespace gaia::db;
using namespace gaia::common;

struct Scalar
{
    int x;
    bool operator==(const Scalar&) const = default;
};

struct Double
{
    int x;
    long y;
    bool operator==(const Double&) const = default;
};

struct Triple
{
    int x;
    long y;
    long double z;
    bool operator==(const Triple&) const = default;
};

/**
 * Google test fixture object.  This class is used by each
 * test case below.  SetUp() is called before each test is run
 * and TearDown() is called after each test case is done.
 */
class db__core__stm_api__test : public db_test_base_t
{
private:
    void init_data()
    {
        begin_transaction();
        {
            scalar_var = gaia_var_t<Scalar>::create(scalar_val_1);
            dbl_var = gaia_var_t<Double>::create(dbl_val_1);
            triple_var = gaia_var_t<Triple>::create(triple_val_1);
        }
        commit_transaction();
    }

protected:
    Scalar scalar_val_1{100};
    Double dbl_val_1{100, 200};
    Triple triple_val_1{100, 200, 300};

    Scalar scalar_val_2{101};
    Double dbl_val_2{101, 201};
    Triple triple_val_2{101, 201, 301};

    gaia_var_t<Scalar> scalar_var;
    gaia_var_t<Double> dbl_var;
    gaia_var_t<Triple> triple_var;

    void SetUp() override
    {
        db_test_base_t::SetUp();

        begin_session();
        init_data();
    }

    void TearDown() override
    {
        end_session();

        db_test_base_t::TearDown();
    }
};

TEST_F(db__core__stm_api__test, read_data)
{
    begin_transaction();
    {
        EXPECT_EQ(scalar_var.get(), scalar_val_1);
        EXPECT_EQ(dbl_var.get(), dbl_val_1);
        EXPECT_EQ(triple_var.get(), triple_val_1);
    }
    commit_transaction();
}

TEST_F(db__core__stm_api__test, update_payload)
{
    begin_transaction();
    {
        EXPECT_EQ(scalar_var.get(), scalar_val_1);
        EXPECT_EQ(dbl_var.get(), dbl_val_1);
        EXPECT_EQ(triple_var.get(), triple_val_1);

        scalar_var.set(scalar_val_2);
        dbl_var.set(dbl_val_2);
        triple_var.set(triple_val_2);
    }
    commit_transaction();

    begin_transaction();
    {
        EXPECT_EQ(scalar_var.get(), scalar_val_2);
        EXPECT_EQ(dbl_var.get(), dbl_val_2);
        EXPECT_EQ(triple_var.get(), triple_val_2);
    }
    commit_transaction();
}

TEST_F(db__core__stm_api__test, update_payload_rollback)
{
    begin_transaction();
    {
        EXPECT_EQ(scalar_var.get(), scalar_val_1);
        EXPECT_EQ(dbl_var.get(), dbl_val_1);
        EXPECT_EQ(triple_var.get(), triple_val_1);

        scalar_var.set(scalar_val_2);
        dbl_var.set(dbl_val_2);
        triple_var.set(triple_val_2);
    }
    rollback_transaction();

    begin_transaction();
    {
        EXPECT_EQ(scalar_var.get(), scalar_val_1);
        EXPECT_EQ(dbl_var.get(), dbl_val_1);
        EXPECT_EQ(triple_var.get(), triple_val_1);
    }
    commit_transaction();
}
