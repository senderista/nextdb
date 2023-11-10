////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include "gaia/db/db.hpp"
#include "gaia/exceptions.hpp"

#include "gaia_internal/db/db.hpp"
#include "gaia_internal/db/gaia_ptr.hpp"

#include "db_test_base.hpp"

using namespace gaia::db;
using namespace gaia::common;

// NOTE: This test is used to test lower level DB client and 'gaia_ptr_t' APIs.

void print_payload(std::ostream& o, size_t size, const char* payload)
{
    if (size)
    {
        o << " Payload: ";
    }

    for (size_t i = 0; i < size; i++)
    {
        if ('\\' == payload[i])
        {
            o << "\\\\";
        }
        else if (isprint(payload[i]))
        {
            o << payload[i];
        }
        else
        {
            o << "\\x" << std::setw(2) << std::setfill('0') << std::hex << short(payload[i]) << std::dec;
        }
    }
}

void print_item(const gaia_ptr_t& item, bool indent = false)
{
    if (!item)
    {
        return;
    }

    std::cerr << std::endl;

    if (indent)
    {
        std::cerr << "  ";
    }

    std::cerr
        << "item id:"
        << item.id() << ", type:"
        << item.type();

    print_payload(std::cerr, item.data_size(), item.data());

    if (indent)
    {
        return;
    }

    std::cerr << std::endl;
}

/**
 * Google test fixture object.  This class is used by each
 * test case below.  SetUp() is called before each test is run
 * and TearDown() is called after each test case is done.
 */
class db__core__db_client__test : public db_test_base_t
{
private:
    void init_data()
    {
        item1_id = gaia_ptr_t::generate_id();
        item2_id = gaia_ptr_t::generate_id();
        item3_id = gaia_ptr_t::generate_id();
        item4_id = gaia_ptr_t::generate_id();

        begin_transaction();
        {
            type1 = 1;
            type2 = 2;

            std::cerr << std::endl;
            std::cerr << "*** create test items" << std::endl;
            gaia_ptr_t item1 = gaia_ptr_t::create(item1_id, type1, 0, nullptr);
            print_item(item1);
            gaia_ptr_t item2 = gaia_ptr_t::create(item2_id, type1, 0, nullptr);
            print_item(item2);
            gaia_ptr_t item3 = gaia_ptr_t::create(item3_id, type2, 0, nullptr);
            print_item(item3);
            gaia_ptr_t item4 = gaia_ptr_t::create(item4_id, type2, 0, nullptr);
            print_item(item4);
        }
        commit_transaction();
    }

protected:
    gaia_id_t item1_id;
    gaia_id_t item2_id;
    gaia_id_t item3_id;
    gaia_id_t item4_id;
    gaia_type_t type1;
    gaia_type_t type2;

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

TEST_F(db__core__db_client__test, early_session_termination)
{
    // Test that closing the session after starting a transaction
    // does not generate any internal assertion failures
    // when attempting to reopen a session.
    begin_transaction();
    EXPECT_THROW(end_session(), transaction_in_progress);
    EXPECT_THROW(begin_session(), session_exists);
    rollback_transaction();
}

TEST_F(db__core__db_client__test, read_data)
{
    begin_transaction();
    {
        std::cerr << std::endl;
        std::cerr << "*** Update payload and verify" << std::endl;
        gaia_ptr_t item1 = gaia_ptr_t::from_gaia_id(item1_id);
        gaia_ptr_t item2 = gaia_ptr_t::from_gaia_id(item2_id);
        gaia_ptr_t item3 = gaia_ptr_t::from_gaia_id(item3_id);
        gaia_ptr_t item4 = gaia_ptr_t::from_gaia_id(item4_id);
        print_item(item1);
        print_item(item2);
        print_item(item3);
        print_item(item4);
        EXPECT_EQ(item1.id(), item1_id);
        EXPECT_EQ(item2.id(), item2_id);
        EXPECT_EQ(item3.id(), item3_id);
        EXPECT_EQ(item4.id(), item4_id);
    }
    commit_transaction();
}

TEST_F(db__core__db_client__test, update_payload)
{
    auto payload = "payload str";
    begin_transaction();
    {
        std::cerr << std::endl;
        std::cerr << "*** Update payload and verify" << std::endl;
        gaia_ptr_t item1 = gaia_ptr_t::from_gaia_id(item1_id);
        print_item(item1);
        item1.update_payload(strlen(payload), payload);
        print_item(item1);
        EXPECT_STREQ(item1.data(), payload);
    }
    commit_transaction();

    begin_transaction();
    {
        std::cerr << std::endl;
        std::cerr << "*** Reload data and verify update" << std::endl;
        gaia_ptr_t item1 = gaia_ptr_t::from_gaia_id(item1_id);
        print_item(item1);
        EXPECT_STREQ(item1.data(), payload);
    }
    commit_transaction();
}

TEST_F(db__core__db_client__test, update_payload_rollback)
{
    auto payload = "payload str";
    begin_transaction();
    {
        std::cerr << std::endl;
        std::cerr << "*** Update payload and verify" << std::endl;
        gaia_ptr_t item1 = gaia_ptr_t::from_gaia_id(item1_id);
        print_item(item1);
        item1.update_payload(strlen(payload), payload);
        print_item(item1);
        EXPECT_STREQ(item1.data(), payload);
    }
    rollback_transaction();

    begin_transaction();
    {
        std::cerr << std::endl;
        std::cerr << "*** Reload data and verify update" << std::endl;
        gaia_ptr_t item1 = gaia_ptr_t::from_gaia_id(item1_id);
        print_item(item1);
        EXPECT_EQ(item1.data(), nullptr);
    }
    commit_transaction();
}

constexpr gaia_type_t c_first_iterate_test_type = 4;
constexpr size_t c_buffer_size_exact = c_stream_batch_size;
constexpr size_t c_buffer_size_exact_multiple = c_stream_batch_size * 2;
constexpr size_t c_buffer_size_inexact_multiple = c_stream_batch_size * 2 + 3;
constexpr size_t c_buffer_size_minus_one = c_stream_batch_size - 1;
constexpr size_t c_buffer_size_plus_one = c_stream_batch_size + 1;

void iterate_test_create_items()
{
    std::cerr << "*** Creating items for cursor test..." << std::endl;

    // Create objects for iterator test.
    //
    // "One item" test.
    gaia_type_t next_type = c_first_iterate_test_type;
    gaia_ptr_t::create(gaia_ptr_t::generate_id(), next_type, 0, nullptr);

    // "Exact buffer size" test.
    ++next_type;
    for (size_t i = 0; i < c_buffer_size_exact; i++)
    {
        gaia_ptr_t::create(gaia_ptr_t::generate_id(), next_type, 0, nullptr);
    }

    // "Exact multiple of buffer size" test
    ++next_type;
    for (size_t i = 0; i < c_buffer_size_exact_multiple; i++)
    {
        gaia_ptr_t::create(gaia_ptr_t::generate_id(), next_type, 0, nullptr);
    }

    // "Inexact multiple of buffer size" test
    ++next_type;
    for (size_t i = 0; i < c_buffer_size_inexact_multiple; i++)
    {
        gaia_ptr_t::create(gaia_ptr_t::generate_id(), next_type, 0, nullptr);
    }

    // "One less than buffer size" test
    ++next_type;
    for (size_t i = 0; i < c_buffer_size_minus_one; i++)
    {
        gaia_ptr_t::create(gaia_ptr_t::generate_id(), next_type, 0, nullptr);
    }

    // "One more than buffer size" test
    ++next_type;
    for (size_t i = 0; i < c_buffer_size_plus_one; i++)
    {
        gaia_ptr_t::create(gaia_ptr_t::generate_id(), next_type, 0, nullptr);
    }
}

void iterate_test_validate_iterations()
{
    size_t count, expected_count;

    std::cerr << std::endl;

    std::cerr << "*** Iterating over empty type:" << std::endl;
    gaia_type_t type = c_first_iterate_test_type - 1;
    count = 0;
    expected_count = 0;
    for (auto item : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(item.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over one item in type:" << std::endl;
    type = c_first_iterate_test_type;
    count = 0;
    expected_count = 1;
    for (auto item : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(item.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over items with exact buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_exact;
    for (auto item : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(item.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over items with exact multiple of buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_exact_multiple;
    for (auto item : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(item.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over items with inexact multiple of buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_inexact_multiple;
    for (auto item : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(item.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over items with one less than buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_minus_one;
    for (auto item : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(item.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over items with one more than buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_plus_one;
    for (auto item : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(item.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
}

TEST_F(db__core__db_client__test, iterate_type_cursor_separate_txn)
{
    // Test that we can see additions across transactions.
    begin_transaction();
    {
        iterate_test_create_items();
    }
    commit_transaction();

    begin_transaction();
    {
        iterate_test_validate_iterations();
    }
    commit_transaction();
}

TEST_F(db__core__db_client__test, iterate_type_cursor_same_txn)
{
    // Test that we can see additions in the transaction that made them.
    begin_transaction();
    {
        iterate_test_create_items();
        iterate_test_validate_iterations();
    }
    commit_transaction();
}

// TEST_F(db__core__db_client__test, iterate_type_delete)
// {
//     begin_transaction();
//     {
//         std::cerr << std::endl;
//         std::cerr << "*** Iterating over items of type 1 before delete:" << std::endl;
//         auto item_iter = gaia_ptr_t::find_all_iterator(type1);
//         print_item(*item_iter);
//         EXPECT_EQ(item_iter->id(), item1_id);
//         std::cerr << std::endl;
//         std::cerr << "*** Preparing to delete first item of type 1:" << std::endl;
//         item_iter->reset();
//         std::cerr << "*** Iterating over items of type 1 after delete:" << std::endl;
//         item_iter = gaia_ptr_t::find_all_iterator(type1);
//         print_item(*item_iter);
//         EXPECT_EQ(item_iter->id(), item2_id);
//     }
//     commit_transaction();

//     begin_transaction();
//     {
//         std::cerr << std::endl;
//         std::cerr << "*** Reloading data: iterating over items of type 1 after delete:" << std::endl;
//         auto item_iter = gaia_ptr_t::find_all_iterator(type1);
//         print_item(*item_iter);
//         EXPECT_EQ(item_iter->id(), item2_id);
//     }
//     commit_transaction();
// }

TEST_F(db__core__db_client__test, null_payload_check)
{
    begin_transaction();
    {
        constexpr size_t c_test_payload_size = 50;

        std::cerr << std::endl;
        std::cerr << "*** Creating a zero-length item:" << std::endl;
        EXPECT_NE(gaia_ptr_t::create(gaia_ptr_t::generate_id(), type1, 0, nullptr), nullptr);

        std::cerr << std::endl;
        std::cerr << "*** Creating a item with no payload and non-zero payload size (error):" << std::endl;
        EXPECT_THROW(gaia_ptr_t::create(gaia_ptr_t::generate_id(), type1, c_test_payload_size, nullptr), assertion_failure);
    }
    commit_transaction();
}

TEST_F(db__core__db_client__test, create_large_object)
{
    begin_transaction();
    {
        uint8_t payload[c_db_object_max_payload_size];

        size_t payload_size = sizeof(payload);
        std::cerr << std::endl;
        std::cerr << "*** Creating the largest item (" << payload_size << " bytes):" << std::endl;
        EXPECT_NE(gaia_ptr_t::create(gaia_ptr_t::generate_id(), type1, payload_size, payload), nullptr);

        std::cerr << std::endl;
        std::cerr << "*** Creating a too-large item (" << payload_size + sizeof(gaia_id_t) << " bytes):" << std::endl;
        EXPECT_THROW(gaia_ptr_t::create(gaia_ptr_t::generate_id(), type1, payload_size + 1, payload), object_too_large);
    }
    commit_transaction();
}
