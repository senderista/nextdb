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

void print_node(const gaia_ptr_t& node, bool indent = false)
{
    if (!node)
    {
        return;
    }

    std::cerr << std::endl;

    if (indent)
    {
        std::cerr << "  ";
    }

    std::cerr
        << "Node id:"
        << node.id() << ", type:"
        << node.type();

    print_payload(std::cerr, node.data_size(), node.data());

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
        node1_id = gaia_ptr_t::generate_id();
        node2_id = gaia_ptr_t::generate_id();
        node3_id = gaia_ptr_t::generate_id();
        node4_id = gaia_ptr_t::generate_id();

        begin_transaction();
        {
            type1 = 1;
            type2 = 2;

            std::cerr << std::endl;
            std::cerr << "*** create test nodes" << std::endl;
            gaia_ptr_t node1 = gaia_ptr_t::create(node1_id, type1, 0, nullptr);
            print_node(node1);
            gaia_ptr_t node2 = gaia_ptr_t::create(node2_id, type1, 0, nullptr);
            print_node(node2);
            gaia_ptr_t node3 = gaia_ptr_t::create(node3_id, type2, 0, nullptr);
            print_node(node3);
            gaia_ptr_t node4 = gaia_ptr_t::create(node4_id, type2, 0, nullptr);
            print_node(node4);
        }
        commit_transaction();
    }

protected:
    gaia_id_t node1_id;
    gaia_id_t node2_id;
    gaia_id_t node3_id;
    gaia_id_t node4_id;
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
        gaia_ptr_t node1 = gaia_ptr_t::from_gaia_id(node1_id);
        gaia_ptr_t node2 = gaia_ptr_t::from_gaia_id(node2_id);
        gaia_ptr_t node3 = gaia_ptr_t::from_gaia_id(node3_id);
        gaia_ptr_t node4 = gaia_ptr_t::from_gaia_id(node4_id);
        print_node(node1);
        print_node(node2);
        print_node(node3);
        print_node(node4);
        EXPECT_EQ(node1.id(), node1_id);
        EXPECT_EQ(node2.id(), node2_id);
        EXPECT_EQ(node3.id(), node3_id);
        EXPECT_EQ(node4.id(), node4_id);
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
        gaia_ptr_t node1 = gaia_ptr_t::from_gaia_id(node1_id);
        print_node(node1);
        node1.update_payload(strlen(payload), payload);
        print_node(node1);
        EXPECT_STREQ(node1.data(), payload);
    }
    commit_transaction();

    begin_transaction();
    {
        std::cerr << std::endl;
        std::cerr << "*** Reload data and verify update" << std::endl;
        gaia_ptr_t node1 = gaia_ptr_t::from_gaia_id(node1_id);
        print_node(node1);
        EXPECT_STREQ(node1.data(), payload);
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
        gaia_ptr_t node1 = gaia_ptr_t::from_gaia_id(node1_id);
        print_node(node1);
        node1.update_payload(strlen(payload), payload);
        print_node(node1);
        EXPECT_STREQ(node1.data(), payload);
    }
    rollback_transaction();

    begin_transaction();
    {
        std::cerr << std::endl;
        std::cerr << "*** Reload data and verify update" << std::endl;
        gaia_ptr_t node1 = gaia_ptr_t::from_gaia_id(node1_id);
        print_node(node1);
        EXPECT_EQ(node1.data(), nullptr);
    }
    commit_transaction();
}

constexpr gaia_type_t c_first_iterate_test_type = 4;
constexpr size_t c_buffer_size_exact = c_stream_batch_size;
constexpr size_t c_buffer_size_exact_multiple = c_stream_batch_size * 2;
constexpr size_t c_buffer_size_inexact_multiple = c_stream_batch_size * 2 + 3;
constexpr size_t c_buffer_size_minus_one = c_stream_batch_size - 1;
constexpr size_t c_buffer_size_plus_one = c_stream_batch_size + 1;

void iterate_test_create_nodes()
{
    std::cerr << "*** Creating nodes for cursor test..." << std::endl;

    // Create objects for iterator test.
    //
    // "One node" test.
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
    for (auto node : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(node.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over one node in type:" << std::endl;
    type = c_first_iterate_test_type;
    count = 0;
    expected_count = 1;
    for (auto node : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(node.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over nodes with exact buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_exact;
    for (auto node : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(node.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over nodes with exact multiple of buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_exact_multiple;
    for (auto node : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(node.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over nodes with inexact multiple of buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_inexact_multiple;
    for (auto node : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(node.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over nodes with one less than buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_minus_one;
    for (auto node : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(node.type(), type);
        count++;
    }
    EXPECT_EQ(count, expected_count);

    std::cerr << std::endl;
    std::cerr << "*** Iterating over nodes with one more than buffer size:" << std::endl;
    ++type;
    count = 0;
    expected_count = c_buffer_size_plus_one;
    for (auto node : gaia_ptr_t::find_all_range(type))
    {
        EXPECT_EQ(node.type(), type);
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
        iterate_test_create_nodes();
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
        iterate_test_create_nodes();
        iterate_test_validate_iterations();
    }
    commit_transaction();
}

// TEST_F(db__core__db_client__test, iterate_type_delete)
// {
//     begin_transaction();
//     {
//         std::cerr << std::endl;
//         std::cerr << "*** Iterating over nodes of type 1 before delete:" << std::endl;
//         auto node_iter = gaia_ptr_t::find_all_iterator(type1);
//         print_node(*node_iter);
//         EXPECT_EQ(node_iter->id(), node1_id);
//         std::cerr << std::endl;
//         std::cerr << "*** Preparing to delete first node of type 1:" << std::endl;
//         node_iter->reset();
//         std::cerr << "*** Iterating over nodes of type 1 after delete:" << std::endl;
//         node_iter = gaia_ptr_t::find_all_iterator(type1);
//         print_node(*node_iter);
//         EXPECT_EQ(node_iter->id(), node2_id);
//     }
//     commit_transaction();

//     begin_transaction();
//     {
//         std::cerr << std::endl;
//         std::cerr << "*** Reloading data: iterating over nodes of type 1 after delete:" << std::endl;
//         auto node_iter = gaia_ptr_t::find_all_iterator(type1);
//         print_node(*node_iter);
//         EXPECT_EQ(node_iter->id(), node2_id);
//     }
//     commit_transaction();
// }

TEST_F(db__core__db_client__test, null_payload_check)
{
    begin_transaction();
    {
        constexpr size_t c_test_payload_size = 50;

        std::cerr << std::endl;
        std::cerr << "*** Creating a zero-length node:" << std::endl;
        EXPECT_NE(gaia_ptr_t::create(gaia_ptr_t::generate_id(), type1, 0, nullptr), nullptr);

        std::cerr << std::endl;
        std::cerr << "*** Creating a node with no payload and non-zero payload size (error):" << std::endl;
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
        std::cerr << "*** Creating the largest node (" << payload_size << " bytes):" << std::endl;
        EXPECT_NE(gaia_ptr_t::create(gaia_ptr_t::generate_id(), type1, payload_size, payload), nullptr);

        std::cerr << std::endl;
        std::cerr << "*** Creating a too-large node (" << payload_size + sizeof(gaia_id_t) << " bytes):" << std::endl;
        EXPECT_THROW(gaia_ptr_t::create(gaia_ptr_t::generate_id(), type1, payload_size + 1, payload), object_too_large);
    }
    commit_transaction();
}
