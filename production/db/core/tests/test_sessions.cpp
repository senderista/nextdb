////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include "gaia/exceptions.hpp"

#include "gaia_internal/db/db_test_base.hpp"

using namespace std;
using namespace gaia::db;
using namespace gaia::common;

static constexpr int64_t c_session_sleep_millis = 10;

class session_test : public db_test_base_t
{
public:
    session_test()
        : db_test_base_t(false, false)
    {
    }
};

TEST_F(session_test, starting_multiple_sessions_on_same_thread_fail)
{
    gaia::db::begin_session();
    EXPECT_THROW(gaia::db::begin_session(), session_exists);
    EXPECT_THROW(gaia::db::begin_session(), session_exists);
    EXPECT_THROW(gaia::db::begin_ddl_session(), session_exists);
    gaia::db::end_session();

    gaia::db::begin_ddl_session();
    EXPECT_THROW(gaia::db::begin_ddl_session(), session_exists);
    EXPECT_THROW(gaia::db::begin_ddl_session(), session_exists);
    EXPECT_THROW(gaia::db::begin_session(), session_exists);
    gaia::db::end_session();
}

TEST_F(session_test, concurrent_sessions_succeed)
{
    std::vector<std::thread> session_threads;

    constexpr size_t c_num_concurrent_sessions = 10;

    for (size_t i = 0; i < c_num_concurrent_sessions; ++i)
    {
        session_threads.emplace_back([]() {
            begin_session();
            std::this_thread::sleep_for(std::chrono::milliseconds(c_session_sleep_millis));
            end_session();
        });
    }

    for (auto& thread : session_threads)
    {
        thread.join();
    }
}
