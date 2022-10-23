////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>
#include <libexplain/execve.h>
#include <libexplain/fork.h>
#include <libexplain/kill.h>
#include <libexplain/usleep.h>
#include <sys/prctl.h>

#include "gaia_internal/common/system_error.hpp"
#include "gaia_internal/exceptions.hpp"

using namespace gaia::common;
using namespace gaia::db;

// Ensures both the server and the client use the environment variables.
class db_test_base_t : public ::testing::Test
{
protected:
    /// Starts the gaia_db_server passing instance_name and data_dir as
    /// environment variables.
    void start_server()
    {
        m_server_pid = ::fork();

        if (m_server_pid < 0)
        {
            const char* reason = ::explain_fork();
            throw_system_error(reason);
        }
        else if (m_server_pid == 0)
        {
            // Kills the child process (gaia_db_server) after the parent dies (current process).
            // This must be put right after ::fork() and before ::execve().
            // This works well with ctest where each test is run as a separated process.
            if (-1 == ::prctl(PR_SET_PDEATHSIG, SIGKILL))
            {
                throw_system_error("prctl() failed!");
            }

            char* argv[] = {nullptr};
            char* envp[] = {nullptr};
            const char gaia_db_server_path[] = "/build/production/db/core/gaia_db_server";

            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
            if (-1 == ::execve(gaia_db_server_path, argv, envp))
            {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
                const char* reason = ::explain_execve(gaia_db_server_path, argv, envp);
                throw_system_error(reason);
            }
        }
        else
        {
            // We're in the parent, so wait a bit for the DB to start accepting connections.
            // 50ms seems to work well.
            constexpr useconds_t c_wait_usecs = 50 * 1000;
            if (-1 == ::usleep(c_wait_usecs))
            {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
                const char* reason = ::explain_usleep(c_wait_usecs);
                throw_system_error(reason);
            }
        }
    }

    void kill_server()
    {
        if (-1 == ::kill(m_server_pid, SIGKILL))
        {
            const char* reason = ::explain_kill(m_server_pid, SIGKILL);
            gaia::common::throw_system_error(reason);
        }
    }

    void SetUp() override
    {
        start_server();
    }

    void TearDown() override
    {
        kill_server();
    }

private:
    pid_t m_server_pid;
};
