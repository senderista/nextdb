/////////////////////////////////////////////
// Copyright (c) Gaia Platform LLC
// All rights reserved.
/////////////////////////////////////////////

#pragma once

#include <cstdlib>
#include <string>
#include <iostream>
#include <memory>
#include <thread>
#include <chrono>

#include "system_error.hpp"
#include "gaia_db.hpp"
#include "gaia_db_internal.hpp"

namespace gaia {
namespace db {

constexpr char const *c_daemonize_command = "daemonize ";

void remove_persistent_store() {
    string cmd = "rm -rf ";
    cmd.append(PERSISTENT_DIRECTORY_PATH);
    cerr << cmd << endl;
    ::system(cmd.c_str());
}

void wait_for_server_init() {
    static constexpr int c_poll_interval_millis = 10;
    // Wait for server to initialize.
    while (true) {
        try {
            begin_session();
        } catch (system_error& ex) {
            if (ex.get_errno() == ECONNREFUSED) {
                std::this_thread::sleep_for(std::chrono::milliseconds(c_poll_interval_millis));
                continue;
            } else {
                throw;
            }
        } catch (...) {
            throw;
        }
        break;
    }
    // This was just a test connection, so disconnect.
    end_session();
}

class db_server_t {
  public:
    void start(const char *db_server_path, bool stop_server = true) {
        set_path(db_server_path);
        
        if (stop_server) {
            stop();
        }

        // Launch SE server in background.
        string cmd = c_daemonize_command + m_server_path;
        cerr << cmd << endl;
        ::system(cmd.c_str());

        // Wait for server to initialize.
        cerr << "Waiting for server to initialize..." << endl;
        wait_for_server_init();
        m_server_started = true;
    }

    void stop() {
        // Try to kill the SE server process.
        // REVIEW: we should be using a proper process library for this, so we can kill by PID.
        string cmd = "pkill -f -KILL ";
        cmd.append(m_server_path.c_str());
        cerr << cmd << endl;
        ::system(cmd.c_str());
    }

    bool server_started() {
        return m_server_started;
    }

    // Add a trailing '/' if not provided.
    static void terminate_path(string &path) {
        if (path.back() != '/') {
            path.append("/");
        }
    }

  private:
    
    void set_path(const char *db_server_path) {
        if (!db_server_path) {
            m_server_path = gaia::db::SE_SERVER_NAME;
        } else {
            m_server_path = db_server_path;
            terminate_path(m_server_path);
            m_server_path.append(gaia::db::SE_SERVER_NAME);
        }
    }

    string m_server_path;
    bool m_server_started = false;
};

}
}