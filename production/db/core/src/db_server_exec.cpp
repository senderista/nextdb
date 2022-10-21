////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include <cstdlib>

#include <iostream>
#include <limits>
#include <string>

#include "gaia_internal/db/db.hpp"

#include "db_server.hpp"

using namespace gaia::common;
using namespace gaia::db;

static constexpr char c_help_param[] = "--help";
static constexpr char c_persistence_param[] = "--persistence";

static constexpr char c_persistence_enabled_mode[] = "enabled";
static constexpr char c_persistence_disabled_mode[] = "disabled";
static constexpr char c_persistence_disabled_after_recovery_mode[] = "disabled-after-recovery";

static void usage()
{
    std::cerr
        << "OVERVIEW: Gaia Database Server. Used by Gaia applications to store data.\n"
           "USAGE: gaia_db_server [options]\n"
           "\n"
           "OPTIONS:\n"
           "  --persistence <mode>        Specifies the database persistence mode.\n"
           "                              If not specified, the default mode is enabled.\n"
           "                              - <enabled>: Persist data [default].\n"
           "                              - <disabled>: Do not persist any data.\n"
           "                              - <disabled-after-recovery>: Load data from the datastore and\n"
           "                                disable persistence.\n"
        << ".\n"
           "  --help                      Print help information.\n";
}

static server_config_t::persistence_mode_t parse_persistence_mode(std::string persistence_mode)
{
    if (persistence_mode == c_persistence_enabled_mode)
    {
        return server_config_t::persistence_mode_t::e_enabled;
    }
    else if (persistence_mode == c_persistence_disabled_mode)
    {
        return server_config_t::persistence_mode_t::e_disabled;
    }
    else if (persistence_mode == c_persistence_disabled_after_recovery_mode)
    {
        return server_config_t::persistence_mode_t::e_disabled_after_recovery;
    }
    else
    {
        std::cerr
            << "\nUnrecognized persistence mode: '"
            << persistence_mode
            << "'."
            << std::endl;
        usage();
        std::exit(1);
    }
}

static server_config_t process_command_line(int argc, char* argv[])
{
    std::set<std::string> used_params;

    server_config_t::persistence_mode_t persistence_mode{server_config_t::c_default_persistence_mode};
    std::string instance_name;
    std::string data_dir;

    for (int i = 1; i < argc; ++i)
    {
        used_params.insert(argv[i]);
        if (strcmp(argv[i], c_help_param) == 0)
        {
            usage();
            std::exit(0);
        }
        else if (strcmp(argv[i], c_persistence_param) == 0)
        {
            persistence_mode = parse_persistence_mode(argv[++i]);
        }
        else
        {
            std::cerr
                << "\nUnrecognized argument, '"
                << argv[i]
                << "'."
                << std::endl;
            usage();
            std::exit(1);
        }
    }

    std::cerr << "Starting " << c_db_server_name << "..." << std::endl;

    data_dir = c_default_data_dir;

    instance_name = c_default_instance_name;

    if (persistence_mode == server_config_t::persistence_mode_t::e_disabled)
    {
        std::cerr
            << "Persistence is disabled." << std::endl;
    }

    if (persistence_mode == server_config_t::persistence_mode_t::e_disabled_after_recovery)
    {
        std::cerr
            << "Persistence is disabled after recovery." << std::endl;
    }

    std::cerr
        << "Database instance name is '" << instance_name << "'." << std::endl;

    std::cerr
        << "Database directory is '" << data_dir << "'." << std::endl;

    return server_config_t{persistence_mode, instance_name, data_dir};
}

int main(int argc, char* argv[])
{
    auto server_conf = process_command_line(argc, argv);

    gaia::db::server_t::run(server_conf);
}
