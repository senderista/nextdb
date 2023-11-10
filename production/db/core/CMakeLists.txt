###################################################
# Copyright (c) Gaia Platform Authors
#
# Use of this source code is governed by the MIT
# license that can be found in the LICENSE.txt file
# or at https://opensource.org/licenses/MIT.
###################################################

project(core)

set(GAIA_DB_CORE_PUBLIC_INCLUDES
  "${GAIA_INC}"
)

message(VERBOSE "GAIA_DB_CORE_PUBLIC_INCLUDES=${GAIA_DB_CORE_PUBLIC_INCLUDES}")

set(GAIA_DB_CORE_PRIVATE_INCLUDES
  "${GAIA_DB_INC}/core"
  "${GAIA_DB_INC}/index"
  "${GAIA_DB_INC}/memory_manager"
  "${PROJECT_SOURCE_DIR}/inc"
)

message(VERBOSE "GAIA_DB_CORE_PRIVATE_INCLUDES=${GAIA_DB_CORE_PRIVATE_INCLUDES}")

###############################################
# Database-related code begins here.
###############################################
add_library(gaia_db_client STATIC
  src/chunk_manager.cpp
  src/client_contexts.cpp
  src/db_client_api.cpp
  src/db_client.cpp
  src/exceptions.cpp
  src/gaia_ptr.cpp
  src/memory_manager.cpp
)

configure_gaia_target(gaia_db_client)
target_include_directories(gaia_db_client PUBLIC ${GAIA_DB_CORE_PUBLIC_INCLUDES})
target_include_directories(gaia_db_client PRIVATE ${GAIA_DB_CORE_PRIVATE_INCLUDES})
target_link_libraries(gaia_db_client PUBLIC gaia_common)
target_link_libraries(gaia_db_client PRIVATE ${LIB_EXPLAIN} ${LIB_CAP} dl)

# Suppress spurious warnings about zero-initialized structs.
target_compile_options(gaia_db_client PRIVATE -Wno-missing-field-initializers)

# libstdc++ apparently needs libatomic for struct last_allocation_metadata_t, even though it's 8 bytes.
target_link_libraries(gaia_db_client PRIVATE atomic)

# Liburing
# Choose static version of the library.
message(CHECK_START "Looking for liburing")
find_path(LIBURING_INCLUDE_DIR NAMES liburing.h)
find_library(LIBURING_LIBRARY NAMES liburing.a)
if(LIBURING_LIBRARY)
  message(CHECK_PASS "found")
else()
  message(CHECK_FAIL "not found")
  message(FATAL_ERROR "liburing is required.")
endif()

set(GAIA_DB_SERVER_SOURCES
  src/chunk_manager.cpp
  src/db_server.cpp
  src/exceptions.cpp
  src/gaia_ptr.cpp
  src/memory_manager.cpp
)

add_library(gaia_db_server ${GAIA_DB_SERVER_SOURCES})
# Add GAIA_DB_SERVER preprocessor definition for conditional includes.
target_compile_definitions(gaia_db_server PUBLIC GAIA_DB_SERVER=1)
configure_gaia_target(gaia_db_server)
target_include_directories(gaia_db_server PUBLIC
  "${GAIA_DB_CORE_PUBLIC_INCLUDES}"
  "${GAIA_DB_CORE_PRIVATE_INCLUDES}")

# libstdc++ apparently needs libatomic for struct last_allocation_metadata_t, even though it's 8 bytes.
target_link_libraries(gaia_db_server PRIVATE atomic)

target_link_libraries(gaia_db_server
  PUBLIC
    ${LIB_CAP}
    ${LIB_EXPLAIN}
    Threads::Threads
    dl
    gaia_common
)

add_executable(gaia_db_server_exec src/db_server_exec.cpp)
configure_gaia_target(gaia_db_server_exec)
set_target_properties(gaia_db_server_exec PROPERTIES OUTPUT_NAME gaia_db_server)
target_include_directories(gaia_db_server_exec PRIVATE
  "${GAIA_DB_CORE_PUBLIC_INCLUDES}"
  "${GAIA_DB_CORE_PRIVATE_INCLUDES}"
  "${GEN_DIR}")
target_link_libraries(gaia_db_server_exec PRIVATE gaia_db_server)
if(ENABLE_STACKTRACE)
  target_link_libraries(gaia_db_server_exec PRIVATE gaia_stack_trace)
endif()
install(TARGETS gaia_db_server_exec DESTINATION ${CMAKE_INSTALL_BINDIR})

set(GAIA_DB_CORE_TEST_INCLUDES
  ${GAIA_DB_CORE_PUBLIC_INCLUDES}
  ${GAIA_DB_CORE_PRIVATE_INCLUDES}
)

add_gtest(test_type_index tests/test_type_index.cpp "${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_common;gaia_db_client;${LIB_CAP};${LIB_EXPLAIN}")
add_gtest(test_db_client tests/test_db_client.cpp "${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_db_client")
add_gtest(test_stm_api tests/test_stm_api.cpp "${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_db_client")
# test_concurrent_db_client has its own main() function, so we pass HAS_MAIN=TRUE to add_gtest().
add_gtest(test_concurrent_db_client tests/test_concurrent_db_client.cpp "${GEN_DIR};${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_db_client" "" TRUE "")
add_gtest(test_sessions tests/test_sessions.cpp "${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_db_client")
