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
# Generate flatbuffers headers.
###############################################
set(GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

# The names of the following source files should be unique because they'll be used as targets.
set(FBS_SOURCES
  "${PROJECT_SOURCE_DIR}/src/flatbuffers/messages.fbs"
)

set(FBS_SOURCE_FILENAMES "")
foreach(FBS_SOURCE ${FBS_SOURCES})
  get_filename_component(FBS_SOURCE_FILENAME ${FBS_SOURCE} NAME)
  string(REGEX REPLACE "\\.fbs$" "_generated.h" GEN_HEADER ${FBS_SOURCE_FILENAME})

  add_custom_command(
    COMMENT "Compiling ${FBS_SOURCE} with flatbuffers..."
    OUTPUT "${GEN_DIR}/${GEN_HEADER}"
    COMMAND cmake -E make_directory "${GEN_DIR}"
    COMMAND ${FLATBUFFERS_BIN}/flatc --cpp --scoped-enums -o "${GEN_DIR}" "${FBS_SOURCE}"
    DEPENDS "${FBS_SOURCE}"
    DEPENDS flatc
    VERBATIM)

  add_custom_target(${FBS_SOURCE_FILENAME} ALL DEPENDS "${GEN_DIR}/${GEN_HEADER}")
  list(APPEND FBS_SOURCE_FILENAMES ${FBS_SOURCE_FILENAME})
endforeach(FBS_SOURCE)

###############################################
# Database-related code begins here.
###############################################
add_library(gaia_db_client STATIC
  src/chunk_manager.cpp
  src/client_contexts.cpp
  src/client_messenger.cpp
  src/db_client_api.cpp
  src/db_client.cpp
  src/exceptions.cpp
  src/gaia_ptr_api.cpp
  src/gaia_ptr_client.cpp
  src/gaia_ptr.cpp
  src/memory_manager.cpp
  src/txn_metadata.cpp
  src/type_metadata.cpp
)

add_dependencies(gaia_db_client ${FBS_SOURCE_FILENAMES})
configure_gaia_target(gaia_db_client)
target_include_directories(gaia_db_client PUBLIC ${GAIA_DB_CORE_PUBLIC_INCLUDES})
target_include_directories(gaia_db_client PRIVATE ${GAIA_DB_CORE_PRIVATE_INCLUDES})
target_link_libraries(gaia_db_client PUBLIC gaia_common)
target_link_libraries(gaia_db_client PRIVATE ${LIB_EXPLAIN} ${LIB_CAP} dl)

# Suppress spurious warnings about zero-initialized structs.
target_compile_options(gaia_db_client PRIVATE -Wno-missing-field-initializers)

# For flatbuffers generated code.
target_include_directories(gaia_db_client SYSTEM PRIVATE "${FLATBUFFERS_INC}")
target_include_directories(gaia_db_client SYSTEM PRIVATE "${GEN_DIR}")

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
  src/gaia_ptr_server.cpp
  src/gaia_ptr.cpp
  src/memory_manager.cpp
  src/server_contexts.cpp
  src/system_checks.cpp
  src/txn_metadata.cpp
)

add_library(gaia_db_server ${GAIA_DB_SERVER_SOURCES})
# Add GAIA_DB_SERVER preprocessor definition for conditional includes.
target_compile_definitions(gaia_db_server PUBLIC GAIA_DB_SERVER=1)
add_dependencies(gaia_db_server ${FBS_SOURCE_FILENAMES})
configure_gaia_target(gaia_db_server)
target_include_directories(gaia_db_server PUBLIC
  "${GAIA_DB_CORE_PUBLIC_INCLUDES}"
  "${GAIA_DB_CORE_PRIVATE_INCLUDES}")

# For flatbuffers generated code.
target_include_directories(gaia_db_server SYSTEM PUBLIC "${FLATBUFFERS_INC}")
target_include_directories(gaia_db_server SYSTEM PUBLIC "${GEN_DIR}")

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
add_gtest(test_concurrent_db_client tests/test_concurrent_db_client.cpp "${GEN_DIR};${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_db_client")
add_gtest(test_gaia_ptr_api tests/test_gaia_ptr_api.cpp "${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_common;gaia_db_client;gaia_direct;gaia_catalog")
add_gtest(test_env_instance_name tests/test_db_server_env.cpp "${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_common;gaia_db_client;gaia_direct")
add_gtest(test_sessions tests/test_sessions.cpp "${GAIA_DB_CORE_TEST_INCLUDES}" "gaia_db_client")
