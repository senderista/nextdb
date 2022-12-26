###################################################
# Copyright (c) Gaia Platform Authors
#
# Use of this source code is governed by the MIT
# license that can be found in the LICENSE.txt file
# or at https://opensource.org/licenses/MIT.
###################################################

cmake_minimum_required(VERSION 3.17)

# Specify the C++ standard.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

project(production VERSION 0.6.0)

option(ENABLE_STACKTRACE "Enable stack traces" ON)

option(EXPORT_SYMBOLS "Export symbols by default from shared libraries" OFF)

option(ENABLE_PROFILING_SUPPORT "Instrument binaries for profiling" OFF)

option(DISABLE_ASSERTS "Disable assertions" OFF)

# This safety feature reduces throughput by ~40% for a single thread, so we need
# to be able to disable it easily to measure baseline performance.
option(DISABLE_SAFE_TIMESTAMPS "Disable safe timestamps" OFF)

# Enable this option to stress-test safe timestamp correctness.
option(TXN_METADATA_GC_IMMEDIATE "Immediately GC txn metadata" OFF)

# This option improves performance when chunks are quickly reused (by reducing
# page faults and physical page allocation/zeroing), but may increase physical
# memory usage otherwise.
option(DISABLE_CHUNK_DECOMMIT "Don't decommit physical memory when a chunk is deallocated" OFF)

option(DISABLE_CCACHE "Disable ccache unconditionally" OFF)

option(ENABLE_CLANG_TIDY "Enable Clang-Tidy to be specified externally" OFF)

# LTO settings are set inside the configure_gaia_target() function to prevent it from being applied to tests.
option(ENABLE_LTO "Enable Link Time Optimization")

if (NOT ENABLE_CLANG_TIDY)
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    option(ENABLE_CLANG_TIDY "Enable clang tidy" ON)
  else()
    option(ENABLE_CLANG_TIDY "Enable clang tidy" OFF)
  endif()
endif()

if (NOT DEFINED ENABLE_LTO)
  # LTO slows tests down significantly, therefore we keep it
  # enabled by default only on Release builds.
  if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(ENABLE_LTO ON)
  else()
    set(ENABLE_LTO OFF)
  endif()
endif()

# Sets the Gaia build architecture which is the SDK target architecture.
# For simplicity we do not support cross-compiling yet.
if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "amd64|AMD64|x86_64|X86_64")
  set(GAIA_BUILD_ARCH "amd64")
elseif (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "arm64|ARM64|aarch64|AARCH64")
  set(GAIA_BUILD_ARCH "arm64")
else()
  message(FATAL_ERROR "Unrecognised architecture: ${CMAKE_HOST_SYSTEM_PROCESSOR}")
endif()

message(STATUS "GAIA_BUILD_ARCH: ${GAIA_BUILD_ARCH}")

include(GNUInstallDirs)
include(ExternalProject)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

include(gaia_internal)
include(GoogleTest)

# Check that PIE flags are actually passed to the linker.
# This ensures that our executables are ASLR-compatible.
include(CheckPIESupported)
check_pie_supported(OUTPUT_VARIABLE check_pie_error LANGUAGES CXX)
if(NOT CMAKE_CXX_LINK_PIE_SUPPORTED)
  message(FATAL_ERROR "PIE is not supported at link time: ${check_pie_error}")
endif()
# See https://cmake.org/cmake/help/latest/policy/CMP0083.html#policy:CMP0083.
# The NEW behavior under this policy is to pass the -pie flag to the linker if
# POSITION_INDEPENDENT_CODE is set.
cmake_policy(SET CMP0083 NEW)
# Our shared libraries and executables must all be ASLR-compatible.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Set some global variables.
get_repo_root(${PROJECT_SOURCE_DIR} GAIA_REPO)

# Default to Release builds if not specified on the command line.
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND AND NOT DISABLE_CCACHE)
  set_property(GLOBAL PROPERTY CMAKE_CXX_COMPILER_LAUNCHER ccache)
endif()

# Default compiler/linker flags for Gaia targets.
add_library(gaia_build_options INTERFACE)
target_compile_options(gaia_build_options INTERFACE -Wall -Wextra -Wunused -Wunreachable-code -ftime-trace)
# Ensure Debug builds have the DEBUG macro defined.
target_compile_definitions(gaia_build_options INTERFACE $<$<CONFIG:Debug>:DEBUG>)
# Propagate the DISABLE_ASSERTS option to a macro definition.
target_compile_definitions(gaia_build_options INTERFACE $<$<BOOL:${DISABLE_ASSERTS}>:DISABLE_ASSERTS>)
# Disabling asserts will produce bogus unused variable warnings.
target_compile_options(gaia_build_options INTERFACE $<$<BOOL:${DISABLE_ASSERTS}>:-Wno-unused-parameter -Wno-unused-lambda-capture -Wno-unused-variable -Wno-unused-but-set-variable>)
# Propagate the DISABLE_SAFE_TIMESTAMPS option to a macro definition.
target_compile_definitions(gaia_build_options INTERFACE $<$<BOOL:${DISABLE_SAFE_TIMESTAMPS}>:DISABLE_SAFE_TIMESTAMPS>)
# Propagate the TXN_METADATA_GC_IMMEDIATE option to a macro definition.
target_compile_definitions(gaia_build_options INTERFACE $<$<BOOL:${TXN_METADATA_GC_IMMEDIATE}>:TXN_METADATA_GC_IMMEDIATE>)
# Propagate the DISABLE_CHUNK_DECOMMIT option to a macro definition.
target_compile_definitions(gaia_build_options INTERFACE $<$<BOOL:${DISABLE_CHUNK_DECOMMIT}>:DISABLE_CHUNK_DECOMMIT>)

# Always use libc++ for better C++20 and sanitizer support.
# (For eventual MemorySanitizer support, we need all targets in debug builds to
# use libc++, including third-party packages.)
add_compile_options(-stdlib=libc++)
add_link_options(-stdlib=libc++)
# Use incomplete C++20 features in libc++ (for std::format and maybe std::ranges).
add_compile_options(-fexperimental-library)
add_link_options(-lc++experimental)

# Debug build-specific options.
if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
  # Don't omit debug symbols from any built binaries.
  add_compile_options(-fno-limit-debug-info)

  target_compile_options(gaia_build_options INTERFACE -g3 -ggdb -fno-omit-frame-pointer -fno-optimize-sibling-calls -ggnu-pubnames -gsplit-dwarf)
  target_link_options(gaia_build_options INTERFACE -Wl,--gdb-index)

  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(gaia_build_options INTERFACE -O0)

    # Enable ASan by default in debug builds.
    set(SANITIZER "ASAN" CACHE STRING "Enable sanitizers in debug builds")

    # Explicitly list all legal values for SANITIZER.
    # (This will eventually include ASAN, MSAN, TSAN.)
    set_property(CACHE SANITIZER PROPERTY STRINGS "ASAN" "TSAN" "NONE")
  endif()

  if(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # Set the highest optimization level compatible with reasonable debuggability.
    target_compile_options(gaia_build_options INTERFACE -Og)
  endif()
else()
  # Set the maximum optimization level for release builds.
  target_compile_options(gaia_build_options INTERFACE -O3)
endif()

# Sanitizer-specific compiler/linker flags.
if(DEFINED CACHE{SANITIZER})
  message(VERBOSE "Sanitizer=$CACHE{SANITIZER}")

  # Sanitizers can only be defined in debug builds.
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Clear the given value from the cache so subsequent runs don't fail.
    unset(SANITIZER CACHE)
    message(FATAL_ERROR "The SANITIZER option can only be set in debug builds.")
  endif()

  # Extract allowed option values.
  get_property(SANITIZER_OPTIONS CACHE SANITIZER PROPERTY STRINGS)

  # Check that value of the option is in the list of allowed values.
  if(NOT "$CACHE{SANITIZER}" IN_LIST SANITIZER_OPTIONS)
    # Clear a bad value from the cache so subsequent runs don't fail.
    set(bad_sanitizer "$CACHE{SANITIZER}")
    unset(SANITIZER CACHE)
    message(FATAL_ERROR "Unrecognized value for SANITIZER option: ${bad_sanitizer}.\n"
      "Allowed values: ${SANITIZER_OPTIONS}.")
  endif()

  # Set compiler/linker flags that apply to all sanitizers.
  add_compile_options(-fno-sanitize-recover=all)
  add_link_options(-fno-sanitize-recover=all)

  if("$CACHE{SANITIZER}" STREQUAL "ASAN")
    # Enable ASan on all targets because libc++ requires it to prevent false positives.
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)

    # Add UBSan as well as ASan to Gaia targets.
    # (They are compatible so we have no separate option for UBSan.)
    target_compile_options(gaia_build_options INTERFACE -fsanitize=undefined)
    target_link_options(gaia_build_options INTERFACE -fsanitize=undefined)
  endif()

  if("$CACHE{SANITIZER}" STREQUAL "TSAN")
    # Add TSan flags only to Gaia targets to avoid compatibility issues with other libraries.
    target_compile_options(gaia_build_options INTERFACE -fsanitize=thread)
    target_link_options(gaia_build_options INTERFACE -fsanitize=thread)
  endif()
endif()

message(STATUS "CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
get_target_property(gaia_compile_options gaia_build_options INTERFACE_COMPILE_OPTIONS)
get_target_property(gaia_link_options gaia_build_options INTERFACE_LINK_OPTIONS)
message(VERBOSE "Gaia compile options: ${gaia_compile_options}")
message(VERBOSE "Gaia link options: ${gaia_link_options}")

set(GAIA_INC "${PROJECT_SOURCE_DIR}/inc")
set(DEPENDENCIES_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/deps")

set(GOOGLE_TEST "${GAIA_REPO}/third_party/production/googletest")
set(GOOGLE_TEST_INC "${GOOGLE_TEST}/googletest/include")

set(GAIA_PROD_BUILD "${CMAKE_CURRENT_BINARY_DIR}")

set(GAIA_PARSER_GENERATED "${GAIA_PROD_BUILD}/catalog/parser/generated")
set(GAIA_GENERATED_CODE "${GAIA_PROD_BUILD}/gaia_generated")

# We need pthreads support.
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

message(CHECK_START "Looking for libexplain and libcap")
find_library(LIB_EXPLAIN NAMES libexplain.a)
find_library(LIB_CAP NAMES libcap.a cap)
if(LIB_EXPLAIN AND LIB_CAP)
  message(CHECK_PASS "found")
else()
  message(CHECK_FAIL "not found")
  message(FATAL_ERROR "libexplain and libcap are required.")
endif()

message(VERBOSE "GAIA_INC=${GAIA_INC}")

# External project dependencies.

# Backward (this needs to come first because it changes build options for subsequent dependencies).
if(ENABLE_STACKTRACE)
  message(VERBOSE "ENABLE_STACKTRACE=${ENABLE_STACKTRACE}")
  add_subdirectory("${GAIA_REPO}/third_party/production/backward" backward)
  add_library(gaia_stack_trace INTERFACE)
  target_sources(gaia_stack_trace INTERFACE "${GAIA_INC}/gaia_internal/common/backward.cpp")
  add_compile_options(-funwind-tables -fno-omit-frame-pointer)
  # Only append this option for Release builds, or it will disable all other debug info generation.
  if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-gline-tables-only)
  endif()
  target_link_libraries(gaia_stack_trace INTERFACE backward)
endif()

add_subdirectory(${GOOGLE_TEST} googletest EXCLUDE_FROM_ALL)

# Setting clang-tidy here to avoid it being applied to flatbuffers, rocksdb etc...
# TODO Maybe we could force each subproject to set this flag?
if(ENABLE_CLANG_TIDY)
    set(CMAKE_CXX_CLANG_TIDY clang-tidy)
endif()

# Add individual component folders.
# These are listed roughly in a bottom-up order
# in terms of platform architecture.

# Core database engine.
add_subdirectory(common)
add_subdirectory(db)

# Tools.
add_subdirectory(tools)

# We want to disable all clang-tidy checks for generated code.
# However, if no checks are specified, clang-tidy will assume we
# did not configure it correctly. Just add one check that will never be triggered.
file(WRITE ${GAIA_PROD_BUILD}/.clang-tidy "Checks: '-*,llvm-twine-local'\n")

# Generate dependencies graph
# https://cmake.org/cmake/help/latest/module/CMakeGraphVizOptions.html
# To visualize the results you can:
# - Use dot to generate a png/svg (dot -Tpng -o foo.png foo.dot)
# - Install an IDE plugin that automatically renders graphviz files
# - Use an online renderer such as: https://dreampuf.github.io/GraphvizOnline
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CMakeGraphVizOptions.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/CMakeGraphVizOptions.cmake COPYONLY)

add_custom_target(gaia_dependency_graph
  COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/gaia_dependencies
  COMMAND rm -f ${CMAKE_CURRENT_BINARY_DIR}/gaia_dependencies/*
  COMMAND "${CMAKE_COMMAND}" "--graphviz=gaia_dependencies/gaia" .
  WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
