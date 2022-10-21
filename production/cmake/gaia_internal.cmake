###################################################
# Copyright (c) Gaia Platform Authors
#
# Use of this source code is governed by the MIT
# license that can be found in the LICENSE.txt file
# or at https://opensource.org/licenses/MIT.
###################################################

# The user must explicitly set a C++ standard version; to avoid confusion, we
# do not fall back to a default version.
if(NOT CMAKE_CXX_STANDARD)
  message(FATAL_ERROR "CMAKE_CXX_STANDARD is not set!")
endif()

# Helper function to return the absolute path of the
# repo root directory.  We use this to build absolute
# include paths to code stored in the third-party
# directory.  Note that this code assumes that the
# function is invoked from a directory directly below
# the repo root (i.e. production or demos).
function(get_repo_root project_source_dir repo_dir)
  string(FIND ${project_source_dir} "/" repo_root_path REVERSE)
  string(SUBSTRING ${project_source_dir} 0 ${repo_root_path} project_repo)
  set(${repo_dir} "${project_repo}" PARENT_SCOPE)
endfunction()

#
# This function only exists because CMake symbol visibility properties
# (CXX_VISIBILITY_PRESET/VISIBILITY_INLINES_HIDDEN) don't seem to propagate to
# dependent targets when they're set on an INTERFACE target (i.e.,
# gaia_build_options), so we need to set them directly on the target.
#
function(configure_gaia_target TARGET)
  # Keep this dependency PRIVATE to avoid leaking Gaia build options into all dependent targets.
  target_link_libraries(${TARGET} PRIVATE gaia_build_options)

  # Check whether the target is a test.
  set(IS_TEST false)
  get_target_property(TARGET_LIBS ${TARGET} LINK_LIBRARIES)
  if(gtest IN_LIST TARGET_LIBS)
    set(IS_TEST true)
  endif()

  # Enable Thin LTO only on non-test targets.
  if(ENABLE_LTO)
    if(IS_TEST)
      # TODO: this is currently useless because LLD perform LTO on tests even with -fno-lto
      # I have posted this question on SO: https://stackoverflow.com/questions/72190379/lld-runs-lto-even-if-fno-lto-is-passed
      target_compile_options(${TARGET} PRIVATE -fno-lto)
      target_link_options(${TARGET} PRIVATE -fno-lto)
    else()
      target_compile_options(${TARGET} PRIVATE -flto=thin)
      target_link_options(${TARGET} PRIVATE -flto=thin -Wl,--thinlto-cache-dir=${GAIA_PROD_BUILD}/lto.cache)
    endif()
  endif()

  if(NOT EXPORT_SYMBOLS)
    # See https://cmake.org/cmake/help/latest/policy/CMP0063.html.
    cmake_policy(SET CMP0063 NEW)
    # This property sets the compiler option -fvisibility=hidden, so all symbols
    # are "hidden" (i.e., not exported) from our shared library (libgaia.so).
    # See https://gcc.gnu.org/wiki/Visibility.
    set_target_properties(${TARGET} PROPERTIES CXX_VISIBILITY_PRESET hidden)
    # This property sets the compiler option -fvisibility-inlines-hidden.
    # "This causes all inlined class member functions to have hidden visibility"
    # (https://gcc.gnu.org/wiki/Visibility).
    set_target_properties(${TARGET} PROPERTIES VISIBILITY_INLINES_HIDDEN ON)
  endif(NOT EXPORT_SYMBOLS)

  if(ENABLE_PROFILING_SUPPORT)
    # Profiling support only makes sense in Release mode.
    if(NOT CMAKE_BUILD_TYPE STREQUAL "Release")
      message(FATAL_ERROR "ENABLE_PROFILING_SUPPORT=ON is only supported in Release builds.")
    endif()

    # Instrument all Gaia static libraries/executables for profiling (e.g. uftrace).
    # Keep this property PRIVATE to avoid leaking it into dependent targets.
    # REVIEW: Listing alternative profiling options for trial-and-error
    # evaluation. Only `-pg` is supported by gcc, while the other 2 options are
    # supported by clang, so if we decide to internally support gcc, we could use
    # that option when gcc is the configured compiler.
    target_compile_options(${TARGET} PRIVATE -finstrument-functions)
    # target_compile_options(${TARGET} PRIVATE -fxray-instrument)
    # target_compile_options(${TARGET} PRIVATE -pg)
  endif(ENABLE_PROFILING_SUPPORT)
endfunction(configure_gaia_target)

#
# Helper function for setting up our tests.
#
function(set_test target arg result)
  add_test(NAME ${target}_${arg} COMMAND ${target} ${arg})
  set_tests_properties(${target}_${arg} PROPERTIES PASS_REGULAR_EXPRESSION ${result})
endfunction(set_test)

#
# Helper function for setting up google tests.
# The named arguments are required:  TARGET, SOURCES, INCLUDES, LIBRARIES
# Three optional arguments are after this:
# [DEPENDENCIES] - for add_dependencies used for generation of flatbuffer files.  Defaults to "".
# [HAS_MAIN] - "{TRUE, 1, ON, YES, Y} indicates the test provides its own main function.  Defaults to "" (FALSE).
# [ENV] - a semicolon-delimited list of key-value pairs for environment variables to be passed to the test. Defaults to "".
#
function(add_gtest TARGET SOURCES INCLUDES LIBRARIES)
  #  message(STATUS "TARGET = ${TARGET}")
  #  message(STATUS "SOURCES = ${SOURCES}")
  #  message(STATUS "INCLUDES = ${INCLUDES}")
  #  message(STATUS "LIBRARIES = ${LIBRARIES}")
  #  message(STATUS "ARGV0 = ${ARGV0}")
  #  message(STATUS "ARGV1 = ${ARGV1}")
  #  message(STATUS "ARGV2 = ${ARGV2}")
  #  message(STATUS "ARGV3 = ${ARGV3}")
  #  message(STATUS "ARGV4 = ${ARGV4}")
  #  message(STATUS "ARGV5 = ${ARGV5}")

  add_executable(${TARGET} ${SOURCES})

  if(NOT ("${ARGV4}" STREQUAL ""))
    add_dependencies(${TARGET} ${ARGV4})
  endif()

  # REVIEW: We don't currently expose a way to mark passed include dirs as
  # SYSTEM (to suppress warnings), so we just treat all of them as such.
  target_include_directories(${TARGET} SYSTEM PRIVATE ${INCLUDES} ${GOOGLE_TEST_INC})
  if("${ARGV5}")
    set(GTEST_LIB "gtest")
  else()
    set(GTEST_LIB "gtest;gtest_main")
  endif()
  target_link_libraries(${TARGET} PRIVATE ${LIBRARIES} ${GTEST_LIB})

  if(ENABLE_STACKTRACE)
    target_link_libraries(${TARGET} PRIVATE gaia_stack_trace)
  endif()

  if(NOT ("${ARGV6}" STREQUAL ""))
    set(ENV "${ARGV6}")
  else()
    set(ENV "")
  endif()

  if("$CACHE{SANITIZER}" STREQUAL "ASAN")
    # Suppress ASan warnings from exception destructors in libc++.
    # REVIEW: spdlog and cpptoml show up in the ASan stack
    # trace, and both are unconditionally built with libstdc++, so this is
    # likely an ABI incompatibility with libc++.
    # NB: This overwrites any previous value of ENV, but apparently we're not
    # using ENV for anything, and I couldn't get concatenation of NAME=VALUE
    # env var pairs to work with ASan. This is just a temporary hack anyway.
    set(ENV "ASAN_OPTIONS=alloc_dealloc_mismatch=0")
  endif()

  if("$CACHE{SANITIZER}" STREQUAL "TSAN")
    # NB: This overwrites any previous value of ENV, but apparently we're not
    # using ENV for anything.
    set(ENV "TSAN_OPTIONS=suppressions=${GAIA_REPO}/.tsan-suppressions")
  endif()

  configure_gaia_target(${TARGET})
  set_target_properties(${TARGET} PROPERTIES CXX_CLANG_TIDY "")
  add_dependencies(${TARGET} gaia_db_server_exec)
  gtest_discover_tests(${TARGET} PROPERTIES ENVIRONMENT "${ENV}")
endfunction(add_gtest)

# Stop CMake if the given parameter was not passed to the function.
macro(check_param PARAM)
  if(NOT DEFINED ${PARAM})
    message(FATAL_ERROR "The parameter ${PARAM} is required!")
  endif()
endmacro()
