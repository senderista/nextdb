###################################################
# Copyright (c) Gaia Platform Authors
#
# Use of this source code is governed by the MIT
# license that can be found in the LICENSE.txt file
# or at https://opensource.org/licenses/MIT.
###################################################

project(tools)

# This should be enabled only for testing and improving .clang-tidy and .clang-format
# we don't want to build it as part of the normal build.
option(BUILD_GAIA_STYLE_TESTS "Build clang-tidy/clang-format tests" OFF)

if(BUILD_GAIA_STYLE_TESTS)
  add_subdirectory(gaia_style)
endif()
