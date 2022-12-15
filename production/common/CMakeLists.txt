###################################################
# Copyright (c) Gaia Platform Authors
#
# Use of this source code is governed by the MIT
# license that can be found in the LICENSE.txt file
# or at https://opensource.org/licenses/MIT.
###################################################

project(common)

set(GAIA_COMMON_INCLUDES
  "${GAIA_INC}")

message(VERBOSE "GAIA_COMMON_INCLUDES=${GAIA_COMMON_INCLUDES}")

# Our main target.
add_library(gaia_common STATIC
  src/hash.cpp
  src/timer.cpp
  src/random.cpp
)

configure_gaia_target(gaia_common)
target_include_directories(gaia_common PUBLIC ${GAIA_COMMON_INCLUDES})

# Tests.
add_gtest(test_assert tests/test_assert.cpp "${GAIA_COMMON_INCLUDES}" "gaia_common")
add_gtest(test_int_type tests/test_int_type.cpp "${GAIA_COMMON_INCLUDES}" "gaia_common")
add_gtest(test_random tests/test_random.cpp "${GAIA_COMMON_INCLUDES}" "gaia_common")
add_gtest(test_inline_shared_lock tests/test_inline_shared_lock.cpp "${GAIA_COMMON_INCLUDES}" "gaia_common")
add_gtest(test_hash tests/test_hash.cpp "${GAIA_COMMON_INCLUDES}" "gaia_common")
add_gtest(test_bitmap tests/test_bitmap.cpp "${GAIA_MEMORY_MANAGER_INCLUDES}" "gaia_common")
