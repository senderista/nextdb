#############################################
# Copyright (c) Gaia Platform Authors
# All rights reserved.
#############################################

project(flatbuffers)

set(FLATBUFFERS_VERSION "2.0.8")

add_library(flatbuffers INTERFACE)
add_dependencies(flatbuffers flatbuffers_dependency)
target_include_directories(flatbuffers SYSTEM INTERFACE ${FLATBUFFERS_INC})
target_link_libraries(flatbuffers INTERFACE ${FLATBUFFERS_ROOT}/lib/libflatbuffers.a)

# Canonicalize the build type, to extract build type-specific flags.
if(CMAKE_BUILD_TYPE)
  string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE)
endif()

# REVIEW: We don't try to pass COMPILE_DEFINITIONS from directory scope in CMAKE_CXX_FLAGS, because
# it's just too much trouble to handle generator expressions. We can revisit this if there's a nice
# way to handle generator expressions. For now, we don't use add_compile_definitions() in our
# top-level CMakeLists.txt, for just this reason.

# Check that there are no COMPILE_DEFINITIONS in this scope.
get_property(FLATBUFFERS_COMPILE_DEFINITIONS DIRECTORY PROPERTY COMPILE_DEFINITIONS)
list(LENGTH FLATBUFFERS_COMPILE_DEFINITIONS FLATBUFFERS_COMPILE_DEFINITIONS_COUNT)
if(FLATBUFFERS_COMPILE_DEFINITIONS_COUNT GREATER 0)
  message(FATAL_ERROR "Global compile definitions detected, aborting configuration: ${FLATBUFFERS_COMPILE_DEFINITIONS}")
endif()

get_property(FLATBUFFERS_COMPILE_OPTIONS DIRECTORY PROPERTY COMPILE_OPTIONS)
string(REPLACE ";" " " FLATBUFFERS_COMPILE_OPTIONS "${FLATBUFFERS_COMPILE_OPTIONS}")

get_property(FLATBUFFERS_LINK_OPTIONS DIRECTORY PROPERTY LINK_OPTIONS)
string(REPLACE ";" " " FLATBUFFERS_LINK_OPTIONS "${FLATBUFFERS_LINK_OPTIONS}")

set(FLATBUFFERS_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${BUILD_TYPE}} ${FLATBUFFERS_COMPILE_OPTIONS}")
set(FLATBUFFERS_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_EXE_LINKER_FLAGS_${BUILD_TYPE}} ${FLATBUFFERS_LINK_OPTIONS}")
set(FLATBUFFERS_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS_${BUILD_TYPE}} ${FLATBUFFERS_LINK_OPTIONS}")
set(FLATBUFFERS_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${CMAKE_MODULE_LINKER_FLAGS_${BUILD_TYPE}} ${FLATBUFFERS_LINK_OPTIONS}")
# Don't include FLATBUFFERS_LINK_OPTIONS in CMAKE_STATIC_LINKER_FLAGS, because the archiver won't accept them.
set(FLATBUFFERS_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} ${CMAKE_STATIC_LINKER_FLAGS_${BUILD_TYPE}}")

ExternalProject_Add(flatbuffers_dependency
  URL
    https://github.com/google/flatbuffers/archive/refs/tags/v${FLATBUFFERS_VERSION}.tar.gz
  URL_HASH
    SHA256=f97965a727d26386afaefff950badef2db3ab6af9afe23ed6d94bfb65f95f37e
  EXCLUDE_FROM_ALL
    TRUE
  SOURCE_DIR
    ${FLATBUFFERS_SRC}
  CMAKE_ARGS
    -G${CMAKE_GENERATOR}
    $<$<BOOL:${CCACHE_FOUND}>:-DCMAKE_CXX_COMPILER_LAUNCHER=ccache>
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${FLATBUFFERS_CXX_FLAGS}
    -DCMAKE_EXE_LINKER_FLAGS=${FLATBUFFERS_EXE_LINKER_FLAGS}
    -DCMAKE_MODULE_LINKER_FLAGS=${FLATBUFFERS_MODULE_LINKER_FLAGS}
    -DCMAKE_SHARED_LINKER_FLAGS=${FLATBUFFERS_SHARED_LINKER_FLAGS}
    -DCMAKE_STATIC_LINKER_FLAGS=${FLATBUFFERS_STATIC_LINKER_FLAGS}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DFLATBUFFERS_BUILD_TESTS=OFF
    -DFLATBUFFERS_INSTALL=ON
    -DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_ROOT}
  BUILD_BYPRODUCTS
    ${FLATBUFFERS_INC}
    ${FLATBUFFERS_SRC}/src/idl_gen_cpp.cpp
    ${FLATBUFFERS_SRC}/src/code_generators.cpp
    ${FLATBUFFERS_BIN}/flatc
    ${FLATBUFFERS_LIB}/libflatbuffers.a
)
