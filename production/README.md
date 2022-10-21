# production
This is a folder for production code. Only code that is meant to be shipped should be placed here.

## Build Overview

We have two ways to build the configurations listed above:
1. Using docker and our `gdev` tool: this is what our TeamCity Continuous Integration servers use to build and run tests in a consistent "approved" environment.
1. Using your own local environment:  this enables you to build outside of docker using your own environment and tools.

The remainder of this document will focus on #2:  building in your own local environment outside of a docker container.  For instructions on how to use `gdev` on your local machine, please see: [gdev docker build CLI README](../dev_tools/gdev/README.md).

For instructions on how to setup your environment, please see our *New Hire Guidelines* document on our GaiaPlatform wiki.

## Build Instructions
Create a subfolder `GaiaPlatform/production/build` and then execute the following commands in it depending upon which set of targets you want to build:

### Core
```
cmake ..
make -j<number of CPUs>
```
If `CMAKE_BUILD_TYPE` is not specified on the command line, then by default we add compile and link flags to include debugging information.

### Other Flags
Other CMake variables we use but are not required:

```
# Override the build type to Debug or Release.
# If explicitly set to Debug, then address sanitizer will be enabled.
-DCMAKE_BUILD_TYPE=Debug|Release

# Get more info on CMake messages.
--log-level=VERBOSE

# Suppress CMake dev warnings.
-Wno-dev
```
