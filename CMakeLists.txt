cmake_minimum_required(VERSION 3.25)

# Minimal top-level CMake to expose the C++ engine targets to CLion
# without altering the existing Python packaging yet.
project(hgraph_cpp_root NONE)

# Enable CTest at the top level so ctest can be run from the root build dir
include(CTest)
enable_testing()

add_subdirectory(cpp)