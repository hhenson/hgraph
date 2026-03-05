# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file LICENSE.rst or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-src")
  file(MAKE_DIRECTORY "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-src")
endif()
file(MAKE_DIRECTORY
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-build"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-subbuild/nanoarrow-populate-prefix"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-subbuild/nanoarrow-populate-prefix/tmp"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-subbuild/nanoarrow-populate-prefix/src/nanoarrow-populate-stamp"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-subbuild/nanoarrow-populate-prefix/src"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-subbuild/nanoarrow-populate-prefix/src/nanoarrow-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-subbuild/nanoarrow-populate-prefix/src/nanoarrow-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/nanoarrow-subbuild/nanoarrow-populate-prefix/src/nanoarrow-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
