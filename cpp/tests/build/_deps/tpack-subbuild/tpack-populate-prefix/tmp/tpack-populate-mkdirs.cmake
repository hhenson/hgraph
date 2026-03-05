# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file LICENSE.rst or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-src")
  file(MAKE_DIRECTORY "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-src")
endif()
file(MAKE_DIRECTORY
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-build"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-subbuild/tpack-populate-prefix"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-subbuild/tpack-populate-prefix/tmp"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-subbuild/tpack-populate-prefix/src/tpack-populate-stamp"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-subbuild/tpack-populate-prefix/src"
  "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-subbuild/tpack-populate-prefix/src/tpack-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-subbuild/tpack-populate-prefix/src/tpack-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/hhenson/CLionProjects/hgraph/cpp/tests/build/_deps/tpack-subbuild/tpack-populate-prefix/src/tpack-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
