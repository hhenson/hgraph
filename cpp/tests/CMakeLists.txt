cmake_minimum_required(VERSION 3.25)
project(hgraph_tests CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Prefer the project virtualenv interpreter so nanobind discovery matches
# the runtime build/test environment.
set(HGRAPH_VENV_PYTHON "${CMAKE_CURRENT_SOURCE_DIR}/../../.venv/bin/python")
if (EXISTS "${HGRAPH_VENV_PYTHON}")
    set(Python_EXECUTABLE "${HGRAPH_VENV_PYTHON}" CACHE FILEPATH "Python executable" FORCE)
endif()

# Python + nanobind headers are required by hgraph value/TS metadata headers.
find_package(Python 3.12 COMPONENTS Interpreter Development Development.Module REQUIRED)
execute_process(
    COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE
    OUTPUT_VARIABLE nanobind_ROOT
)
if (nanobind_ROOT)
    set(nanobind_DIR "${nanobind_ROOT}" CACHE PATH "nanobind CMake package dir" FORCE)
    list(PREPEND CMAKE_PREFIX_PATH "${nanobind_ROOT}")
endif()
find_package(nanobind CONFIG REQUIRED)
nanobind_build_library(hgraph_nanobind-static)

# Find Catch2
find_package(Catch2 3 QUIET)
if (NOT Catch2_FOUND)
    message(STATUS "Catch2 not found via find_package, using FetchContent")
    include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v3.5.0
    )
    FetchContent_MakeAvailable(Catch2)
endif()

# Find fmt
find_package(fmt CONFIG QUIET)
if (NOT fmt_FOUND)
    message(STATUS "fmt not found via find_package, using FetchContent")
    include(FetchContent)
    FetchContent_Declare(fmt GIT_REPOSITORY https://github.com/fmtlib/fmt.git GIT_TAG 10.1.0)
    FetchContent_MakeAvailable(fmt)
endif()

# hgraph forward declarations include tp/tpack.h
find_package(tpack CONFIG QUIET)
if (NOT tpack_FOUND)
    include(FetchContent)
    FetchContent_Declare(tpack GIT_REPOSITORY https://github.com/uentity/tpack.git GIT_TAG v0.3.2)
    FetchContent_MakeAvailable(tpack)
endif()

# hgraph value headers include sul/dynamic_bitset.hpp via KeySet.
find_package(dynamic_bitset CONFIG QUIET)
if (NOT dynamic_bitset_FOUND)
    include(FetchContent)
    FetchContent_Declare(
        dynamic_bitset
        GIT_REPOSITORY https://github.com/pinam45/dynamic_bitset.git
        GIT_TAG v1.2.0
    )
    FetchContent_MakeAvailable(dynamic_bitset)
endif()

# hgraph value validity helpers depend on nanoarrow bitmap helpers.
find_package(nanoarrow CONFIG QUIET)
if (NOT TARGET nanoarrow::nanoarrow)
    include(FetchContent)
    set(NANOARROW_BUILD_TESTS OFF CACHE BOOL "Disable nanoarrow tests" FORCE)
    set(NANOARROW_BUILD_BENCHMARKS OFF CACHE BOOL "Disable nanoarrow benchmarks" FORCE)
    set(NANOARROW_IPC OFF CACHE BOOL "Disable nanoarrow IPC extension" FORCE)
    FetchContent_Declare(
        nanoarrow
        GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
        GIT_TAG apache-arrow-nanoarrow-0.7.0
    )
    FetchContent_MakeAvailable(nanoarrow)
endif()

# Create visitor test executable
add_executable(hgraph_visitor_tests
    test_visitor_standalone.cpp
)

target_link_libraries(hgraph_visitor_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
)

# Set C++ standard
target_compile_features(hgraph_visitor_tests PRIVATE cxx_std_23)

# Select the best available Python link target for this environment.
set(HGRAPH_PYTHON_TARGET Python::Module)
if (TARGET Python::Python)
    set(HGRAPH_PYTHON_TARGET Python::Python)
endif()

# Create TS schema registry test executable
add_executable(hgraph_ts_schema_tests
    test_delta_view.cpp
    test_link_observer_registry.cpp
    test_ts_schema_registry.cpp
    ts_test_stubs.cpp
    ../src/cpp/types/value/type_registry.cpp
    ../src/cpp/types/time_series/link_observer_registry.cpp
    ../src/cpp/types/time_series/ts_meta_schema_cache.cpp
    ../src/cpp/types/time_series/ts_type_registry.cpp
    ../src/cpp/types/time_series/link_target.cpp
    ../src/cpp/types/time_series/ref_link.cpp
    ../src/cpp/types/time_series/view_data.cpp
)

target_include_directories(hgraph_ts_schema_tests PRIVATE
    ../include
    ${Python_INCLUDE_DIRS}
    ${NB_DIR}/include
)
if (DEFINED tpack_SOURCE_DIR)
    target_include_directories(hgraph_ts_schema_tests PRIVATE "${tpack_SOURCE_DIR}/include")
endif()
if (DEFINED dynamic_bitset_SOURCE_DIR)
    target_include_directories(hgraph_ts_schema_tests PRIVATE "${dynamic_bitset_SOURCE_DIR}/include")
endif()

target_link_libraries(hgraph_ts_schema_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
    ${HGRAPH_PYTHON_TARGET}
    hgraph_nanobind-static
)
if (TARGET tp::tpack)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE tp::tpack)
elseif (TARGET tpack)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE tpack)
endif()
if (TARGET dynamic_bitset)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE dynamic_bitset)
elseif (TARGET sul::dynamic_bitset)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE sul::dynamic_bitset)
endif()
if (TARGET nanoarrow::nanoarrow)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE nanoarrow::nanoarrow)
elseif (TARGET nanoarrow)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE nanoarrow)
elseif (TARGET nanoarrow::nanoarrow_static)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE nanoarrow::nanoarrow_static)
elseif (TARGET nanoarrow_static)
    target_link_libraries(hgraph_ts_schema_tests PRIVATE nanoarrow_static)
endif()

target_compile_features(hgraph_ts_schema_tests PRIVATE cxx_std_23)

# Enable testing
enable_testing()

# Register tests with CTest
include(CTest)
include(Catch)
catch_discover_tests(hgraph_visitor_tests)
catch_discover_tests(hgraph_ts_schema_tests)

# Optional: Add custom target to run tests
add_custom_target(run_visitor_tests
    COMMAND hgraph_visitor_tests
    DEPENDS hgraph_visitor_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running visitor pattern tests"
)

add_custom_target(run_ts_schema_tests
    COMMAND hgraph_ts_schema_tests
    DEPENDS hgraph_ts_schema_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running TS schema registry tests"
)
