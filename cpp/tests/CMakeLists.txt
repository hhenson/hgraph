cmake_minimum_required(VERSION 3.25)
project(hgraph_tests CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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

# Create test executable
add_executable(hgraph_visitor_tests
    test_visitor_standalone.cpp
)

target_link_libraries(hgraph_visitor_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
)

# Set C++ standard
target_compile_features(hgraph_visitor_tests PRIVATE cxx_std_23)

# Enable testing
enable_testing()

# Register tests with CTest
include(CTest)
include(Catch)
catch_discover_tests(hgraph_visitor_tests)

# Optional: Add custom target to run tests
add_custom_target(run_visitor_tests
    COMMAND hgraph_visitor_tests
    DEPENDS hgraph_visitor_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running visitor pattern tests"
)

# ============================================================================
# TSTypeRegistry Tests
# ============================================================================

# Create test executable for TSTypeRegistry
# Note: This test requires the hgraph library to be built first
# as it depends on TSTypeRegistry and TypeRegistry implementations
add_executable(hgraph_ts_type_registry_tests
    types/time_series/test_ts_type_registry.cpp
)

target_include_directories(hgraph_ts_type_registry_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/cpp/include
)

target_link_libraries(hgraph_ts_type_registry_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
    _hgraph  # Link against the main hgraph library
)

target_compile_features(hgraph_ts_type_registry_tests PRIVATE cxx_std_23)

# Register TSTypeRegistry tests with CTest
catch_discover_tests(hgraph_ts_type_registry_tests)

# Optional: Add custom target to run TSTypeRegistry tests
add_custom_target(run_ts_type_registry_tests
    COMMAND hgraph_ts_type_registry_tests
    DEPENDS hgraph_ts_type_registry_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running TSTypeRegistry tests"
)
