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

# Create test executable for visitor tests
add_executable(hgraph_visitor_tests
    test_visitor_standalone.cpp
)

target_link_libraries(hgraph_visitor_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
)

# Set C++ standard
target_compile_features(hgraph_visitor_tests PRIVATE cxx_std_23)

# Create test executable for value type system tests
add_executable(hgraph_value_tests
    test_value.cpp
    value_examples.cpp
)

target_include_directories(hgraph_value_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(hgraph_value_tests PRIVATE
    Catch2::Catch2WithMain
)

target_compile_features(hgraph_value_tests PRIVATE cxx_std_23)

# Create test executable for TSInput and access strategy tests
# Note: This test requires parent build configuration (HGRAPH_BUILD_TESTS=ON from cpp/CMakeLists.txt)
# It uses nanobind and tpack dependencies from the parent project
add_executable(hgraph_ts_input_tests
    test_ts_input.cpp
    ../src/cpp/types/time_series/ts_output.cpp
    ../src/cpp/types/time_series/ts_input.cpp
    ../src/cpp/types/time_series/access_strategy.cpp
    ../src/cpp/types/ts_type_meta.cpp
)

# Include directories for the test
# Note: We need to explicitly add some directories that are normally inherited
# from the main _hgraph target
target_include_directories(hgraph_ts_input_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${Python_INCLUDE_DIRS}
    ${NB_DIR}/include
    ${CMAKE_BINARY_DIR}/_deps/tpack-src/include
    ${CMAKE_BINARY_DIR}/_deps/ddvisitor-src/include
)

# Link libraries
# Note: tpack is header-only (interface library), so no need to link it
target_link_libraries(hgraph_ts_input_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
)

target_compile_features(hgraph_ts_input_tests PRIVATE cxx_std_23)

# Enable testing
enable_testing()

# Register tests with CTest
include(CTest)
include(Catch)
catch_discover_tests(hgraph_visitor_tests)
catch_discover_tests(hgraph_value_tests)
catch_discover_tests(hgraph_ts_input_tests)

# Optional: Add custom target to run tests
add_custom_target(run_visitor_tests
    COMMAND hgraph_visitor_tests
    DEPENDS hgraph_visitor_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running visitor pattern tests"
)

add_custom_target(run_value_tests
    COMMAND hgraph_value_tests
    DEPENDS hgraph_value_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running value type system tests"
)

add_custom_target(run_ts_input_tests
    COMMAND hgraph_ts_input_tests
    DEPENDS hgraph_ts_input_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running TSInput and access strategy tests"
)
