cmake_minimum_required(VERSION 3.25)

project(hgraph_ts_v2_tests)

add_executable(hgraph_ts_v2_tests
        test_time_series_v2.cpp
        test_ts_value_anyvalue.cpp
        ../src/cpp/types/v2/ts_event.cpp
        ../src/cpp/types/v2/ts_value.cpp
)

# Note: ts_value.cpp is compiled here because tests use TSOutput/TSInput directly
# This doesn't require nanobind symbols since we're not using the template wrappers

# Inherit include dirs from parent (Python, nanobind, project includes)
# Link Catch2 and fmt if available via parent FetchContent

# No Python runtime/dev required for Catch2 tests; link only minimal deps

target_link_libraries(hgraph_ts_v2_tests PRIVATE Catch2::Catch2WithMain fmt::fmt nanobind-static)

target_compile_features(hgraph_ts_v2_tests PRIVATE cxx_std_23)

# Note: HGRAPH_WITH_PYTHON_TOSTRING is NOT enabled for tests
# Tests are standalone executables and don't have Python runtime available

# Define NB_STATIC to ensure nanobind provides intrusive_counter implementations
target_compile_definitions(hgraph_ts_v2_tests PRIVATE NB_STATIC=1)

include(CTest)
# Register the test executable explicitly with CTest
add_test(NAME hgraph_ts_v2_tests COMMAND hgraph_ts_v2_tests)
