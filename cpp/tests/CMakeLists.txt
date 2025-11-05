cmake_minimum_required(VERSION 3.25)

project(hgraph_ts_v2_tests)

add_executable(hgraph_ts_v2_tests
        test_time_series_v2.cpp
        test_ts_value_anyvalue.cpp
)

# Link against the core hgraph library which includes all C++ sources
# This avoids manually managing individual source files
# Note: Python libraries are needed because hgraph_core includes Python bindings
target_link_libraries(hgraph_ts_v2_tests PRIVATE
    hgraph_core
    Catch2::Catch2WithMain
    nanobind-static
    Python::Python
)

target_compile_features(hgraph_ts_v2_tests PRIVATE cxx_std_23)

# Note: HGRAPH_WITH_PYTHON_TOSTRING is NOT enabled for tests
# Tests don't use Python-backed string conversion

include(CTest)
# Register the test executable explicitly with CTest
add_test(NAME hgraph_ts_v2_tests COMMAND hgraph_ts_v2_tests)
