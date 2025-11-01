cmake_minimum_required(VERSION 3.25)

project(hgraph_ts_v2_tests)

add_executable(hgraph_ts_v2_tests
    test_time_series_v2.cpp
    ../src/cpp/types/v2/time_series.cpp
)

# Inherit include dirs from parent (Python, nanobind, project includes)
# Link Catch2 and fmt if available via parent FetchContent

target_link_libraries(hgraph_ts_v2_tests PRIVATE Catch2::Catch2WithMain fmt::fmt nanobind-static Python::Python)

target_compile_features(hgraph_ts_v2_tests PRIVATE cxx_std_23)

include(CTest)
# Register the test executable explicitly with CTest
add_test(NAME hgraph_ts_v2_tests COMMAND hgraph_ts_v2_tests)
