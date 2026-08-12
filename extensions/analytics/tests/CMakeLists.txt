add_executable(hgraph_analytics_tests
    test_pct_change.cpp
)

target_compile_features(hgraph_analytics_tests PRIVATE cxx_std_23)
target_link_libraries(hgraph_analytics_tests PRIVATE hgraph::analytics)

if(MSVC)
    target_compile_options(hgraph_analytics_tests PRIVATE /W4 /permissive-)
else()
    target_compile_options(hgraph_analytics_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_test(NAME hgraph_analytics_tests COMMAND hgraph_analytics_tests)
if(COMMAND hgraph_configure_test_runtime_paths)
    hgraph_configure_test_runtime_paths(hgraph_analytics_tests)
endif()
