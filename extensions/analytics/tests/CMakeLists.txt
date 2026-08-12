add_executable(hgraph_analytics_tests
    test_pct_change.cpp
)

add_executable(hgraph_analytics_array_tests
    test_array_operators.cpp
)

add_executable(hgraph_analytics_statistics_tests
    test_statistics.cpp
)

foreach(_hgraph_analytics_test IN ITEMS
        hgraph_analytics_tests
        hgraph_analytics_array_tests
        hgraph_analytics_statistics_tests)
    target_compile_features(${_hgraph_analytics_test} PRIVATE cxx_std_23)
    target_link_libraries(${_hgraph_analytics_test} PRIVATE hgraph::analytics)

    if(MSVC)
        target_compile_options(${_hgraph_analytics_test} PRIVATE /W4 /permissive-)
    else()
        target_compile_options(${_hgraph_analytics_test} PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(NAME ${_hgraph_analytics_test} COMMAND ${_hgraph_analytics_test})
    if(COMMAND hgraph_configure_test_runtime_paths)
        hgraph_configure_test_runtime_paths(${_hgraph_analytics_test})
    endif()
endforeach()
