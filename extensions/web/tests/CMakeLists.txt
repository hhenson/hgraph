add_executable(hgraph_web_tests
    test_types.cpp
)

target_compile_features(hgraph_web_tests PRIVATE cxx_std_23)
target_link_libraries(hgraph_web_tests PRIVATE hgraph::web)

if(MSVC)
    target_compile_options(hgraph_web_tests PRIVATE /W4 /permissive-)
else()
    target_compile_options(hgraph_web_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_test(NAME hgraph_web_tests COMMAND hgraph_web_tests)
if(COMMAND hgraph_configure_test_runtime_paths)
    hgraph_configure_test_runtime_paths(hgraph_web_tests)
endif()
