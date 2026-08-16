add_executable(hgraph_web_tests
    test_types.cpp
)
add_executable(hgraph_web_service_tests
    test_service.cpp
)

foreach(target hgraph_web_tests hgraph_web_service_tests)
    target_compile_features(${target} PRIVATE cxx_std_23)
    target_link_libraries(${target} PRIVATE hgraph::web)

    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(NAME ${target} COMMAND ${target})
    if(COMMAND hgraph_configure_test_runtime_paths)
        hgraph_configure_test_runtime_paths(${target})
    endif()
endforeach()
