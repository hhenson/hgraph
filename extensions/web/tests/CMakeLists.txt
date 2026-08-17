add_executable(hgraph_web_tests
    test_types.cpp
)
add_executable(hgraph_web_service_tests
    test_service.cpp
)
add_executable(hgraph_web_route_table_tests
    test_route_table.cpp
)
add_executable(hgraph_web_bridge_tests
    test_bridge.cpp
)
add_executable(hgraph_web_h2_engine_tests
    test_h2_engine.cpp
)
add_executable(hgraph_web_loopback_tests
    test_loopback.cpp
)
add_executable(hgraph_web_client_loopback_tests
    test_client_loopback.cpp
)
add_executable(hgraph_web_h2_loopback_tests
    test_h2_loopback.cpp
)
# The loopback oracle is an independent Beast synchronous client, so this
# test target reaches the (otherwise PRIVATE) header-only Boost targets.
target_link_libraries(hgraph_web_loopback_tests
    PRIVATE ${HGRAPH_WEB_BOOST_TARGETS})
# The route table is an implementation detail of the extension, so its header
# is reached the same way the extension's own sources reach it.
target_include_directories(hgraph_web_route_table_tests
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
# The bridge is likewise src-internal; its teardown contract is unit-tested
# directly (late reserved completions after stop must reject, not throw).
target_include_directories(hgraph_web_bridge_tests
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
# The H2 engine test converses with the engine through a genuine nghttp2
# client session, so it links the same nghttp2 the extension uses.
target_include_directories(hgraph_web_h2_engine_tests
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_link_libraries(hgraph_web_h2_engine_tests
    PRIVATE ${HGRAPH_WEB_NGHTTP2_TARGET})

foreach(target hgraph_web_tests hgraph_web_service_tests hgraph_web_route_table_tests hgraph_web_bridge_tests hgraph_web_h2_engine_tests hgraph_web_loopback_tests hgraph_web_client_loopback_tests hgraph_web_h2_loopback_tests)
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
