add_executable(hgraph_kafka_tests
    test_service.cpp
)

target_compile_features(hgraph_kafka_tests PRIVATE cxx_std_23)
target_link_libraries(hgraph_kafka_tests PRIVATE hgraph::kafka)
target_include_directories(hgraph_kafka_tests
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

if(MSVC)
    target_compile_options(hgraph_kafka_tests PRIVATE /W4 /permissive-)
else()
    target_compile_options(hgraph_kafka_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_test(NAME hgraph_kafka_tests COMMAND hgraph_kafka_tests)
if(COMMAND hgraph_configure_test_runtime_paths)
    hgraph_configure_test_runtime_paths(hgraph_kafka_tests)
endif()

add_executable(hgraph_kafka_transport_perf
    transport_perf.cpp
)

target_compile_features(hgraph_kafka_transport_perf PRIVATE cxx_std_23)
target_link_libraries(hgraph_kafka_transport_perf PRIVATE hgraph::kafka)
target_include_directories(hgraph_kafka_transport_perf
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

if(MSVC)
    target_compile_options(hgraph_kafka_transport_perf PRIVATE /W4 /permissive-)
else()
    target_compile_options(hgraph_kafka_transport_perf PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # This benchmark replaces global new/delete to count allocations. GCC can
    # pair an inlined replacement new with the replacement delete and report
    # a spurious mismatched-new-delete warning.
    target_compile_options(hgraph_kafka_transport_perf PRIVATE -Wno-mismatched-new-delete)
endif()
