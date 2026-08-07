add_executable(hgraph_kafka_tests
    test_service.cpp
)

target_compile_features(hgraph_kafka_tests PRIVATE cxx_std_23)
target_link_libraries(hgraph_kafka_tests PRIVATE hgraph::kafka)

if(MSVC)
    target_compile_options(hgraph_kafka_tests PRIVATE /W4 /permissive-)
else()
    target_compile_options(hgraph_kafka_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_test(NAME hgraph_kafka_tests COMMAND hgraph_kafka_tests)
