cmake_minimum_required(VERSION 3.25)

project(hgraph_kafka_consumer LANGUAGES CXX)

include(CTest)

find_package(hgraph-kafka CONFIG REQUIRED)

add_executable(hgraph_kafka_consumer main.cpp)
target_compile_features(hgraph_kafka_consumer PRIVATE cxx_std_23)
target_link_libraries(hgraph_kafka_consumer PRIVATE hgraph::kafka)

add_test(NAME hgraph_kafka_consumer COMMAND hgraph_kafka_consumer)
