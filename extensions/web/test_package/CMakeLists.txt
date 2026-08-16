cmake_minimum_required(VERSION 3.25)

project(hgraph_web_consumer LANGUAGES CXX)

include(CTest)

find_package(hgraph-web CONFIG REQUIRED)

add_executable(hgraph_web_consumer main.cpp)
target_compile_features(hgraph_web_consumer PRIVATE cxx_std_23)
target_link_libraries(hgraph_web_consumer PRIVATE hgraph::web)

add_test(NAME hgraph_web_consumer COMMAND hgraph_web_consumer)
