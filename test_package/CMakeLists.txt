cmake_minimum_required(VERSION 3.25)

project(hgraph_conan_consumer LANGUAGES CXX)

find_package(hgraph CONFIG REQUIRED)

add_executable(hgraph_conan_consumer main.cpp)
target_link_libraries(hgraph_conan_consumer PRIVATE hgraph::core)
