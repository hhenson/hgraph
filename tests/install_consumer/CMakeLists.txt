cmake_minimum_required(VERSION 3.25)

project(hgraph_install_consumer LANGUAGES CXX)

find_package(hgraph CONFIG REQUIRED)

add_executable(hgraph_install_consumer main.cpp)
target_link_libraries(hgraph_install_consumer PRIVATE hgraph::core)

enable_testing()
add_test(NAME hgraph.install_consumer COMMAND hgraph_install_consumer)
