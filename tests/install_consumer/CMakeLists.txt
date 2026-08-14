cmake_minimum_required(VERSION 3.25)

project(hgraph_install_consumer LANGUAGES CXX)

find_package(hgraph CONFIG REQUIRED)

add_executable(hgraph_install_consumer main.cpp)
target_link_libraries(hgraph_install_consumer PRIVATE hgraph::core)
if(COMMAND hgraph_link_python_embedding)
    # Python-enabled wheel runtimes resolve their stable-ABI symbols from an
    # importing interpreter. This is a native executable, so supply that host
    # explicitly without imposing libpython on extension-module consumers.
    hgraph_link_python_embedding(hgraph_install_consumer)
endif()

enable_testing()
add_test(NAME hgraph.install_consumer COMMAND hgraph_install_consumer)
