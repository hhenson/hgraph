cmake_minimum_required(VERSION 3.25)

project(hgraph_fabric_consumer LANGUAGES CXX)

include(CTest)

find_package(hgraph-fabric CONFIG REQUIRED)

add_executable(hgraph_fabric_consumer main.cpp)
target_compile_features(hgraph_fabric_consumer PRIVATE cxx_std_23)
target_link_libraries(hgraph_fabric_consumer PRIVATE hgraph::fabric)

if(TARGET hgraph::nanobind AND NOT TARGET Python::Python)
    find_package(Python 3.12 COMPONENTS Interpreter Development.Embed REQUIRED)
endif()
if(TARGET Python::Python)
    target_link_libraries(hgraph_fabric_consumer PRIVATE Python::Python)
    if(UNIX AND NOT APPLE)
        target_link_options(hgraph_fabric_consumer PRIVATE LINKER:--no-as-needed)
    endif()
endif()

add_test(NAME hgraph_fabric_consumer COMMAND hgraph_fabric_consumer)
