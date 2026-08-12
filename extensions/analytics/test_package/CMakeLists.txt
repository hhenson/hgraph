cmake_minimum_required(VERSION 3.25)

project(hgraph_analytics_consumer LANGUAGES CXX)

include(CTest)

find_package(hgraph-analytics CONFIG REQUIRED)

add_executable(hgraph_analytics_consumer main.cpp)
target_compile_features(hgraph_analytics_consumer PRIVATE cxx_std_23)
target_link_libraries(hgraph_analytics_consumer PRIVATE hgraph::analytics)

# A wheel-installed SDK exposes Python conversion hooks whose symbols are
# supplied by the host interpreter for extension modules. A standalone native
# executable using that SDK must embed Python; a Python-disabled native install
# exports no nanobind target and remains interpreter-free.
if(TARGET hgraph::nanobind AND NOT TARGET Python::Python)
    find_package(Python 3.12 COMPONENTS Interpreter Development.Embed REQUIRED)
endif()
if(TARGET Python::Python)
    target_link_libraries(hgraph_analytics_consumer PRIVATE Python::Python)
endif()

add_test(NAME hgraph_analytics_consumer COMMAND hgraph_analytics_consumer)
