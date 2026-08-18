cmake_minimum_required(VERSION 3.25)

project(hgraph_persistence_consumer LANGUAGES CXX)

include(CTest)

find_package(hgraph-persistence CONFIG REQUIRED)

add_executable(hgraph_persistence_consumer main.cpp)
target_compile_features(hgraph_persistence_consumer PRIVATE cxx_std_23)
target_link_libraries(hgraph_persistence_consumer PRIVATE hgraph::persistence)

# A wheel-installed SDK exposes Python conversion hooks whose symbols are
# supplied by the host interpreter for extension modules. A standalone native
# executable using that SDK must embed Python; a Python-disabled native install
# exports no nanobind target and remains interpreter-free.
if(TARGET hgraph::nanobind AND NOT TARGET Python::Python)
    find_package(Python 3.12 COMPONENTS Interpreter Development.Embed REQUIRED)
endif()
if(TARGET Python::Python)
    target_link_libraries(hgraph_persistence_consumer PRIVATE Python::Python)
    # GNU-like Linux linkers commonly enable --as-needed.  Python::Python is
    # encountered before the transitive hgraph shared libraries on the final
    # link line, so retain it until those libraries contribute their Python
    # symbol references.
    if(UNIX AND NOT APPLE)
        target_link_options(hgraph_persistence_consumer PRIVATE LINKER:--no-as-needed)
    endif()
endif()

add_test(NAME hgraph_persistence_consumer COMMAND hgraph_persistence_consumer)
