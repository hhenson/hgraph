cmake_minimum_required(VERSION 3.25)

project(hgraph_install_consumer LANGUAGES CXX)

find_package(hgraph CONFIG REQUIRED)
if(DEFINED hgraph_WITH_ARROW AND NOT hgraph_WITH_ARROW)
    add_executable(hgraph_runtime_consumer ../cpp/runtime_consumer.cpp)
    target_link_libraries(hgraph_runtime_consumer PRIVATE hgraph::wiring)
    enable_testing()
    add_test(NAME hgraph.install_consumer.runtime COMMAND hgraph_runtime_consumer)
    if(WIN32)
        set_tests_properties(hgraph.install_consumer.runtime PROPERTIES ENVIRONMENT_MODIFICATION
            "PATH=path_list_prepend:$<TARGET_FILE_DIR:hgraph::runtime>")
    endif()
    return()
endif()

add_executable(hgraph_install_consumer main.cpp fabric_extension_probe.cpp spawn_probe.cpp)
target_link_libraries(hgraph_install_consumer PRIVATE hgraph::core)
if(COMMAND hgraph_link_python_embedding)
    # Python-enabled wheel runtimes resolve their stable-ABI symbols from an
    # importing interpreter. This is a native executable, so supply that host
    # explicitly without imposing libpython on extension-module consumers.
    hgraph_link_python_embedding(hgraph_install_consumer)
endif()

enable_testing()
add_test(NAME hgraph.install_consumer COMMAND hgraph_install_consumer)

add_executable(hgraph_arrow_abi_check ../cpp/arrow_abi_check.cpp)
target_link_libraries(hgraph_arrow_abi_check PRIVATE hgraph::core)
if(COMMAND hgraph_link_python_embedding)
    hgraph_link_python_embedding(hgraph_arrow_abi_check)
endif()
add_test(NAME hgraph.install_consumer.arrow_abi COMMAND hgraph_arrow_abi_check)
