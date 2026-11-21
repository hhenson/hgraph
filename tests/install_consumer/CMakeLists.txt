cmake_minimum_required(VERSION 3.25)

project(hgraph_install_consumer LANGUAGES CXX)

add_subdirectory(independent)

function(find_initial_sdk)
    find_package(hgraph CONFIG REQUIRED)
endfunction()
find_initial_sdk()
get_target_property(original_links hgraph::options INTERFACE_LINK_LIBRARIES)
find_package(hgraph CONFIG REQUIRED)
add_subdirectory(nested)
get_target_property(current_links hgraph::options INTERFACE_LINK_LIBRARIES)
if(NOT current_links STREQUAL original_links)
    message(FATAL_ERROR "Repeated hgraph lookups changed the SDK link interface")
endif()
enable_testing()

add_executable(hgraph_compiled_try_except_consumer compiled_try_except.cpp)
add_executable(hgraph_optional_bundle_consumer optional_bundle_member.cpp)
add_executable(hgraph_nullable_reference_consumer nullable_reference.cpp)
foreach(consumer IN ITEMS hgraph_compiled_try_except_consumer hgraph_optional_bundle_consumer hgraph_nullable_reference_consumer)
    target_link_libraries(${consumer} PRIVATE hgraph::wiring)
    if(COMMAND hgraph_link_python_embedding)
        hgraph_link_python_embedding(${consumer})
    endif()
endforeach()
add_test(NAME hgraph.install_consumer.scalar_streams COMMAND hgraph_scalar_stream_consumer)
add_test(NAME hgraph.install_consumer.mock_executor COMMAND hgraph_mock_executor_consumer)
add_test(NAME hgraph.install_consumer.compiled_try_except COMMAND hgraph_compiled_try_except_consumer)
add_test(NAME hgraph.install_consumer.optional_bundle COMMAND hgraph_optional_bundle_consumer)
add_test(NAME hgraph.install_consumer.nullable_reference COMMAND hgraph_nullable_reference_consumer)

if(DEFINED hgraph_WITH_ARROW AND NOT hgraph_WITH_ARROW)
    add_executable(hgraph_runtime_consumer ../cpp/runtime_consumer.cpp)
    target_link_libraries(hgraph_runtime_consumer PRIVATE hgraph::wiring)
    add_test(NAME hgraph.install_consumer.runtime COMMAND hgraph_runtime_consumer)
    if(WIN32)
        set_tests_properties(hgraph.install_consumer.runtime hgraph.install_consumer.scalar_streams
            hgraph.install_consumer.mock_executor hgraph.install_consumer.compiled_try_except
            hgraph.install_consumer.optional_bundle hgraph.install_consumer.nullable_reference PROPERTIES ENVIRONMENT_MODIFICATION
            "PATH=path_list_prepend:$<TARGET_FILE_DIR:hgraph::runtime>")
    endif()
    return()
endif()

target_link_libraries(hgraph_optional_bundle_consumer PRIVATE hgraph::core)

add_executable(hgraph_install_consumer main.cpp fabric_extension_probe.cpp spawn_probe.cpp)
target_link_libraries(hgraph_install_consumer PRIVATE hgraph::core)
if(COMMAND hgraph_link_python_embedding)
    # Python-enabled wheel runtimes resolve their stable-ABI symbols from an
    # importing interpreter. This is a native executable, so supply that host
    # explicitly without imposing libpython on extension-module consumers.
    hgraph_link_python_embedding(hgraph_install_consumer)
endif()

add_test(NAME hgraph.install_consumer COMMAND hgraph_install_consumer)

add_executable(hgraph_arrow_abi_check ../cpp/arrow_abi_check.cpp)
target_link_libraries(hgraph_arrow_abi_check PRIVATE hgraph::core)
if(COMMAND hgraph_link_python_embedding)
    hgraph_link_python_embedding(hgraph_arrow_abi_check)
endif()
add_test(NAME hgraph.install_consumer.arrow_abi COMMAND hgraph_arrow_abi_check)

add_executable(hgraph_compiled_subgraph_consumer compiled_subgraph.cpp)
target_link_libraries(hgraph_compiled_subgraph_consumer PRIVATE hgraph::wiring)
if(COMMAND hgraph_link_python_embedding)
    hgraph_link_python_embedding(hgraph_compiled_subgraph_consumer)
endif()
add_test(NAME hgraph.install_consumer.compiled_subgraph COMMAND hgraph_compiled_subgraph_consumer)
