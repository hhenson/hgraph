cmake_minimum_required(VERSION 3.26)
project(runtime_contract_probe LANGUAGES CXX)
find_package(hgraph CONFIG REQUIRED)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Embed)
add_executable(runtime_contract_probe native_probe.cpp)
target_compile_features(runtime_contract_probe PRIVATE cxx_std_23)
target_link_libraries(runtime_contract_probe PRIVATE hgraph::core Python::Python)
add_executable(runtime_nested_probe native_nested_probe.cpp)
get_target_property(sdk_includes hgraph::options INTERFACE_INCLUDE_DIRECTORIES)
foreach(directory IN LISTS sdk_includes)
    if(EXISTS "${directory}/hgraph/types/static_node.h")
        set(sdk_include_root "${directory}")
        break()
    endif()
endforeach()
if(NOT sdk_include_root)
    message(FATAL_ERROR "The native probe needs an installed hgraph SDK include root")
endif()
file(GLOB_RECURSE sdk_headers CONFIGURE_DEPENDS "${sdk_include_root}/*")
add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/native_build_identity.h"
    COMMAND "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/native_nested_build.py"
            "${sdk_include_root}" "${CMAKE_CURRENT_SOURCE_DIR}"
            "${CMAKE_CURRENT_BINARY_DIR}/native_build_identity.h"
    DEPENDS native_nested_build.py native_nested_probe.cpp native_loaded_libraries.h observed.json ${sdk_headers}
    VERBATIM)
target_sources(runtime_nested_probe PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/native_build_identity.h")
target_include_directories(runtime_nested_probe PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_compile_features(runtime_nested_probe PRIVATE cxx_std_23)
target_link_libraries(runtime_nested_probe PRIVATE hgraph::core Python::Python)
enable_testing()
add_test(NAME fixed_native_nested_wiring
    COMMAND "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/native_nested.py"
            --executable "$<TARGET_FILE:runtime_nested_probe>")
