cmake_minimum_required(VERSION 3.26)
project(runtime_contract_probe LANGUAGES CXX)
find_package(hgraph CONFIG REQUIRED)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Embed)
add_executable(runtime_contract_probe native_probe.cpp)
target_compile_features(runtime_contract_probe PRIVATE cxx_std_23)
target_link_libraries(runtime_contract_probe PRIVATE hgraph::core Python::Python)
add_executable(runtime_nested_probe native_nested_probe.cpp)
target_compile_features(runtime_nested_probe PRIVATE cxx_std_23)
target_link_libraries(runtime_nested_probe PRIVATE hgraph::core Python::Python)
enable_testing()
add_test(NAME fixed_native_nested_wiring
    COMMAND "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/native_nested.py"
            --executable "$<TARGET_FILE:runtime_nested_probe>")
