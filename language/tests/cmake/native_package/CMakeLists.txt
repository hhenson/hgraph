cmake_minimum_required(VERSION 3.25)

project(hgl_native_package_consumer LANGUAGES CXX)

if(NOT HGL_LANGUAGE_TARGETS)
    message(FATAL_ERROR "HGL_LANGUAGE_TARGETS is required")
endif()

include("${HGL_LANGUAGE_TARGETS}")

add_executable(hgl_native_package_consumer main.cpp)
target_compile_features(hgl_native_package_consumer PRIVATE cxx_std_23)
target_link_libraries(hgl_native_package_consumer PRIVATE hgl::native_package)
set_target_properties(hgl_native_package_consumer PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "$<1:${CMAKE_CURRENT_BINARY_DIR}/bin>")
