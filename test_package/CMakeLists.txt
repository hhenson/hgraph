cmake_minimum_required(VERSION 3.25)

project(hgraph_conan_consumer LANGUAGES CXX)

find_package(hgraph CONFIG REQUIRED)

add_executable(hgraph_conan_consumer main.cpp)
target_link_libraries(hgraph_conan_consumer PRIVATE hgraph::core)

# With the recipe's ``language`` option the package also carries ``hgl`` and
# HglLanguage.cmake; build an HGL package against it the way the user guide
# ("Building a package") describes.
option(HGRAPH_TEST_LANGUAGE "Build the hgl_add_module() consumer" OFF)
if(HGRAPH_TEST_LANGUAGE)
    include("${hgraph_DIR}/../hgl/HglLanguage.cmake")
    hgl_add_module(hgraph_conan_smoke STATIC HGL "${CMAKE_CURRENT_SOURCE_DIR}/smoke.hgl")
    add_executable(hgraph_conan_hgl_consumer hgl_consumer.cpp)
    target_link_libraries(hgraph_conan_hgl_consumer PRIVATE hgraph_conan_smoke)
endif()
