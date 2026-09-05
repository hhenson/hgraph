# The installed-SDK consumer every channel builds after installing (RFC 0032):
# the shape the user guide documents under "Building a package" -- the SDK
# through find_package(hgraph), the helper beside it, one hgl_add_module()
# from the smoke module. Configuring proves the installed hgraphConfig.cmake
# resolves every dependency it names on that channel; building and running
# prove the installed compiler and headers produce a working native package.
cmake_minimum_required(VERSION 3.25)

project(hgl_smoke_consumer LANGUAGES CXX)

find_package(hgraph CONFIG REQUIRED)
include("${hgraph_DIR}/../hgl/HglLanguage.cmake")

hgl_add_module(smoke_module STATIC HGL "${CMAKE_CURRENT_SOURCE_DIR}/../smoke.hgl")

add_executable(hgl_smoke_consumer main.cpp)
target_link_libraries(hgl_smoke_consumer PRIVATE smoke_module)
if(COMMAND hgraph_link_python_embedding)
    # Only a Python-enabled SDK defines this; the native channels do not.
    hgraph_link_python_embedding(hgl_smoke_consumer)
endif()

enable_testing()
add_test(NAME hgl.smoke_consumer COMMAND hgl_smoke_consumer)
