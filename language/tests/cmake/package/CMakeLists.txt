cmake_minimum_required(VERSION 3.25)

project(hgl_package_helper_test LANGUAGES CXX)

if(NOT HGL_LANGUAGE_CMAKE OR NOT HGL_EXECUTABLE OR NOT NATIVE_DESCRIPTOR)
    message(FATAL_ERROR "HGL_LANGUAGE_CMAKE, HGL_EXECUTABLE and NATIVE_DESCRIPTOR are required")
endif()

# Minimal imported SDK surface needed to configure hgl_add_module. Nothing is
# compiled in this fixture; its generation target exercises the custom command.
add_library(hgl_test_core INTERFACE)
add_library(hgraph::core ALIAS hgl_test_core)
add_library(hgl_test_nanobind INTERFACE)
add_library(hgraph::nanobind ALIAS hgl_test_nanobind)

function(hgraph_add_python_module name)
    cmake_parse_arguments(PARSE_ARGV 1 _module "STABLE_ABI;NOMINSIZE" "" "")
    add_library(${name} MODULE ${_module_UNPARSED_ARGUMENTS})
endfunction()

include("${HGL_LANGUAGE_CMAKE}")

add_library(hgl_test_native INTERFACE)
set_property(TARGET hgl_test_native PROPERTY HGL_MODULE_DESCRIPTORS "${NATIVE_DESCRIPTOR}")

set(_package_dir "${CMAKE_CURRENT_BINARY_DIR}/python/_fixture")
hgl_add_module(hgl_fixture STATIC
    HGL unit.hgl
    LINK_LIBRARIES hgl_test_native
    PYTHON_MODULE _fixture
    PYTHON_PACKAGE_DIR "${_package_dir}")

# A source-native declaration emits an ordinary out-of-line C++ symbol. Shared
# HGL packages must export those symbols from their Windows DLL so another HGL
# target can link a descriptor-selected call to them.
hgl_add_module(hgl_shared_fixture SHARED
    HGL unit.hgl
    LINK_LIBRARIES hgl_test_native)
get_target_property(_shared_exports hgl_shared_fixture WINDOWS_EXPORT_ALL_SYMBOLS)
if(NOT _shared_exports)
    message(FATAL_ERROR "shared HGL modules must export generated symbols on Windows")
endif()

# PARTS contributes several files to one nominal module, one descriptor, and
# one generated C++ artifact set named after the anchor source.
hgl_add_module(hgl_parts_fixture STATIC
    HGL parts.hgl
    PARTS part-implementation.hgl
    PYTHON_MODULE _parts
    PYTHON_PACKAGE_DIR "${CMAKE_CURRENT_BINARY_DIR}/python/_parts")
get_target_property(_part_descriptors hgl_parts_fixture HGL_MODULE_DESCRIPTORS)
set(_expected_part_descriptor "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_parts_fixture/src/parts.hgl-module.json")
if(NOT _part_descriptors STREQUAL _expected_part_descriptor)
    message(FATAL_ERROR
        "module-parts descriptor property is '${_part_descriptors}', expected '${_expected_part_descriptor}'")
endif()
get_target_property(_part_bootstrap _parts HGL_PYTHON_BOOTSTRAP)

get_target_property(_library_dir _fixture LIBRARY_OUTPUT_DIRECTORY)
get_target_property(_runtime_dir _fixture RUNTIME_OUTPUT_DIRECTORY)
get_target_property(_descriptors hgl_fixture HGL_MODULE_DESCRIPTORS)
set(_expected_dir "$<1:${_package_dir}>")
if(NOT _library_dir STREQUAL _expected_dir OR NOT _runtime_dir STREQUAL _expected_dir)
    message(FATAL_ERROR
        "native module output is not multi-config safe: library='${_library_dir}', runtime='${_runtime_dir}'")
endif()
set(_expected_descriptor "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/src/unit.hgl-module.json")
if(NOT _descriptors STREQUAL _expected_descriptor)
    message(FATAL_ERROR
        "generated descriptor target property is '${_descriptors}', expected '${_expected_descriptor}'")
endif()

# The Python bootstrap is generated at build time from the descriptor, so the
# namespace spelling of a reserved segment (`hgl_state`) is checked by
# package_test.cmake after generation, not here.
get_target_property(_bootstrap _fixture HGL_PYTHON_BOOTSTRAP)
if(NOT _bootstrap STREQUAL "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/_fixture_module.cpp")
    message(FATAL_ERROR "python bootstrap property is '${_bootstrap}'")
endif()

add_custom_target(hgl_fixture_generate
    DEPENDS
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/include/unit.h"
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/src/unit.cpp"
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/src/unit.hgl-module.json"
        "${_package_dir}/unit.py"
        "${_bootstrap}"
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_parts_fixture/include/parts.h"
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_parts_fixture/src/parts.cpp"
        "${_expected_part_descriptor}"
        "${CMAKE_CURRENT_BINARY_DIR}/python/_parts/parts.py"
        "${_part_bootstrap}")
