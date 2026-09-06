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

set(_bootstrap "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/_fixture_module.cpp")
file(READ "${_bootstrap}" _bootstrap_text)
if(NOT _bootstrap_text MATCHES "pkg::new_::register_operators\\(\\)")
    message(FATAL_ERROR "keyword module namespace was not escaped in '${_bootstrap}'")
endif()

add_custom_target(hgl_fixture_generate
    DEPENDS
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/include/unit.h"
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/src/unit.cpp"
        "${CMAKE_CURRENT_BINARY_DIR}/hgl/hgl_fixture/src/unit.hgl-module.json"
        "${_package_dir}/unit.py")
