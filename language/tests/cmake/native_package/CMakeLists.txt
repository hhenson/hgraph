cmake_minimum_required(VERSION 3.25)

project(hgl_native_package_consumer LANGUAGES CXX)

if(NOT HGL_LANGUAGE_CMAKE)
    message(FATAL_ERROR "HGL_LANGUAGE_CMAKE is required")
endif()

find_package(hgraph CONFIG REQUIRED)
include("${HGL_LANGUAGE_CMAKE}")

if(NOT TARGET hgl::core_native)
    message(FATAL_ERROR "the installed HGL SDK does not define hgl::core_native")
endif()
get_target_property(_core_native_descriptors hgl::core_native HGL_MODULE_DESCRIPTORS)
if(NOT _core_native_descriptors OR NOT EXISTS "${_core_native_descriptors}")
    message(FATAL_ERROR "hgl::core_native has no installed module descriptor: '${_core_native_descriptors}'")
endif()

if(NOT TARGET hgl::standard_library)
    message(FATAL_ERROR "the installed HGL SDK does not define hgl::standard_library")
endif()
get_target_property(_standard_library_descriptors hgl::standard_library HGL_MODULE_DESCRIPTORS)
if(NOT _standard_library_descriptors)
    message(FATAL_ERROR
        "hgl::standard_library has no installed module descriptor: '${_standard_library_descriptors}'")
endif()
foreach(_descriptor IN LISTS _standard_library_descriptors)
    if(NOT EXISTS "${_descriptor}")
        message(FATAL_ERROR "hgl::standard_library module descriptor is missing: '${_descriptor}'")
    endif()
endforeach()
list(LENGTH _standard_library_descriptors _standard_module_count)
if(NOT _standard_module_count EQUAL 2)
    message(FATAL_ERROR "hgl::standard_library must publish both standard and operator module descriptors")
endif()

add_executable(hgl_native_package_consumer main.cpp)
target_compile_features(hgl_native_package_consumer PRIVATE cxx_std_23)
target_link_libraries(hgl_native_package_consumer PRIVATE
    hgl::native_package
    hgl::core_native
    hgl::standard_library)
set_target_properties(hgl_native_package_consumer PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "$<1:${CMAKE_CURRENT_BINARY_DIR}/bin>")

if(NOT HGL_STDLIB_SOURCE)
    message(FATAL_ERROR "HGL_STDLIB_SOURCE is required")
endif()
hgl_add_module(rebuilt_native STATIC
    HGL ${HGL_STDLIB_SOURCE}/native.hgl
    PARTS
        ${HGL_STDLIB_SOURCE}/native/sequences.hgl
        ${HGL_STDLIB_SOURCE}/native/sets_maps.hgl
        ${HGL_STDLIB_SOURCE}/native/windows.hgl
        ${HGL_STDLIB_SOURCE}/native/scalar_values.hgl
)
add_executable(hgl_native_parts_consumer parts.cpp)
target_link_libraries(hgl_native_parts_consumer PRIVATE rebuilt_native)
set_target_properties(hgl_native_parts_consumer PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "$<1:${CMAKE_CURRENT_BINARY_DIR}/bin>")

# Three independently compiled HGL modules must exchange contracts using the
# installed SDK, without private compiler headers or source-tree include paths.
set(_imported_operator_sources "${CMAKE_CURRENT_SOURCE_DIR}/../../codegen/imported-operators")
hgl_add_module(imported_contracts STATIC HGL "${_imported_operator_sources}/contracts.hgl")
hgl_add_module(imported_provider STATIC HGL "${_imported_operator_sources}/provider.hgl"
    LINK_LIBRARIES imported_contracts)
hgl_add_module(imported_consumer STATIC HGL "${_imported_operator_sources}/consumer.hgl"
    LINK_LIBRARIES imported_contracts imported_provider)
add_executable(hgl_imported_operator_consumer imported.cpp)
target_link_libraries(hgl_imported_operator_consumer PRIVATE imported_consumer)
set_target_properties(hgl_imported_operator_consumer PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "$<1:${CMAKE_CURRENT_BINARY_DIR}/bin>")
