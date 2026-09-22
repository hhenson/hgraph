cmake_minimum_required(VERSION 3.25)
project(hgraph_arrow_dependency_consumer LANGUAGES NONE)

# Other SDK dependencies are already supplied by this consumer. Arrow uses
# separate fixture packages so the test cannot silently find a system package.
foreach(_target IN ITEMS fmt::fmt spdlog::spdlog simdjson::simdjson)
    add_library("${_target}" INTERFACE IMPORTED)
endforeach()
set(HGRAPH_FETCH_MISSING_DEPENDENCIES OFF)

foreach(_component IN ITEMS Arrow ArrowCompute ArrowAcero)
    if(_component STREQUAL "Arrow")
        set(_name arrow)
    elseif(_component STREQUAL "ArrowCompute")
        set(_name arrow_compute)
    else()
        set(_name arrow_acero)
    endif()
    set(_config_dir "${CMAKE_CURRENT_BINARY_DIR}/packages/${_component}")
    file(MAKE_DIRECTORY "${_config_dir}")
    # Deliberately not idempotent: rediscovering an existing component is a bug.
    file(WRITE "${_config_dir}/${_component}Config.cmake"
        "add_library(${_component}::${_name}_${ARROW_KIND} INTERFACE IMPORTED)\n"
        "set_property(TARGET ${_component}::${_name}_${ARROW_KIND} PROPERTY HGRAPH_TEST_PREEXISTING preserved)\n"
        "set_property(TARGET ${_component}::${_name}_${ARROW_KIND} PROPERTY INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}\")\n")
    set("${_component}_DIR" "${_config_dir}")
endforeach()

if(NOT PRELOAD STREQUAL "none")
    find_package(Arrow CONFIG REQUIRED)
endif()
if(PRELOAD STREQUAL "core_compute" OR PRELOAD STREQUAL "all")
    find_package(ArrowCompute CONFIG REQUIRED)
endif()
if(PRELOAD STREQUAL "core_acero" OR PRELOAD STREQUAL "all")
    find_package(ArrowAcero CONFIG REQUIRED)
endif()
set(_existing_targets "")
foreach(_target IN ITEMS Arrow::arrow ArrowCompute::arrow_compute ArrowAcero::arrow_acero)
    if(TARGET "${_target}_${ARROW_KIND}")
        list(APPEND _existing_targets "${_target}_${ARROW_KIND}")
    endif()
endforeach()

include("${HGRAPH_CMAKE_DIR}/HgraphNativeDependencies.cmake")

foreach(_target IN ITEMS Arrow::arrow ArrowCompute::arrow_compute ArrowAcero::arrow_acero)
    if(NOT TARGET "${_target}_${ARROW_KIND}")
        message(FATAL_ERROR "SDK discovery left missing component ${_target}_${ARROW_KIND}")
    endif()
endforeach()
foreach(_target IN LISTS _existing_targets)
    get_target_property(_marker "${_target}" HGRAPH_TEST_PREEXISTING)
    get_target_property(_type "${_target}" TYPE)
    get_target_property(_include "${_target}" INTERFACE_INCLUDE_DIRECTORIES)
    if(NOT _marker STREQUAL "preserved" OR NOT _type STREQUAL "INTERFACE_LIBRARY" OR
       NOT _include STREQUAL CMAKE_CURRENT_SOURCE_DIR)
        message(FATAL_ERROR "SDK discovery replaced existing target ${_target}")
    endif()
endforeach()
