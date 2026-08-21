cmake_minimum_required(VERSION 3.25)

project(hgraph_fabric VERSION 0.8.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)

option(HGRAPH_FABRIC_BUILD_PYTHON "Build the optional Python authoring bridge" OFF)
option(HGRAPH_FABRIC_WARNINGS_AS_ERRORS
    "Treat hgraph-fabric compiler warnings as errors"
    ${HGRAPH_WARNINGS_AS_ERRORS})

set(_hgraph_fabric_kafka_default OFF)
if(TARGET hgraph::kafka)
    set(_hgraph_fabric_kafka_default ON)
endif()
option(HGRAPH_FABRIC_BUILD_KAFKA
    "Build the optional graph-native Kafka transport adapter"
    ${_hgraph_fabric_kafka_default})

if(HGRAPH_FABRIC_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS
        Interpreter Development.Module Development.SABIModule REQUIRED)
endif()

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()
if(NOT TARGET hgraph::persistence)
    find_package(hgraph-persistence CONFIG REQUIRED)
endif()

add_library(hgraph_fabric
    src/config.cpp
    src/keys.cpp
    src/metadata_codec.cpp
    src/notifier.cpp
    src/operators.cpp
    src/planning.cpp
    src/publication.cpp
    src/resolution.cpp
    src/service.cpp
    src/service_transport.cpp
    src/subscription.cpp
    src/types.cpp
    src/value_builders.cpp
    src/impl/memory_notifier.cpp
)
add_library(hgraph::fabric ALIAS hgraph_fabric)

get_target_property(HGRAPH_FABRIC_LIBRARY_TYPE hgraph_fabric TYPE)
if(HGRAPH_FABRIC_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
    target_compile_definitions(hgraph_fabric PUBLIC HGRAPH_FABRIC_STATIC_DEFINE)
endif()

target_compile_features(hgraph_fabric PUBLIC cxx_std_23)
target_link_libraries(hgraph_fabric PUBLIC hgraph::core hgraph::persistence)
target_include_directories(hgraph_fabric
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
set_target_properties(hgraph_fabric PROPERTIES
    EXPORT_NAME fabric
    POSITION_INDEPENDENT_CODE ON
)

if(HGRAPH_FABRIC_BUILD_KAFKA)
    if(NOT TARGET hgraph::kafka)
        find_package(hgraph-kafka CONFIG REQUIRED)
    endif()
    add_library(hgraph_fabric_kafka src/kafka.cpp)
    add_library(hgraph::fabric_kafka ALIAS hgraph_fabric_kafka)
    get_target_property(HGRAPH_FABRIC_KAFKA_LIBRARY_TYPE hgraph_fabric_kafka TYPE)
    if(HGRAPH_FABRIC_KAFKA_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
        target_compile_definitions(hgraph_fabric_kafka PUBLIC HGRAPH_FABRIC_KAFKA_STATIC_DEFINE)
    endif()
    target_compile_features(hgraph_fabric_kafka PUBLIC cxx_std_23)
    target_link_libraries(hgraph_fabric_kafka PUBLIC hgraph::fabric hgraph::kafka)
    target_include_directories(hgraph_fabric_kafka
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    set_target_properties(hgraph_fabric_kafka PROPERTIES
        EXPORT_NAME fabric_kafka
        POSITION_INDEPENDENT_CODE ON
    )
    if(MSVC)
        target_compile_options(hgraph_fabric_kafka PRIVATE /W4 /permissive-)
        if(HGRAPH_FABRIC_WARNINGS_AS_ERRORS)
            target_compile_options(hgraph_fabric_kafka PRIVATE /WX)
        endif()
    else()
        target_compile_options(hgraph_fabric_kafka PRIVATE -Wall -Wextra -Wpedantic)
        if(HGRAPH_FABRIC_WARNINGS_AS_ERRORS)
            target_compile_options(hgraph_fabric_kafka PRIVATE -Werror)
        endif()
    endif()
endif()

if(MSVC)
    target_compile_options(hgraph_fabric PRIVATE /W4 /permissive-)
    if(HGRAPH_FABRIC_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_fabric PRIVATE /WX)
    endif()
else()
    target_compile_options(hgraph_fabric PRIVATE -Wall -Wextra -Wpedantic)
    if(HGRAPH_FABRIC_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_fabric PRIVATE -Werror)
    endif()
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(HGRAPH_FABRIC_BUILD_PYTHON)
    if(NOT COMMAND hgraph_add_python_module OR NOT TARGET hgraph::nanobind)
        message(FATAL_ERROR
            "HGRAPH_FABRIC_BUILD_PYTHON requires a Python-enabled installed hgraph SDK")
    endif()
    hgraph_add_python_module(_hgraph_fabric STABLE_ABI NOMINSIZE src/python_module.cpp)
    target_link_libraries(_hgraph_fabric PRIVATE hgraph::fabric)
    if(APPLE)
        set_target_properties(_hgraph_fabric PROPERTIES
            INSTALL_RPATH "@loader_path/../${CMAKE_INSTALL_LIBDIR};@loader_path/../pyarrow")
    elseif(UNIX)
        set_target_properties(_hgraph_fabric PROPERTIES
            INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR};$ORIGIN/../pyarrow")
    endif()
    install(TARGETS _hgraph_fabric
        COMPONENT Python
        LIBRARY DESTINATION hgraph_fabric
        RUNTIME DESTINATION hgraph_fabric
    )
endif()

install(TARGETS hgraph_fabric
    EXPORT hgraphFabricTargets
    COMPONENT Development
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
if(TARGET hgraph_fabric_kafka)
    install(TARGETS hgraph_fabric_kafka
        EXPORT hgraphFabricTargets
        COMPONENT Development
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT Development FILES_MATCHING PATTERN "*.h")

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraph-fabricConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph-fabricConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/hgraph-fabricConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-fabric
)

install(EXPORT hgraphFabricTargets
    FILE hgraphFabricTargets.cmake
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-fabric
    COMPONENT Development
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraph-fabricConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraph-fabricConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-fabric
    COMPONENT Development
)
