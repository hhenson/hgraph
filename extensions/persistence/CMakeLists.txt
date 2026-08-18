cmake_minimum_required(VERSION 3.25)

project(hgraph_persistence VERSION 0.8.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)
include(FetchContent)

option(HGRAPH_PERSISTENCE_BUILD_PYTHON "Build the optional Python bridge module" OFF)
option(HGRAPH_PERSISTENCE_WARNINGS_AS_ERRORS
    "Treat hgraph-persistence compiler warnings as errors"
    ${HGRAPH_WARNINGS_AS_ERRORS})

if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS
        Interpreter Development.Module Development.SABIModule REQUIRED)
endif()

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()

include(cmake/hgraph_persistence_arrow.cmake)
if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    hgraph_persistence_find_arrow(PREFER_PYARROW)
else()
    hgraph_persistence_find_arrow()
endif()

if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    get_target_property(_hgraph_persistence_core_links hgraph::options INTERFACE_LINK_LIBRARIES)
    if("Python::Python" IN_LIST _hgraph_persistence_core_links)
        message(FATAL_ERROR
            "The selected hgraph SDK embeds a specific Python interpreter and "
            "cannot produce an ABI3 hgraph-persistence wheel; use the SDK "
            "installed by an hgraph stable-ABI wheel")
    endif()
endif()

add_library(hgraph_persistence
    src/frame_store.cpp
    src/recording_store.cpp
    src/record_replay_frame_impl.cpp
    src/registration.cpp
)
add_library(hgraph::persistence ALIAS hgraph_persistence)

get_target_property(HGRAPH_PERSISTENCE_LIBRARY_TYPE hgraph_persistence TYPE)
if(HGRAPH_PERSISTENCE_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
    target_compile_definitions(hgraph_persistence PUBLIC HGRAPH_PERSISTENCE_STATIC_DEFINE)
endif()

target_compile_features(hgraph_persistence PUBLIC cxx_std_23)
target_link_libraries(hgraph_persistence PUBLIC hgraph::core)
if(TARGET Arrow::arrow_shared)
    target_link_libraries(hgraph_persistence PRIVATE Arrow::arrow_shared)
else()
    target_link_libraries(hgraph_persistence PRIVATE Arrow::arrow_static)
endif()
if(HGRAPH_PERSISTENCE_WITH_PARQUET)
    target_compile_definitions(hgraph_persistence PRIVATE HGRAPH_WITH_PARQUET=1)
    if(TARGET Parquet::parquet_shared)
        target_link_libraries(hgraph_persistence PRIVATE Parquet::parquet_shared)
    elseif(TARGET Parquet::parquet_static)
        target_link_libraries(hgraph_persistence PRIVATE Parquet::parquet_static)
    endif()
endif()
if(HGRAPH_PERSISTENCE_WITH_S3)
    target_compile_definitions(hgraph_persistence PRIVATE HGRAPH_WITH_S3=1)
endif()
target_include_directories(hgraph_persistence
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
set_target_properties(hgraph_persistence PROPERTIES
    EXPORT_NAME persistence
    POSITION_INDEPENDENT_CODE ON
)

if(MSVC)
    target_compile_options(hgraph_persistence PRIVATE /W4 /permissive-)
    if(HGRAPH_PERSISTENCE_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_persistence PRIVATE /WX)
    endif()
else()
    target_compile_options(hgraph_persistence PRIVATE -Wall -Wextra -Wpedantic)
    if(HGRAPH_PERSISTENCE_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_persistence PRIVATE -Werror)
    endif()
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    if(NOT COMMAND hgraph_add_python_module OR NOT TARGET hgraph::nanobind)
        message(FATAL_ERROR
            "HGRAPH_PERSISTENCE_BUILD_PYTHON requires a Python-enabled installed hgraph SDK")
    endif()
    hgraph_add_python_module(_hgraph_persistence STABLE_ABI NOMINSIZE src/python_module.cpp)
    target_link_libraries(_hgraph_persistence PRIVATE hgraph::persistence)
    # One more `../` than hgraph_add_python_module's default: the module
    # installs into hgraph_persistence/, and both the core shared runtime
    # (site-packages lib/) and pyarrow's Arrow must resolve from there.
    if(APPLE)
        set_target_properties(_hgraph_persistence PROPERTIES
            INSTALL_RPATH "@loader_path/../${CMAKE_INSTALL_LIBDIR};@loader_path/../pyarrow")
    elseif(UNIX)
        set_target_properties(_hgraph_persistence PROPERTIES
            INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR};$ORIGIN/../pyarrow")
    endif()
    install(TARGETS _hgraph_persistence
        COMPONENT Python
        LIBRARY DESTINATION hgraph_persistence
        RUNTIME DESTINATION hgraph_persistence
    )
endif()

install(TARGETS hgraph_persistence
    EXPORT hgraphPersistenceTargets
    COMPONENT Development
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT Development FILES_MATCHING PATTERN "*.h")

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph-persistenceConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-persistence
)

install(EXPORT hgraphPersistenceTargets
    FILE hgraphPersistenceTargets.cmake
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-persistence
    COMPONENT Development
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfigVersion.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph_persistence_arrow.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-persistence
    COMPONENT Development
)
