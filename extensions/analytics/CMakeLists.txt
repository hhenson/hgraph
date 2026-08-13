cmake_minimum_required(VERSION 3.25)

project(hgraph_analytics VERSION 0.8.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)
include(FetchContent)

option(HGRAPH_ANALYTICS_BUILD_PYTHON "Build the optional Python authoring bridge" OFF)
option(HGRAPH_ANALYTICS_WARNINGS_AS_ERRORS
    "Treat hgraph-analytics compiler warnings as errors"
    ${HGRAPH_WARNINGS_AS_ERRORS})

if(HGRAPH_ANALYTICS_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS
        Interpreter Development.Module Development.SABIModule REQUIRED)
endif()

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()

include(cmake/hgraph_analytics_arrow.cmake)
if(HGRAPH_ANALYTICS_BUILD_PYTHON)
    hgraph_analytics_find_arrow(PREFER_PYARROW)
else()
    hgraph_analytics_find_arrow()
endif()

# Boost.Math supplies the header-only correlation kernel owned by this package.
find_package(boost_math 1.90 CONFIG QUIET)
if(NOT TARGET Boost::math)
    FetchContent_Declare(
        boost_math
        GIT_REPOSITORY https://github.com/boostorg/math.git
        GIT_TAG        boost-1.91.0
        GIT_SHALLOW    TRUE
        SYSTEM
    )
    set(BOOST_MATH_STANDALONE ON CACHE BOOL
        "Use Boost.Math without the Boost superproject" FORCE)
    set(_hgraph_analytics_build_testing "${BUILD_TESTING}")
    set(BUILD_TESTING OFF)
    FetchContent_MakeAvailable(boost_math)
    set(BUILD_TESTING "${_hgraph_analytics_build_testing}")
    unset(_hgraph_analytics_build_testing)
endif()

if(HGRAPH_ANALYTICS_BUILD_PYTHON)
    get_target_property(_hgraph_analytics_core_links hgraph::options INTERFACE_LINK_LIBRARIES)
    if("Python::Python" IN_LIST _hgraph_analytics_core_links)
        message(FATAL_ERROR
            "The selected hgraph SDK embeds a specific Python interpreter and "
            "cannot produce an ABI3 hgraph-analytics wheel; use the SDK installed "
            "by an hgraph stable-ABI wheel")
    endif()
endif()

add_library(hgraph_analytics
    src/array_operators.cpp
    src/operators.cpp
    src/registration.cpp
    src/shaped_array_operators.cpp
    src/statistics.cpp
)
add_library(hgraph::analytics ALIAS hgraph_analytics)

get_target_property(HGRAPH_ANALYTICS_LIBRARY_TYPE hgraph_analytics TYPE)
if(HGRAPH_ANALYTICS_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
    target_compile_definitions(hgraph_analytics PUBLIC HGRAPH_ANALYTICS_STATIC_DEFINE)
endif()

target_compile_features(hgraph_analytics PUBLIC cxx_std_23)
target_link_libraries(hgraph_analytics PUBLIC hgraph::core)
target_link_libraries(hgraph_analytics PRIVATE $<BUILD_INTERFACE:Boost::math>)
if(TARGET ArrowCompute::arrow_compute_shared)
    target_link_libraries(hgraph_analytics PRIVATE
        Arrow::arrow_shared ArrowCompute::arrow_compute_shared)
else()
    target_link_libraries(hgraph_analytics PRIVATE
        Arrow::arrow_static ArrowCompute::arrow_compute_static)
endif()
target_include_directories(hgraph_analytics
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
set_target_properties(hgraph_analytics PROPERTIES
    EXPORT_NAME analytics
    POSITION_INDEPENDENT_CODE ON
)

if(MSVC)
    target_compile_options(hgraph_analytics PRIVATE /W4 /permissive-)
    if(HGRAPH_ANALYTICS_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_analytics PRIVATE /WX)
    endif()
else()
    target_compile_options(hgraph_analytics PRIVATE -Wall -Wextra -Wpedantic)
    if(HGRAPH_ANALYTICS_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_analytics PRIVATE -Werror)
    endif()
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(HGRAPH_ANALYTICS_BUILD_PYTHON)
    if(NOT COMMAND hgraph_add_python_module OR NOT TARGET hgraph::nanobind)
        message(FATAL_ERROR
            "HGRAPH_ANALYTICS_BUILD_PYTHON requires a Python-enabled installed hgraph SDK")
    endif()
    hgraph_add_python_module(_hgraph_analytics STABLE_ABI NOMINSIZE src/python_module.cpp)
    target_link_libraries(_hgraph_analytics PRIVATE hgraph::analytics)
    install(TARGETS _hgraph_analytics
        COMPONENT Python
        LIBRARY DESTINATION hgraph_analytics
        RUNTIME DESTINATION hgraph_analytics
    )
endif()

install(TARGETS hgraph_analytics
    EXPORT hgraphAnalyticsTargets
    COMPONENT Development
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT Development FILES_MATCHING PATTERN "*.h")

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraph-analyticsConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph-analyticsConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/hgraph-analyticsConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-analytics
)

install(EXPORT hgraphAnalyticsTargets
    FILE hgraphAnalyticsTargets.cmake
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-analytics
    COMPONENT Development
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraph-analyticsConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraph-analyticsConfigVersion.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph_analytics_arrow.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-analytics
    COMPONENT Development
)
