cmake_minimum_required(VERSION 3.25)

project(hgraph_language VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)

set(_hgraph_language_warnings_as_errors OFF)
if(DEFINED HGRAPH_WARNINGS_AS_ERRORS)
    set(_hgraph_language_warnings_as_errors "${HGRAPH_WARNINGS_AS_ERRORS}")
endif()
option(HGRAPH_LANGUAGE_WARNINGS_AS_ERRORS
    "Treat hgraph-language compiler warnings as errors"
    "${_hgraph_language_warnings_as_errors}")
unset(_hgraph_language_warnings_as_errors)

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()

add_executable(hgl src/main.cpp)
target_compile_features(hgl PRIVATE cxx_std_23)
target_compile_definitions(hgl PRIVATE
    HGRAPH_LANGUAGE_VERSION="${PROJECT_VERSION}"
)
target_link_libraries(hgl PRIVATE hgraph::core)

if(MSVC)
    target_compile_options(hgl PRIVATE /W4 /permissive-)
    if(HGRAPH_LANGUAGE_WARNINGS_AS_ERRORS)
        target_compile_options(hgl PRIVATE /WX)
    endif()
else()
    target_compile_options(hgl PRIVATE -Wall -Wextra -Wpedantic)
    if(HGRAPH_LANGUAGE_WARNINGS_AS_ERRORS)
        target_compile_options(hgl PRIVATE -Werror)
    endif()
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(TARGETS hgl
    COMPONENT Runtime
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
