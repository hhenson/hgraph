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

# Shared warning settings for every language target.
function(hgl_apply_warnings target)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
        if(HGRAPH_LANGUAGE_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE /WX)
        endif()
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
        if(HGRAPH_LANGUAGE_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    endif()
endfunction()

# The frontend: source manager, diagnostics, lexer, parser, AST, printer
# (developer guide, "Frontend components": src/syntax).
add_library(hgl_syntax STATIC
    src/syntax/source.cpp
    src/syntax/diagnostic.cpp
    src/syntax/temporal.cpp
    src/syntax/token.cpp
    src/syntax/lexer.cpp
    src/syntax/ast.cpp
    src/syntax/parser.cpp
    src/syntax/ast_printer.cpp
)
add_library(hgl::syntax ALIAS hgl_syntax)
target_compile_features(hgl_syntax PUBLIC cxx_std_23)
target_include_directories(hgl_syntax PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
hgl_apply_warnings(hgl_syntax)

# The command driver (src/driver): check and the dumps.
add_library(hgl_driver STATIC
    src/driver/driver.cpp
)
add_library(hgl::driver ALIAS hgl_driver)
target_compile_features(hgl_driver PUBLIC cxx_std_23)
target_include_directories(hgl_driver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_driver PUBLIC hgl::syntax hgraph::core)
hgl_apply_warnings(hgl_driver)

add_executable(hgl src/main.cpp)
target_compile_features(hgl PRIVATE cxx_std_23)
target_compile_definitions(hgl PRIVATE
    HGRAPH_LANGUAGE_VERSION="${PROJECT_VERSION}"
)
target_link_libraries(hgl PRIVATE hgl::driver hgraph::core)
hgl_apply_warnings(hgl)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(TARGETS hgl
    COMPONENT Runtime
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
