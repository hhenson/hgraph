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

# The resolver (src/semantics): scopes, the interim kernel table, phase
# classification. hgraph-free; the driver supplies the operator lookup.
add_library(hgl_semantics STATIC
    src/semantics/resolve.cpp
)
add_library(hgl::semantics ALIAS hgl_semantics)
target_compile_features(hgl_semantics PUBLIC cxx_std_23)
target_include_directories(hgl_semantics PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_semantics PUBLIC hgl::syntax)
hgl_apply_warnings(hgl_semantics)

# The direct-wiring backend (src/wiring): walks the resolved tree straight
# into an hgraph Wiring for test, run and the REPL (developer guide,
# "Direct-wiring backend", "First pass").
add_library(hgl_wiring STATIC
    src/wiring/backend.cpp
)
add_library(hgl::wiring ALIAS hgl_wiring)
target_compile_features(hgl_wiring PUBLIC cxx_std_23)
target_include_directories(hgl_wiring PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_wiring PUBLIC hgl::semantics hgraph::core)
if(TARGET hgraph::analytics)
    target_link_libraries(hgl_wiring PUBLIC hgraph::analytics)
    target_compile_definitions(hgl_wiring PRIVATE HGL_HAVE_ANALYTICS=1)
endif()
hgl_apply_warnings(hgl_wiring)

# Executables that link the backend against an installed, Python-enabled SDK
# need the interpreter library resolved (developer guide of hgraph,
# build_system: `hgraph_link_python_embedding`). The repository build links
# the static core and needs nothing.
function(hgl_link_runtime_executable target)
    if(COMMAND hgraph_link_python_embedding)
        hgraph_link_python_embedding(${target})
    endif()
endfunction()

# The command driver (src/driver): check, test, run, repl and the dumps.
add_library(hgl_driver STATIC
    src/driver/driver.cpp
)
add_library(hgl::driver ALIAS hgl_driver)
target_compile_features(hgl_driver PUBLIC cxx_std_23)
target_include_directories(hgl_driver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_driver PUBLIC hgl::wiring hgraph::core)
hgl_apply_warnings(hgl_driver)

add_executable(hgl src/main.cpp)
target_compile_features(hgl PRIVATE cxx_std_23)
target_compile_definitions(hgl PRIVATE
    HGRAPH_LANGUAGE_VERSION="${PROJECT_VERSION}"
)
target_link_libraries(hgl PRIVATE hgl::driver hgraph::core)
hgl_link_runtime_executable(hgl)
hgl_apply_warnings(hgl)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(TARGETS hgl
    COMPONENT Runtime
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
