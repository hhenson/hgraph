cmake_minimum_required(VERSION 3.25)

# hgl rides the hgraph release train: it reports hgraph::release_version_string
# (RFC 0032) and carries no version of its own.
project(hgraph_language LANGUAGES C CXX)

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

option(HGL_ENABLE_LINE_EDITING
    "Give the REPL line editing, history and completion (isocline, fetched at configure time)"
    ON)

# The declarative grammar selected in ADR 0001. It is private to hgl_syntax:
# parser-template types must not leak into the compiler's public pass contracts.
include(FetchContent)
set(LEXY_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    lexy
    GIT_REPOSITORY https://github.com/foonathan/lexy.git
    GIT_TAG        v2025.05.0
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(lexy)

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()

# The line editor behind `hgl repl` (developer guide, "Scripted, REPL, and
# AOT drivers"): isocline, MIT, one C translation unit. Fetched and compiled
# here rather than added as a subproject so its own CMake (tests, install)
# stays out of the build; without it the REPL reads plain lines.
if(HGL_ENABLE_LINE_EDITING)
    include(FetchContent)
    FetchContent_Declare(
        isocline
        GIT_REPOSITORY https://github.com/daanx/isocline.git
        GIT_TAG        v1.1.0
        GIT_SHALLOW    TRUE
        SOURCE_SUBDIR  hgl-no-cmake-project
    )
    FetchContent_MakeAvailable(isocline)
    add_library(hgl_isocline STATIC ${isocline_SOURCE_DIR}/src/isocline.c)
    target_include_directories(hgl_isocline SYSTEM PUBLIC ${isocline_SOURCE_DIR}/include)
    set_target_properties(hgl_isocline PROPERTIES C_STANDARD 99 POSITION_INDEPENDENT_CODE ON)
    if(NOT MSVC)
        target_compile_options(hgl_isocline PRIVATE -w)
    endif()
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
    src/syntax/syntax_tree.cpp
    src/syntax/ast.cpp
    src/syntax/ast_projection.cpp
    src/syntax/syntax_diagnostics.cpp
    src/syntax/token_grammar.cpp
    src/syntax/parser.cpp
    src/syntax/ast_printer.cpp
)
# GCC diagnoses the overloaded lexy DSL operators as though they were built-in
# arithmetic (`-Wparentheses`) and, after deep inlining, reports the library's
# scoped parse-handler link as escaping (`-Wdangling-pointer`). Both are false
# positives in template code instantiated by this one implementation TU. Keep
# the narrow suppression here rather than weakening warnings for hgl_syntax or
# exposing parser-library headers to its clients.
set_property(SOURCE src/syntax/token_grammar.cpp APPEND PROPERTY COMPILE_OPTIONS
    "$<$<CXX_COMPILER_ID:GNU>:-Wno-parentheses>"
    "$<$<CXX_COMPILER_ID:GNU>:-Wno-dangling-pointer>"
)
add_library(hgl::syntax ALIAS hgl_syntax)
target_compile_features(hgl_syntax PUBLIC cxx_std_23)
target_include_directories(hgl_syntax PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_syntax PRIVATE foonathan::lexy::core)
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

# The backend-independent HGL representation (src/ir): stable declaration and
# symbol identities first, followed by type completion and hgraph-IR lowering
# in later passes. Backends migrate to this boundary without depending on AST
# node identities.
add_library(hgl_ir STATIC
    src/ir/canonical_types.cpp
    src/ir/constraint_solver.cpp
    src/ir/generic_substitution.cpp
    src/ir/hir.cpp
    src/ir/hir_printer.cpp
    src/ir/lower.cpp
    src/ir/type_check.cpp
)
add_library(hgl::ir ALIAS hgl_ir)
target_compile_features(hgl_ir PUBLIC cxx_std_23)
target_include_directories(hgl_ir PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_ir PUBLIC hgl::semantics)
hgl_apply_warnings(hgl_ir)

# The execution-facing semantic IR (src/hgraph_ir). It owns canonical
# contracts and structured callable/test bodies; provider planning advances
# those bodies to an executable plan before either backend migrates here.
add_library(hgl_graph_ir STATIC
    src/hgraph_ir/lower.cpp
    src/hgraph_ir/printer.cpp
)
add_library(hgl::graph_ir ALIAS hgl_graph_ir)
target_compile_features(hgl_graph_ir PUBLIC cxx_std_23)
target_include_directories(hgl_graph_ir PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_graph_ir PUBLIC hgl::ir)
hgl_apply_warnings(hgl_graph_ir)

# Canonical hgraph metadata materialization for execution backends. Keeping
# this bridge separate prevents hgraph runtime types from entering hgraph IR.
add_library(hgl_wiring_types STATIC
    src/wiring/type_bridge.cpp
)
add_library(hgl::wiring_types ALIAS hgl_wiring_types)
target_compile_features(hgl_wiring_types PUBLIC cxx_std_23)
target_include_directories(hgl_wiring_types PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_wiring_types PUBLIC hgl::graph_ir hgraph::core)
hgl_apply_warnings(hgl_wiring_types)

# The direct-wiring backend (src/wiring): walks the resolved tree straight
# into an hgraph Wiring for test, run and the REPL (developer guide,
# "Direct-wiring backend", "First pass").
add_library(hgl_wiring STATIC
    src/wiring/backend.cpp
    src/wiring/operator_types.cpp
)
add_library(hgl::wiring ALIAS hgl_wiring)
target_compile_features(hgl_wiring PUBLIC cxx_std_23)
target_include_directories(hgl_wiring PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_wiring PUBLIC hgl::ir hgl::wiring_types hgraph::core)
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

# The C++ backend (src/codegen): emits a header/source pair of public hgraph
# authoring code per module (developer guide, "C++ backend, first pass").
# Hgraph IR owns module and callable/operator interfaces, struct layouts, and
# construction defaults while a temporary syntax adapter prints bodies and
# local annotations.
add_library(hgl_codegen STATIC
    src/codegen/cpp_emitter.cpp
)
add_library(hgl::codegen ALIAS hgl_codegen)
target_compile_features(hgl_codegen PUBLIC cxx_std_23)
target_include_directories(hgl_codegen PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_codegen PUBLIC hgl::graph_ir)
hgl_apply_warnings(hgl_codegen)

# The command driver (src/driver): check, test, run, repl, emit-cpp and the dumps.
add_library(hgl_driver STATIC
    src/driver/cpp_formatter.cpp
    src/driver/driver.cpp
    src/driver/line_reader.cpp
    src/driver/native_module.cpp
    src/driver/process.cpp
)
add_library(hgl::driver ALIAS hgl_driver)
target_compile_features(hgl_driver PUBLIC cxx_std_23)
target_include_directories(hgl_driver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(hgl_driver PUBLIC hgl::graph_ir hgl::wiring hgl::codegen hgraph::core)
if(UNIX)
    target_link_libraries(hgl_driver PRIVATE ${CMAKE_DL_LIBS})
endif()
if(HGL_ENABLE_LINE_EDITING)
    target_link_libraries(hgl_driver PRIVATE hgl_isocline)
    target_compile_definitions(hgl_driver PRIVATE HGL_HAVE_ISOCLINE=1)
endif()
hgl_apply_warnings(hgl_driver)

add_executable(hgl src/main.cpp)
target_compile_features(hgl PRIVATE cxx_std_23)
target_link_libraries(hgl PRIVATE hgl::driver hgraph::core)
# Scripted modules are loadable images which resolve the hgraph symbols from
# this process. Export those symbols so every module shares this process's
# operator registry rather than linking a second static runtime.
set_target_properties(hgl PROPERTIES ENABLE_EXPORTS ON)

# Capture the compiler context needed by the scripted module builder. The
# executable-prefix include path is added at runtime for an installed SDK;
# these evaluated paths also support the in-tree compiler and external/system
# dependencies selected by this build.
set(_hgl_native_config_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/$<CONFIG>")
find_program(HGL_CLANG_FORMAT_EXECUTABLE
    NAMES clang-format clang-format-23 clang-format-22 clang-format-21 clang-format-20 clang-format-19 clang-format-18
    REQUIRED)
file(RELATIVE_PATH _hgl_install_include_from_bindir
    "${CMAKE_INSTALL_FULL_BINDIR}/"
    "${CMAKE_INSTALL_FULL_INCLUDEDIR}/")
set(_hgl_native_definitions "$<JOIN:$<TARGET_PROPERTY:hgl,COMPILE_DEFINITIONS>,;>")
set(_hgl_native_include_directories "$<JOIN:$<TARGET_PROPERTY:hgl,INCLUDE_DIRECTORIES>,;>")
set(_hgl_native_compile_options "$<JOIN:$<TARGET_PROPERTY:hgl,COMPILE_OPTIONS>,;>")
set(_hgl_native_link_options "$<JOIN:$<TARGET_PROPERTY:hgl,LINK_OPTIONS>,;>")
if(WIN32)
    # Scripted native modules are currently Unix-only. Avoid evaluating the
    # executable's language-dependent compile and link properties on Windows:
    # CMake otherwise attempts to generate different content at the same path.
    set(_hgl_native_definitions "")
    set(_hgl_native_include_directories "")
    set(_hgl_native_compile_options "")
    set(_hgl_native_link_options "")
endif()
set(_hgl_native_config_content
    "#pragma once\n\n#include <string_view>\n\nnamespace hgl::driver::native_config\n{\n    inline constexpr std::string_view compiler = R\"hgl(${CMAKE_CXX_COMPILER})hgl\";\n    inline constexpr std::string_view clang_format = R\"hgl(${HGL_CLANG_FORMAT_EXECUTABLE})hgl\";\n    inline constexpr std::string_view install_include_from_bindir = R\"hgl(${_hgl_install_include_from_bindir})hgl\";\n    inline constexpr std::string_view definitions = R\"hgl(${_hgl_native_definitions})hgl\";\n    inline constexpr std::string_view include_directories = R\"hgl(${_hgl_native_include_directories})hgl\";\n    inline constexpr std::string_view compile_options = R\"hgl(${_hgl_native_compile_options})hgl\";\n    inline constexpr std::string_view link_options = R\"hgl(${_hgl_native_link_options})hgl\";\n    inline constexpr std::string_view system_name = R\"hgl(${CMAKE_SYSTEM_NAME})hgl\";\n    inline constexpr std::string_view system_processor = R\"hgl(${CMAKE_SYSTEM_PROCESSOR})hgl\";\n    inline constexpr std::string_view configuration = R\"hgl($<CONFIG>)hgl\";\n}\n")
if(WIN32)
    file(GENERATE
        OUTPUT "${_hgl_native_config_dir}/hgl_native_compile_config.h"
        CONTENT "${_hgl_native_config_content}")
else()
    file(GENERATE
        OUTPUT "${_hgl_native_config_dir}/hgl_native_compile_config.h"
        CONTENT "${_hgl_native_config_content}"
        TARGET hgl)
endif()
unset(_hgl_native_config_content)
unset(_hgl_native_definitions)
unset(_hgl_native_include_directories)
unset(_hgl_native_compile_options)
unset(_hgl_native_link_options)
target_include_directories(hgl_driver PRIVATE "${_hgl_native_config_dir}")
# `hgl` is a build-time tool for installed consumers. Preserve runtime search
# paths contributed by external shared dependencies when CMake installs the
# executable; otherwise HglLanguage.cmake can find the compiler but a
# pyarrow-backed build cannot launch it. The `../lib` entry is the standard
# prefix layout (`bin/hgl` beside `lib/libhgraph_*` when HGRAPH_BUILD_SHARED
# is on, RFC 0032); the `../pyarrow` entry supports an SDK installed beside
# the pyarrow package; the packager's CMAKE_INSTALL_RPATH, if any, comes
# first; and the concrete build path supports ordinary prefix installs on
# the machine that configured them.
if(UNIX)
    if(APPLE)
        set(_hgl_install_rpath "@loader_path/../${CMAKE_INSTALL_LIBDIR}" "@loader_path/../pyarrow")
    else()
        set(_hgl_install_rpath "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}" "$ORIGIN/../pyarrow")
    endif()
    if(CMAKE_INSTALL_RPATH)
        list(PREPEND _hgl_install_rpath ${CMAKE_INSTALL_RPATH})
        list(REMOVE_DUPLICATES _hgl_install_rpath)
    endif()
    if(HGRAPH_PYARROW_LIBRARY_DIR)
        list(APPEND _hgl_install_rpath "${HGRAPH_PYARROW_LIBRARY_DIR}")
    elseif(TARGET hgraph::pyarrow_arrow)
        get_target_property(_hgl_pyarrow_location hgraph::pyarrow_arrow IMPORTED_LOCATION)
        if(_hgl_pyarrow_location AND NOT _hgl_pyarrow_location MATCHES "-NOTFOUND$")
            get_filename_component(_hgl_pyarrow_dir "${_hgl_pyarrow_location}" DIRECTORY)
            list(APPEND _hgl_install_rpath "${_hgl_pyarrow_dir}")
        endif()
    endif()
    set_target_properties(hgl PROPERTIES
        INSTALL_RPATH "${_hgl_install_rpath}"
        INSTALL_RPATH_USE_LINK_PATH ON)
endif()
hgl_link_runtime_executable(hgl)
hgl_apply_warnings(hgl)

# `hgl_add_module()` for consumers building packages from .hgl sources
# (user guide, "Building a package"). Included here so the repository's
# own codegen tests use the same function an installed SDK ships.
include(cmake/HglLanguage.cmake)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(TARGETS hgl
    COMPONENT Runtime
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES
    cmake/HglLanguage.cmake
    cmake/hgl_python_module.cpp.in
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgl
    COMPONENT Development
)
