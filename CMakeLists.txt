cmake_minimum_required(VERSION 3.25)

# Modern Apple and MSVC linkers rescan static archives without repeated inputs.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()

project(
    hgraph
    VERSION 0.8.0
    DESCRIPTION "C++ first implementation of the hgraph runtime"
    HOMEPAGE_URL "https://github.com/hhenson/hgraph"
    LANGUAGES CXX
)

include(CMakePackageConfigHelpers)
include(CheckCXXSourceCompiles)
include(CheckCXXSourceRuns)
include(CTest)
include(FetchContent)
include(GNUInstallDirs)

set(HGRAPH_GENERATED_INCLUDE_DIR "${PROJECT_BINARY_DIR}/generated/include")
set(HGRAPH_VERSION_HEADER_IN "${PROJECT_SOURCE_DIR}/include/hgraph/version.h.in")
set(HGRAPH_VERSION_HEADER "${HGRAPH_GENERATED_INCLUDE_DIR}/hgraph/version.h")

set(HGRAPH_GIT_BRANCH "unknown")
set(HGRAPH_GIT_COMMIT_HASH "unknown")
set(HGRAPH_GIT_COMMIT_DATE "unknown")

find_package(Git QUIET)
if(Git_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    function(hgraph_read_git_metadata output_variable)
        execute_process(
            COMMAND "${GIT_EXECUTABLE}" ${ARGN}
            WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
            OUTPUT_VARIABLE git_output
            OUTPUT_STRIP_TRAILING_WHITESPACE
            RESULT_VARIABLE git_result
            ERROR_QUIET
        )
        if(git_result EQUAL 0 AND NOT git_output STREQUAL "")
            set("${output_variable}" "${git_output}" PARENT_SCOPE)
        endif()
    endfunction()

    hgraph_read_git_metadata(HGRAPH_GIT_BRANCH rev-parse --abbrev-ref HEAD)
    hgraph_read_git_metadata(HGRAPH_GIT_COMMIT_HASH log -1 --format=%H)
    hgraph_read_git_metadata(HGRAPH_GIT_COMMIT_DATE log -1 --format=%cD)
endif()

message(STATUS "hgraph git branch: ${HGRAPH_GIT_BRANCH}")
message(STATUS "hgraph git commit: ${HGRAPH_GIT_COMMIT_HASH}")
message(STATUS "hgraph git commit date: ${HGRAPH_GIT_COMMIT_DATE}")

file(MAKE_DIRECTORY "${HGRAPH_GENERATED_INCLUDE_DIR}/hgraph")
configure_file("${HGRAPH_VERSION_HEADER_IN}" "${HGRAPH_VERSION_HEADER}" @ONLY)

option(HGRAPH_BUILD_SHARED "Build hgraph_core as a shared library" OFF)
option(HGRAPH_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
option(HGRAPH_FETCH_SIMDJSON "Fetch the supported simdjson release instead of using a system package" OFF)
option(HGRAPH_FETCH_DATE "Fetch the supported date/tz release when no package is available" OFF)
option(HGRAPH_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(HGRAPH_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(HGRAPH_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(HGRAPH_BUILD_PYTHON_BINDINGS "Build the Python extension bridge" OFF)
option(HGRAPH_ENABLE_PYTHON_USER_NODES "Enable C++ runtime support for Python user nodes" OFF)
option(HGRAPH_PYTHON_STABLE_ABI "Build the Python bridge against the CPython 3.12 stable ABI" ON)
option(HGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS "Use local .venv Python/nanobind headers for IDE code models when Python support is disabled" ON)
option(HGRAPH_ENABLE_COMPILER_CACHE "Use sccache or ccache when available" ON)
option(HGRAPH_ENABLE_PCH "Use private precompiled headers for repository targets" ON)
option(HGRAPH_ENABLE_DEBUGGER_SMOKE_TESTS "Run debugger subprocess smoke tests when the platform debugger is available" OFF)
option(HGRAPH_BUILD_KAFKA_EXTENSION "Build the first-party Kafka extension" OFF)
option(HGRAPH_BUILD_ANALYTICS_EXTENSION "Build the first-party analytics extension" OFF)
option(HGRAPH_BUILD_WEB_EXTENSION "Build the first-party web extension" OFF)
option(HGRAPH_BUILD_PERSISTENCE_EXTENSION "Build the first-party persistence extension" OFF)
option(HGRAPH_BUILD_FABRIC_EXTENSION "Build the first-party versioned dataflow fabric extension" OFF)

if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_BUILD_SHARED)
    # Every static library linked into the extension must provide PIC on ELF
    # platforms, including dependencies created later through FetchContent.
    # Shared native builds have the same requirement.
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

set(_hgraph_default_use_pyarrow_arrow OFF)
if(HGRAPH_BUILD_PYTHON_BINDINGS)
    set(_hgraph_default_use_pyarrow_arrow ON)
endif()
option(HGRAPH_USE_PYARROW_ARROW "Use pyarrow's bundled Arrow libraries for this build" "${_hgraph_default_use_pyarrow_arrow}")

if(HGRAPH_ENABLE_COMPILER_CACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
    find_program(HGRAPH_COMPILER_CACHE_PROGRAM sccache)
    if(NOT HGRAPH_COMPILER_CACHE_PROGRAM)
        find_program(HGRAPH_COMPILER_CACHE_PROGRAM ccache)
    endif()
    if(HGRAPH_COMPILER_CACHE_PROGRAM)
        set(CMAKE_CXX_COMPILER_LAUNCHER "${HGRAPH_COMPILER_CACHE_PROGRAM}" CACHE STRING "C++ compiler launcher" FORCE)
        message(STATUS "hgraph compiler cache: ${HGRAPH_COMPILER_CACHE_PROGRAM}")
    endif()
endif()

if(MSVC AND HGRAPH_ENABLE_PCH AND CMAKE_CXX_COMPILER_LAUNCHER MATCHES "sccache")
    # sccache cannot cache MSVC /Yc-/Yu compilations; with a warm shared
    # cache, hits beat the PCH speedup, so the cache wins.
    message(STATUS "hgraph: disabling PCH (sccache cannot cache MSVC PCH compilations)")
    set(HGRAPH_ENABLE_PCH OFF)
endif()

function(hgraph_enable_private_pch target_name)
    if(HGRAPH_ENABLE_PCH)
        target_precompile_headers(${target_name} PRIVATE
            "$<$<COMPILE_LANGUAGE:CXX>:${PROJECT_SOURCE_DIR}/src/hgraph/pch.h>"
        )
    endif()
endfunction()

if(HGRAPH_ENABLE_TSAN AND (HGRAPH_ENABLE_ASAN OR HGRAPH_ENABLE_UBSAN))
    message(FATAL_ERROR "HGRAPH_ENABLE_TSAN is mutually exclusive with ASAN and UBSAN")
endif()

add_library(hgraph_options INTERFACE)
add_library(hgraph::options ALIAS hgraph_options)
set_target_properties(hgraph_options PROPERTIES EXPORT_NAME options)

# Third-party implementation dependencies are private when the runtime is
# shared. Downstream node extensions then link only the hgraph DSOs and cannot
# accidentally embed a second copy of process-wide registries or dependency
# state. Static builds retain the existing transitive dependency contract.
add_library(hgraph_private_dependencies INTERFACE)

if(NOT HGRAPH_BUILD_SHARED)
    target_compile_definitions(hgraph_options INTERFACE HGRAPH_STATIC_DEFINE)
endif()

target_compile_features(hgraph_options INTERFACE cxx_std_23)
target_include_directories(hgraph_options INTERFACE
    $<BUILD_INTERFACE:${HGRAPH_GENERATED_INCLUDE_DIR}>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/third_party>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/third_party>
)

if(MSVC)
    # Public hgraph headers expose fmt.  MSVC must decode those headers as
    # UTF-8, so this is part of the installed SDK compile contract rather
    # than a private option on the core targets.
    target_compile_options(hgraph_options INTERFACE /W4 /permissive- /utf-8)
    if(HGRAPH_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_options INTERFACE /WX)
    endif()
else()
    target_compile_options(hgraph_options INTERFACE -Wall -Wextra -Wpedantic)
    if(HGRAPH_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_options INTERFACE -Werror)
    endif()
endif()

set(_hgraph_sanitizers)
if(HGRAPH_ENABLE_ASAN)
    list(APPEND _hgraph_sanitizers address)
endif()
if(HGRAPH_ENABLE_UBSAN)
    list(APPEND _hgraph_sanitizers undefined)
endif()
if(HGRAPH_ENABLE_TSAN)
    list(APPEND _hgraph_sanitizers thread)
endif()

if(_hgraph_sanitizers)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        message(FATAL_ERROR "Sanitizers are only configured for Clang and GCC")
    endif()
    list(JOIN _hgraph_sanitizers "," _hgraph_sanitizer_flags)
    target_compile_options(hgraph_options INTERFACE
        -fsanitize=${_hgraph_sanitizer_flags}
        -fno-omit-frame-pointer
    )
    target_link_options(hgraph_options INTERFACE -fsanitize=${_hgraph_sanitizer_flags})
endif()

find_package(Threads REQUIRED)
target_link_libraries(hgraph_options INTERFACE Threads::Threads)

set(HGRAPH_TIME_ZONE_BACKEND "auto" CACHE STRING
    "Named-zone backend: auto, std, or date")
set_property(CACHE HGRAPH_TIME_ZONE_BACKEND PROPERTY STRINGS auto std date)
if(NOT HGRAPH_TIME_ZONE_BACKEND MATCHES "^(auto|std|date)$")
    message(FATAL_ERROR
        "HGRAPH_TIME_ZONE_BACKEND must be one of: auto, std, date")
endif()

set(_hgraph_saved_required_flags "${CMAKE_REQUIRED_FLAGS}")
if(MSVC)
    string(APPEND CMAKE_REQUIRED_FLAGS " /std:c++latest")
else()
    string(APPEND CMAKE_REQUIRED_FLAGS " -std=c++23")
endif()
check_cxx_source_compiles([=[
    #include <chrono>
    #include <sstream>
    int main() {
        const auto &database = std::chrono::get_tzdb();
        const auto *zone = database.locate_zone("UTC");
        (void)zone->get_info(std::chrono::sys_time<std::chrono::microseconds>{});
        (void)zone->get_info(std::chrono::local_time<std::chrono::microseconds>{});
        std::istringstream input{"2024-06-13T10:15:30"};
        std::chrono::sys_time<std::chrono::microseconds> parsed{};
        std::chrono::from_stream(input, "%Y-%m-%dT%H:%M:%S", parsed);
        return database.version.empty();
    }
]=] HGRAPH_STD_CHRONO_TZDB_AVAILABLE)
if(HGRAPH_STD_CHRONO_TZDB_AVAILABLE)
    check_cxx_source_runs([=[
        #include <chrono>
        int main() {
            try {
                const auto *zone =
                    std::chrono::get_tzdb().locate_zone("Pacific/Apia");
                const auto before = zone->get_info(
                    std::chrono::sys_seconds{std::chrono::seconds{1325239199}});
                const auto after = zone->get_info(
                    std::chrono::sys_seconds{std::chrono::seconds{1325239200}});
                return before.offset == std::chrono::seconds{-36000} &&
                               after.offset == std::chrono::seconds{50400}
                           ? 0
                           : 1;
            } catch (...) {
                return 1;
            }
        }
    ]=] HGRAPH_STD_CHRONO_TZDB_CONFORMING)
else()
    set(HGRAPH_STD_CHRONO_TZDB_CONFORMING OFF)
endif()
set(CMAKE_REQUIRED_FLAGS "${_hgraph_saved_required_flags}")
unset(_hgraph_saved_required_flags)

set(_hgraph_selected_time_zone_backend "${HGRAPH_TIME_ZONE_BACKEND}")
if(_hgraph_selected_time_zone_backend STREQUAL "auto")
    if(HGRAPH_STD_CHRONO_TZDB_AVAILABLE AND
       HGRAPH_STD_CHRONO_TZDB_CONFORMING)
        set(_hgraph_selected_time_zone_backend "std")
    else()
        set(_hgraph_selected_time_zone_backend "date")
    endif()
endif()
if(_hgraph_selected_time_zone_backend STREQUAL "std" AND
   (NOT HGRAPH_STD_CHRONO_TZDB_AVAILABLE OR
    NOT HGRAPH_STD_CHRONO_TZDB_CONFORMING))
    message(FATAL_ERROR
        "HGRAPH_TIME_ZONE_BACKEND=std was requested, but this standard "
        "library does not provide a conforming C++20 TZDB implementation")
endif()

set(_hgraph_date_fetched OFF)
if(_hgraph_selected_time_zone_backend STREQUAL "date")
    find_package(date 3.0.4 CONFIG QUIET)
    if(NOT TARGET date::date-tz)
        if(NOT HGRAPH_FETCH_DATE)
            message(FATAL_ERROR
                "The selected named-zone backend requires date/tz >= 3.0.4. "
                "Install its CMake package or configure with "
                "-DHGRAPH_FETCH_DATE=ON.")
        endif()
        FetchContent_Declare(
            date
            GIT_REPOSITORY https://github.com/HowardHinnant/date.git
            GIT_TAG        v3.0.4
            GIT_SHALLOW    TRUE
            SYSTEM
        )
        set(BUILD_TZ_LIB ON CACHE BOOL "Build date/tz" FORCE)
        set(USE_SYSTEM_TZ_DB ON CACHE BOOL "Use the operating-system TZDB" FORCE)
        set(ENABLE_DATE_TESTING OFF CACHE BOOL "Build date tests" FORCE)
        if(HGRAPH_BUILD_SHARED)
            set(ENABLE_DATE_INSTALL OFF CACHE BOOL
                "Install date/tz with static hgraph SDKs" FORCE)
        else()
            set(ENABLE_DATE_INSTALL ON CACHE BOOL
                "Install date/tz with static hgraph SDKs" FORCE)
        endif()
        FetchContent_MakeAvailable(date)
        set(_hgraph_date_fetched ON)
    endif()
    if(HGRAPH_BUILD_SHARED)
        target_link_libraries(hgraph_private_dependencies INTERFACE date::date-tz)
    else()
        target_link_libraries(hgraph_options INTERFACE
            $<BUILD_INTERFACE:date::date-tz>)
    endif()
    target_compile_definitions(hgraph_options INTERFACE
        HGRAPH_TIME_ZONE_BACKEND_DATE=1)
else()
    target_compile_definitions(hgraph_options INTERFACE
        HGRAPH_TIME_ZONE_BACKEND_STD=1)
endif()
message(STATUS
    "hgraph named-zone backend: ${_hgraph_selected_time_zone_backend}")

if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_FETCH_SIMDJSON)
    # Wheels must not retain a dependency on a package-manager shared library;
    # hosted native builds can opt into the same pinned dependency explicitly.
    FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG        v4.6.4
        GIT_SHALLOW    TRUE
        SYSTEM
    )
    set(SIMDJSON_INSTALL OFF CACHE BOOL "Generate simdjson install targets" FORCE)
    set(SIMDJSON_DEVELOPER_MODE OFF CACHE BOOL "Build simdjson developer targets" FORCE)
    FetchContent_MakeAvailable(simdjson)
    # simdjson's PCH includes <bit> when the header exists. Build the fetched
    # library in a mode where that header's contents are available on MSVC.
    target_compile_features(simdjson PRIVATE cxx_std_20)
    if(HGRAPH_BUILD_SHARED AND HGRAPH_BUILD_PYTHON_BINDINGS)
        target_link_libraries(hgraph_private_dependencies INTERFACE simdjson::simdjson)
    else()
        target_link_libraries(hgraph_options INTERFACE
            $<BUILD_INTERFACE:simdjson::simdjson>
        )
    endif()
else()
    # json_impl.cpp uses simdjson::dom::element_type::BIGINT, introduced in
    # simdjson 4.5 (the FetchContent path above pins v4.6.4). Distro packages
    # can be older (Ubuntu 24.04 ships 3.x), so enforce the floor here. The
    # check is explicit because simdjson's package version file uses
    # same-minor compatibility, which would also reject newer 4.x releases.
    find_package(simdjson CONFIG REQUIRED)
    if(simdjson_VERSION VERSION_LESS 4.5)
        message(FATAL_ERROR "hgraph requires simdjson >= 4.5 for "
            "dom::element_type::BIGINT; found ${simdjson_VERSION}")
    endif()
    if(HGRAPH_BUILD_SHARED AND HGRAPH_BUILD_PYTHON_BINDINGS)
        target_link_libraries(hgraph_private_dependencies INTERFACE simdjson::simdjson)
    else()
        target_link_libraries(hgraph_options INTERFACE simdjson::simdjson)
    endif()
endif()

if(NOT HGRAPH_BUILD_PYTHON_BINDINGS)
    # spdlog 1.15 includes fmt/base.h, which is unavailable in older distro
    # releases such as Ubuntu 24.04's fmt 9 package.
    find_package(fmt 11 CONFIG QUIET)
endif()
set(_hgraph_fmt_fetched OFF)
if(NOT fmt_FOUND)
    FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG        12.2.0
        GIT_SHALLOW    TRUE
        SYSTEM
    )
    set(FMT_DOC OFF CACHE BOOL "Build fmt documentation" FORCE)
    set(FMT_INSTALL OFF CACHE BOOL "Generate fmt install target" FORCE)
    set(FMT_TEST OFF CACHE BOOL "Build fmt tests" FORCE)
    FetchContent_MakeAvailable(fmt)
    if(TARGET fmt-c)
        set_target_properties(fmt-c PROPERTIES EXCLUDE_FROM_ALL TRUE)
    endif()
    set(_hgraph_fmt_fetched ON)
endif()
if(HGRAPH_BUILD_SHARED AND HGRAPH_BUILD_PYTHON_BINDINGS)
    if(WIN32)
        # Public hgraph templates call fmt. Let each installed extension
        # instantiate those formatting paths itself instead of introducing a
        # binary dependency on fmt symbols owned by an hgraph DLL.
        target_compile_definitions(hgraph_options INTERFACE
            $<INSTALL_INTERFACE:FMT_HEADER_ONLY>
        )
    endif()
    target_link_libraries(hgraph_private_dependencies INTERFACE fmt::fmt)
    # Public authoring headers use fmt types. Keep the implementation library
    # private to the hgraph DSOs while exposing fetched headers to in-tree
    # extensions; installed wheel SDKs receive the same headers below.
    target_include_directories(hgraph_options INTERFACE
        $<BUILD_INTERFACE:${fmt_SOURCE_DIR}/include>)
elseif(_hgraph_fmt_fetched)
    target_link_libraries(hgraph_options INTERFACE
        $<BUILD_INTERFACE:fmt::fmt>
    )
else()
    target_link_libraries(hgraph_options INTERFACE fmt::fmt)
endif()

# Apache Arrow is a FORMAL dependency (design record: record_replay_table.rst,
# ruling 2026-07-04): the Frame value kind and the table serialization family
# are Arrow-native. A system/package-manager install is expected for normal
# C++ builds (`brew install apache-arrow` / `apt install libarrow-dev`);
# Arrow is too heavy to FetchContent by default. Explicit pyarrow-backed builds
# may use its bundled libraries; the resulting native targets do not link the
# Python runtime unless Python support is independently enabled.
set(HGRAPH_PYARROW_LIBRARY_DIR "" CACHE PATH "Directory containing pyarrow's bundled Arrow libraries")
set(HGRAPH_PYARROW_ABI_MAJOR "25" CACHE STRING "Required pyarrow/Arrow ABI major for pyarrow-backed builds")
if(HGRAPH_USE_PYARROW_ARROW)
    if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
        set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()
    find_package(Python 3.12 COMPONENTS Interpreter REQUIRED)
    set(_hgraph_pyarrow_probe [=[
import pathlib, pyarrow, sys
expected_major = int(sys.argv[1])
actual_major = int(pyarrow.__version__.split(".", 1)[0])
if actual_major != expected_major:
    raise SystemExit(
        f"pyarrow {pyarrow.__version__} provides Arrow ABI major {actual_major}; "
        f"this build requires major {expected_major}"
    )
root = pathlib.Path(pyarrow.__file__).resolve().parent
def pick(pattern):
    matches = sorted(root.glob(pattern))
    if not matches:
        raise SystemExit(f"missing pyarrow library matching {pattern!r} under {root}")
    return matches[0]
if sys.platform == "win32":
    arrow_link = pick("arrow.lib")
    compute_link = pick("arrow_compute.lib")
    acero_link = pick("arrow_acero.lib")
    arrow_runtime = pick("arrow.dll")
    compute_runtime = pick("arrow_compute.dll")
    acero_runtime = pick("arrow_acero.dll")
else:
    arrow_link = arrow_runtime = pick("libarrow.*")
    compute_link = compute_runtime = pick("libarrow_compute.*")
    acero_link = acero_runtime = pick("libarrow_acero.*")
for path in (root, root / "include", arrow_link, compute_link, acero_link,
             arrow_runtime, compute_runtime, acero_runtime):
    print(path.as_posix())
]=])
    execute_process(
        COMMAND "${Python_EXECUTABLE}" -c "${_hgraph_pyarrow_probe}" "${HGRAPH_PYARROW_ABI_MAJOR}"
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE _hgraph_pyarrow_info
        ERROR_VARIABLE _hgraph_pyarrow_error
        RESULT_VARIABLE _hgraph_pyarrow_result
    )
    if(NOT _hgraph_pyarrow_result EQUAL 0)
        message(FATAL_ERROR
            "HGRAPH_USE_PYARROW_ARROW=ON requires a compatible pyarrow installation: "
            "${_hgraph_pyarrow_error}")
    endif()
    string(REPLACE "\n" ";" _hgraph_pyarrow_info "${_hgraph_pyarrow_info}")
    list(GET _hgraph_pyarrow_info 0 _hgraph_pyarrow_dir)
    list(GET _hgraph_pyarrow_info 1 _hgraph_pyarrow_include)
    list(GET _hgraph_pyarrow_info 2 _hgraph_pyarrow_arrow_lib)
    list(GET _hgraph_pyarrow_info 3 _hgraph_pyarrow_compute_lib)
    list(GET _hgraph_pyarrow_info 4 _hgraph_pyarrow_acero_lib)
    list(GET _hgraph_pyarrow_info 5 HGRAPH_PYARROW_ARROW_RUNTIME)
    list(GET _hgraph_pyarrow_info 6 HGRAPH_PYARROW_COMPUTE_RUNTIME)
    list(GET _hgraph_pyarrow_info 7 HGRAPH_PYARROW_ACERO_RUNTIME)
    if(NOT EXISTS "${_hgraph_pyarrow_include}/arrow/api.h")
        message(FATAL_ERROR "pyarrow headers not found under ${_hgraph_pyarrow_include}")
    endif()
    add_library(Arrow::arrow_shared SHARED IMPORTED GLOBAL)
    add_library(ArrowCompute::arrow_compute_shared SHARED IMPORTED GLOBAL)
    add_library(ArrowAcero::arrow_acero_shared SHARED IMPORTED GLOBAL)
    if(WIN32)
        set_target_properties(Arrow::arrow_shared PROPERTIES
            IMPORTED_IMPLIB "${_hgraph_pyarrow_arrow_lib}"
            IMPORTED_LOCATION "${HGRAPH_PYARROW_ARROW_RUNTIME}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
        set_target_properties(ArrowCompute::arrow_compute_shared PROPERTIES
            IMPORTED_IMPLIB "${_hgraph_pyarrow_compute_lib}"
            IMPORTED_LOCATION "${HGRAPH_PYARROW_COMPUTE_RUNTIME}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
        set_target_properties(ArrowAcero::arrow_acero_shared PROPERTIES
            IMPORTED_IMPLIB "${_hgraph_pyarrow_acero_lib}"
            IMPORTED_LOCATION "${HGRAPH_PYARROW_ACERO_RUNTIME}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
    else()
        set_target_properties(Arrow::arrow_shared PROPERTIES
            IMPORTED_LOCATION "${_hgraph_pyarrow_arrow_lib}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
        set_target_properties(ArrowCompute::arrow_compute_shared PROPERTIES
            IMPORTED_LOCATION "${_hgraph_pyarrow_compute_lib}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
        set_target_properties(ArrowAcero::arrow_acero_shared PROPERTIES
            IMPORTED_LOCATION "${_hgraph_pyarrow_acero_lib}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
    endif()
    set(HGRAPH_PYARROW_LIBRARY_DIR "${_hgraph_pyarrow_dir}" CACHE PATH
        "Directory containing pyarrow's bundled Arrow libraries" FORCE)
    set(HGRAPH_PYARROW_RUNTIME_DIRS "${_hgraph_pyarrow_dir}")
    if(WIN32)
        get_filename_component(_hgraph_pyarrow_site_packages
            "${_hgraph_pyarrow_dir}" DIRECTORY)
        list(APPEND HGRAPH_PYARROW_RUNTIME_DIRS
            "${_hgraph_pyarrow_site_packages}")
        if(EXISTS "${_hgraph_pyarrow_site_packages}/pyarrow.libs")
            list(APPEND HGRAPH_PYARROW_RUNTIME_DIRS
                "${_hgraph_pyarrow_site_packages}/pyarrow.libs")
        endif()
    endif()
else()
    find_package(Arrow CONFIG REQUIRED)
    # Recent Arrow releases split compute/acero into their own CMake packages; some
    # packagers (Conan's Arrow recipe) define all three target namespaces
    # from the single Arrow config, in which case the separate packages do
    # not exist and must not be required.
    if(NOT TARGET ArrowCompute::arrow_compute_shared AND
       NOT TARGET ArrowCompute::arrow_compute_static)
        find_package(ArrowCompute CONFIG REQUIRED)
    endif()
    if(NOT TARGET ArrowAcero::arrow_acero_shared AND
       NOT TARGET ArrowAcero::arrow_acero_static)
        find_package(ArrowAcero CONFIG REQUIRED)
    endif()
endif()

function(hgraph_configure_test_runtime_paths test_name)
    if(NOT WIN32 OR NOT HGRAPH_PYARROW_RUNTIME_DIRS)
        return()
    endif()

    set(_hgraph_test_environment)
    foreach(_hgraph_runtime_dir IN LISTS HGRAPH_PYARROW_RUNTIME_DIRS)
        cmake_path(NATIVE_PATH _hgraph_runtime_dir NORMALIZE _hgraph_runtime_dir_native)
        list(APPEND _hgraph_test_environment
            "PATH=path_list_prepend:${_hgraph_runtime_dir_native}")
    endforeach()
    set_tests_properties("${test_name}" PROPERTIES
        ENVIRONMENT_MODIFICATION "${_hgraph_test_environment}")
endfunction()

set(_hgraph_arrow_targets
    Arrow::arrow_shared
    ArrowCompute::arrow_compute_shared
    ArrowAcero::arrow_acero_shared)
if(HGRAPH_BUILD_SHARED AND HGRAPH_BUILD_PYTHON_BINDINGS)
    target_link_libraries(hgraph_private_dependencies INTERFACE ${_hgraph_arrow_targets})
else()
    target_link_libraries(hgraph_options INTERFACE ${_hgraph_arrow_targets})
endif()

# Parquet and S3 are DURABLE STORE policy: detection, linkage and feature
# macros belong to hgraph-persistence, which owns the frame store (RFC 0025
# checkpoint 5). Core links only the Arrow value/compute/acero runtime its
# own Frame value kind needs.

# spdlog backs the LOGGER injectable (ruling 2026-07-04), built in
# SPDLOG_FMT_EXTERNAL mode against the fmt above so there is exactly one fmt
# in the process. A system spdlog is only trusted when fmt also came from the
# system (a fetched fmt paired with a system spdlog risks ODR mismatches).
if(fmt_FOUND)
    find_package(spdlog 1.15 CONFIG QUIET)
endif()
if(NOT spdlog_FOUND)
    FetchContent_Declare(
        spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG        v1.15.3
        GIT_SHALLOW    TRUE
        SYSTEM
    )
    set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "spdlog uses the project fmt" FORCE)
    set(SPDLOG_INSTALL OFF CACHE BOOL "Generate spdlog install target" FORCE)
    set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "Build spdlog examples" FORCE)
    FetchContent_MakeAvailable(spdlog)
endif()
if(HGRAPH_BUILD_SHARED AND HGRAPH_BUILD_PYTHON_BINDINGS)
    target_link_libraries(hgraph_private_dependencies INTERFACE spdlog::spdlog)
    # LoggerView is public, so in-tree extensions need the fetched spdlog
    # headers even though the compiled spdlog library remains DSO-private.
    target_include_directories(hgraph_options INTERFACE
        $<BUILD_INTERFACE:${spdlog_SOURCE_DIR}/include>)
elseif(spdlog_FOUND)
    target_link_libraries(hgraph_options INTERFACE spdlog::spdlog)
else()
    # The FetchContent'd spdlog target cannot join the install export set;
    # link it for the build tree only and let installed consumers resolve
    # spdlog via the exported find_dependency below.
    target_link_libraries(hgraph_options INTERFACE $<BUILD_INTERFACE:spdlog::spdlog>)
endif()

set(HGRAPH_CORE_TARGET_TYPE STATIC)
if(HGRAPH_BUILD_SHARED)
    set(HGRAPH_CORE_TARGET_TYPE SHARED)
endif()

if(EXISTS "${PROJECT_SOURCE_DIR}/src/CMakeLists.txt")
    add_subdirectory(src)
elseif(EXISTS "${PROJECT_SOURCE_DIR}/src/cpp/CMakeLists.txt")
    add_subdirectory(src/cpp)
elseif(EXISTS "${PROJECT_SOURCE_DIR}/src")
    file(GLOB_RECURSE HGRAPH_CORE_SOURCES CONFIGURE_DEPENDS
        "${PROJECT_SOURCE_DIR}/src/*.cc"
        "${PROJECT_SOURCE_DIR}/src/*.cpp"
        "${PROJECT_SOURCE_DIR}/src/*.cxx"
    )

    list(FILTER HGRAPH_CORE_SOURCES EXCLUDE REGEX "/python/")
    list(FILTER HGRAPH_CORE_SOURCES EXCLUDE REGEX "/bindings/python/")

    if(HGRAPH_CORE_SOURCES)
        add_library(hgraph_core ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_CORE_SOURCES})
    else()
        add_library(hgraph_core INTERFACE)
    endif()
else()
    add_library(hgraph_core INTERFACE)
endif()

if(NOT TARGET hgraph_core)
    message(FATAL_ERROR "src/cpp/CMakeLists.txt must define the hgraph_core target")
endif()

if(NOT TARGET hgraph::core)
    add_library(hgraph::core ALIAS hgraph_core)
endif()
set_target_properties(hgraph_core PROPERTIES EXPORT_NAME core)

get_target_property(HGRAPH_CORE_ACTUAL_TYPE hgraph_core TYPE)
if(HGRAPH_CORE_ACTUAL_TYPE STREQUAL "INTERFACE_LIBRARY")
    target_link_libraries(hgraph_core INTERFACE hgraph::options)
else()
    target_link_libraries(hgraph_core PUBLIC hgraph::options)
endif()

set(_hgraph_use_python_stable_abi OFF)
if(HGRAPH_BUILD_PYTHON_BINDINGS AND HGRAPH_PYTHON_STABLE_ABI)
    if(CMAKE_VERSION VERSION_LESS 3.26)
        message(FATAL_ERROR "HGRAPH_PYTHON_STABLE_ABI requires CMake 3.26 or newer")
    endif()
    set(_hgraph_use_python_stable_abi ON)
endif()

if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
        set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()

    if(_hgraph_use_python_stable_abi)
        # nanobind's package config validates Python::Module even though the
        # extension itself links only Python::SABIModule.
        set(_hgraph_python_components Interpreter Development.Module Development.SABIModule)
    else()
        set(_hgraph_python_components Interpreter Development.Module)
    endif()
    # Python user nodes execute in standalone native graphs as well as in the
    # extension.  Native executables therefore need the full embedding library
    # even when the optional extension itself uses the stable ABI.  Wheel
    # builds (SKBUILD) run inside a host interpreter and must not require or
    # link libpython — manylinux images do not ship it at all.
    if(HGRAPH_ENABLE_PYTHON_USER_NODES AND NOT SKBUILD)
        list(APPEND _hgraph_python_components Development.Embed)
    endif()

    find_package(Python 3.12 COMPONENTS ${_hgraph_python_components} REQUIRED)

    execute_process(
        COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE nanobind_ROOT
        RESULT_VARIABLE nanobind_RESULT
    )
    if(nanobind_RESULT EQUAL 0 AND nanobind_ROOT)
        set(nanobind_DIR "${nanobind_ROOT}" CACHE PATH "nanobind CMake package directory" FORCE)
        list(PREPEND CMAKE_PREFIX_PATH "${nanobind_ROOT}")
    endif()

    find_package(nanobind CONFIG REQUIRED)
    if(DEFINED NB_DIR AND EXISTS "${NB_DIR}/include")
        target_include_directories(hgraph_options INTERFACE
            $<BUILD_INTERFACE:${NB_DIR}/include>
        )
    endif()
    target_include_directories(hgraph_options INTERFACE
        $<BUILD_INTERFACE:${Python_INCLUDE_DIRS}>
    )
    if(_hgraph_use_python_stable_abi)
        target_link_libraries(hgraph_options INTERFACE Python::SABIModule)
        if(HGRAPH_BUILD_SHARED)
            set(_hgraph_nanobind_library nanobind-abi3)
        else()
            set(_hgraph_nanobind_library nanobind-static-abi3)
        endif()
    else()
        target_link_libraries(hgraph_options INTERFACE Python::Module)
        if(HGRAPH_BUILD_SHARED)
            set(_hgraph_nanobind_library nanobind)
        else()
            set(_hgraph_nanobind_library nanobind-static)
        endif()
    endif()
    if(HGRAPH_ENABLE_PYTHON_USER_NODES AND TARGET Python::Python)
        target_link_libraries(hgraph_options INTERFACE Python::Python)
    endif()
    if(COMMAND nanobind_build_library)
        nanobind_build_library(${_hgraph_nanobind_library} AS_SYSINCLUDE)
        target_link_libraries(hgraph_options INTERFACE $<BUILD_INTERFACE:${_hgraph_nanobind_library}>)
    endif()
    # Standalone user-node builds embed Python and link Python::Python. Stable
    # bridge builds resolve their limited-API symbols from the importing Python
    # process, keeping the extension independent of the interpreter minor.
    if(HGRAPH_ENABLE_PYTHON_USER_NODES)
        target_compile_definitions(hgraph_options INTERFACE HGRAPH_ENABLE_PYTHON_USER_NODES=1)
    else()
        target_compile_definitions(hgraph_options INTERFACE HGRAPH_ENABLE_PYTHON_USER_NODES=0)
    endif()
else()
    if(HGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS)
        if(WIN32)
            set(_hgraph_ide_python "${PROJECT_SOURCE_DIR}/.venv/Scripts/python.exe")
        else()
            set(_hgraph_ide_python "${PROJECT_SOURCE_DIR}/.venv/bin/python")
        endif()

        if(EXISTS "${_hgraph_ide_python}")
            execute_process(
                COMMAND "${_hgraph_ide_python}" -m nanobind --include_dir
                OUTPUT_STRIP_TRAILING_WHITESPACE
                OUTPUT_VARIABLE _hgraph_ide_nanobind_include
                RESULT_VARIABLE _hgraph_ide_nanobind_result
                ERROR_QUIET
            )
            execute_process(
                COMMAND "${_hgraph_ide_python}" -c "import sysconfig; print(sysconfig.get_path('include') or '')"
                OUTPUT_STRIP_TRAILING_WHITESPACE
                OUTPUT_VARIABLE _hgraph_ide_python_include
                RESULT_VARIABLE _hgraph_ide_python_result
                ERROR_QUIET
            )

            if(_hgraph_ide_nanobind_result EQUAL 0 AND EXISTS "${_hgraph_ide_nanobind_include}")
                target_include_directories(hgraph_options INTERFACE
                    $<BUILD_INTERFACE:${_hgraph_ide_nanobind_include}>
                )
            endif()
            if(_hgraph_ide_python_result EQUAL 0 AND EXISTS "${_hgraph_ide_python_include}")
                target_include_directories(hgraph_options INTERFACE
                    $<BUILD_INTERFACE:${_hgraph_ide_python_include}>
                )
            endif()
        endif()
    endif()
    target_compile_definitions(hgraph_options INTERFACE HGRAPH_ENABLE_PYTHON_USER_NODES=0)
endif()

if(HGRAPH_BUILD_PYTHON_BINDINGS)
    if(EXISTS "${PROJECT_SOURCE_DIR}/bindings/python/CMakeLists.txt")
        add_subdirectory(bindings/python)
    elseif(EXISTS "${PROJECT_SOURCE_DIR}/python/CMakeLists.txt")
        add_subdirectory(python)
    else()
        message(STATUS "HGRAPH_BUILD_PYTHON_BINDINGS=ON but no Python binding CMake subtree exists yet")
    endif()
endif()

if(HGRAPH_BUILD_KAFKA_EXTENSION)
    add_subdirectory(extensions/kafka)
endif()

if(HGRAPH_BUILD_ANALYTICS_EXTENSION)
    add_subdirectory(extensions/analytics)
endif()

if(HGRAPH_BUILD_WEB_EXTENSION)
    add_subdirectory(extensions/web)
endif()

if(HGRAPH_BUILD_PERSISTENCE_EXTENSION)
    add_subdirectory(extensions/persistence)
endif()

if(HGRAPH_BUILD_FABRIC_EXTENSION)
    if(NOT TARGET hgraph::persistence)
        message(FATAL_ERROR
            "HGRAPH_BUILD_FABRIC_EXTENSION requires HGRAPH_BUILD_PERSISTENCE_EXTENSION=ON")
    endif()
    add_subdirectory(extensions/fabric)
endif()

if(BUILD_TESTING AND EXISTS "${PROJECT_SOURCE_DIR}/tests/CMakeLists.txt")
    add_subdirectory(tests)
endif()

set(_hgraph_export_targets hgraph_options hgraph_runtime hgraph_wiring hgraph_stdlib hgraph_core)
set(_hgraph_runtime_destination "${CMAKE_INSTALL_BINDIR}")
if(WIN32 AND HGRAPH_BUILD_SHARED AND HGRAPH_BUILD_PYTHON_BINDINGS)
    # Python loads dependent DLLs from beside the importing extension. Both
    # _hgraph and downstream extension modules are installed at wheel root.
    set(_hgraph_runtime_destination ".")
endif()
install(TARGETS ${_hgraph_export_targets}
    EXPORT hgraphTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION "${_hgraph_runtime_destination}"
)

if(HGRAPH_BUILD_SHARED AND (HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES))
    # Python conversion operations execute from the shared hgraph runtime as
    # well as from extension modules. They must all use one nanobind runtime;
    # separate static copies have independent ndarray/type internals.
    install(TARGETS ${_hgraph_nanobind_library}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION "${_hgraph_runtime_destination}"
    )
endif()

if(EXISTS "${PROJECT_SOURCE_DIR}/include")
    install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hh"
            PATTERN "*.hpp"
    )
endif()
install(FILES "${HGRAPH_VERSION_HEADER}" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/hgraph)

if(HGRAPH_BUILD_SHARED AND HGRAPH_BUILD_PYTHON_BINDINGS)
    # Public node-authoring headers include fmt and spdlog headers. Wheel
    # builds fetch both projects, so ship those headers as part of the SDK;
    # implementation libraries remain private inside the hgraph DSOs.
    install(DIRECTORY "${fmt_SOURCE_DIR}/include/fmt"
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    install(DIRECTORY "${spdlog_SOURCE_DIR}/include/spdlog"
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

if(EXISTS "${PROJECT_SOURCE_DIR}/tools/debugger")
    install(DIRECTORY tools/debugger/ DESTINATION ${CMAKE_INSTALL_DATADIR}/hgraph/debugger
        FILES_MATCHING
            PATTERN "*.py"
            PATTERN "README.md"
            PATTERN "__pycache__" EXCLUDE
    )
endif()

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraphConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

set(_hgraph_config_dependencies "find_dependency(Threads)\n")
if(NOT HGRAPH_BUILD_SHARED OR NOT HGRAPH_BUILD_PYTHON_BINDINGS)
    list(APPEND _hgraph_config_dependencies
        "find_dependency(fmt 11 CONFIG)\n"
        "find_dependency(Arrow CONFIG)\n"
        # Some packagers (Conan's Arrow) define the compute/acero targets
        # from the single Arrow config; only find the split packages when
        # the targets are still missing, mirroring the build-side guard.
        "if(NOT TARGET ArrowCompute::arrow_compute_shared AND NOT TARGET ArrowCompute::arrow_compute_static)\n"
        "  find_dependency(ArrowCompute CONFIG)\n"
        "endif()\n"
        "if(NOT TARGET ArrowAcero::arrow_acero_shared AND NOT TARGET ArrowAcero::arrow_acero_static)\n"
        "  find_dependency(ArrowAcero CONFIG)\n"
        "endif()\n"
        "find_dependency(spdlog 1.15 CONFIG)\n"
        # Static consumers link these implementation dependencies. The
        # simdjson floor is explicit because its package version file uses
        # same-minor compatibility.
        "find_dependency(simdjson CONFIG)\n"
        "if(simdjson_VERSION VERSION_LESS 4.5)\n"
        "  message(FATAL_ERROR \"hgraph requires simdjson >= 4.5; found \${simdjson_VERSION}\")\n"
        "endif()\n"
    )
    if(NOT HGRAPH_BUILD_SHARED AND
       _hgraph_selected_time_zone_backend STREQUAL "date")
        list(APPEND _hgraph_config_dependencies
            "find_dependency(date 3.0.4 CONFIG)\n")
    endif()
endif()
string(JOIN "" _hgraph_config_dependencies ${_hgraph_config_dependencies})
if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    if(_hgraph_use_python_stable_abi)
        set(_hgraph_config_python_components "Interpreter Development.Module Development.SABIModule")
        set(_hgraph_config_python_link_target "Python::SABIModule")
    else()
        set(_hgraph_config_python_components "Interpreter Development.Module")
        set(_hgraph_config_python_link_target "Python::Module")
    endif()
    if(HGRAPH_ENABLE_PYTHON_USER_NODES AND NOT _hgraph_use_python_stable_abi)
        set(_hgraph_config_python_components "Interpreter Development.Module Development.Embed")
    endif()
    string(APPEND _hgraph_config_dependencies
        "find_dependency(Python 3.12 COMPONENTS ${_hgraph_config_python_components})\n"
        "execute_process(\n"
        "  COMMAND \"\${Python_EXECUTABLE}\" -m nanobind --cmake_dir\n"
        "  OUTPUT_VARIABLE nanobind_ROOT\n"
        "  OUTPUT_STRIP_TRAILING_WHITESPACE\n"
        "  RESULT_VARIABLE _hgraph_nanobind_result\n"
        ")\n"
        "if(NOT _hgraph_nanobind_result EQUAL 0 OR nanobind_ROOT STREQUAL \"\")\n"
        "  set(hgraph_FOUND FALSE)\n"
        "  set(hgraph_NOT_FOUND_MESSAGE \"hgraph's Python-enabled SDK requires nanobind\")\n"
        "  return()\n"
        "endif()\n"
        "list(PREPEND CMAKE_PREFIX_PATH \"\${nanobind_ROOT}\")\n"
        # nanobind's shared runtime is not ABI-compatible with headers from a
        # different release. Require the exact version used to build the SDK.
        "find_dependency(nanobind ${nanobind_VERSION} EXACT CONFIG)\n"
    )
endif()

file(WRITE "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
    "include(CMakeFindDependencyMacro)\n"
    "${_hgraph_config_dependencies}"
    "include(\"\${CMAKE_CURRENT_LIST_DIR}/hgraphTargets.cmake\")\n"
)
if(NOT HGRAPH_BUILD_SHARED)
    file(APPEND "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
        # FetchContent dependencies are build-interface-only because their
        # targets cannot join hgraph's export set. Reattach the packages
        # resolved above for installed static configurations.
        "if(TARGET hgraph::options)\n"
        "  target_link_libraries(hgraph::options INTERFACE simdjson::simdjson fmt::fmt spdlog::spdlog)\n"
        "  if(TARGET date::date-tz)\n"
        "    target_link_libraries(hgraph::options INTERFACE date::date-tz)\n"
        "  endif()\n"
        "endif()\n"
    )
elseif(NOT HGRAPH_BUILD_PYTHON_BINDINGS)
    file(APPEND "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
        # A native shared SDK uses its system packages as the public-header
        # include/link surface. Python wheels instead ship the required
        # headers and keep implementation dependencies private.
        "if(TARGET hgraph::options)\n"
        "  target_link_libraries(hgraph::options INTERFACE simdjson::simdjson fmt::fmt spdlog::spdlog Arrow::arrow_shared ArrowCompute::arrow_compute_shared ArrowAcero::arrow_acero_shared)\n"
        "endif()\n"
    )
endif()
if(HGRAPH_BUILD_SHARED AND (HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES))
    if(_hgraph_use_python_stable_abi)
        set(_hgraph_config_nanobind_definitions "NB_SHARED;Py_LIMITED_API=0x030C0000")
    else()
        set(_hgraph_config_nanobind_definitions "NB_SHARED")
    endif()
    file(APPEND "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
        # Expose the wheel's installed shared nanobind runtime without taking
        # the mutable target name used internally by nanobind_add_module().
        # Downstream modules use hgraph_add_python_module(), which applies
        # nanobind's module settings while linking this imported runtime.
        "get_filename_component(_hgraph_package_prefix \"\${CMAKE_CURRENT_LIST_DIR}/../../..\" ABSOLUTE)\n"
        "if(NOT TARGET hgraph::nanobind)\n"
        "  add_library(hgraph::nanobind SHARED IMPORTED)\n"
        "  if(WIN32)\n"
        "    set_target_properties(hgraph::nanobind PROPERTIES\n"
        "      IMPORTED_LOCATION \"\${_hgraph_package_prefix}/${_hgraph_runtime_destination}/${_hgraph_nanobind_library}\${CMAKE_SHARED_LIBRARY_SUFFIX}\"\n"
        "      IMPORTED_IMPLIB \"\${_hgraph_package_prefix}/${CMAKE_INSTALL_LIBDIR}/${_hgraph_nanobind_library}\${CMAKE_IMPORT_LIBRARY_SUFFIX}\"\n"
        "    )\n"
        "  else()\n"
        "    set_target_properties(hgraph::nanobind PROPERTIES\n"
        "      IMPORTED_LOCATION \"\${_hgraph_package_prefix}/${CMAKE_INSTALL_LIBDIR}/\${CMAKE_SHARED_LIBRARY_PREFIX}${_hgraph_nanobind_library}\${CMAKE_SHARED_LIBRARY_SUFFIX}\"\n"
        "    )\n"
        "  endif()\n"
        "  set_target_properties(hgraph::nanobind PROPERTIES\n"
        "    INTERFACE_COMPILE_DEFINITIONS \"${_hgraph_config_nanobind_definitions}\"\n"
        "    INTERFACE_INCLUDE_DIRECTORIES \"\${NB_DIR}/include;\${Python_INCLUDE_DIRS}\"\n"
        "    INTERFACE_LINK_LIBRARIES \"${_hgraph_config_python_link_target}\"\n"
        "  )\n"
        "endif()\n"
        "if(TARGET hgraph::options)\n"
        "  target_link_libraries(hgraph::options INTERFACE hgraph::nanobind)\n"
        "endif()\n"
        # Extension modules resolve stable-ABI symbols from their importing
        # interpreter. A native executable has no such host and must instead
        # link the embedding library for the interpreter it will run. Keep
        # that choice target-scoped so downstream extension modules remain
        # minor-independent and manylinux builds need not provide libpython.
        "function(hgraph_link_python_embedding target)\n"
        "  if(NOT TARGET \"\${target}\")\n"
        "    message(FATAL_ERROR \"hgraph_link_python_embedding requires an existing target\")\n"
        "  endif()\n"
        "  get_target_property(_hgraph_embedding_target_type \"\${target}\" TYPE)\n"
        "  if(NOT _hgraph_embedding_target_type STREQUAL \"EXECUTABLE\")\n"
        "    message(FATAL_ERROR \"hgraph_link_python_embedding is only valid for executable targets\")\n"
        "  endif()\n"
        "  if(NOT TARGET Python::Python)\n"
        "    find_package(Python 3.12 COMPONENTS Interpreter Development.Embed REQUIRED)\n"
        "  endif()\n"
        # GNU ld normally uses --as-needed and CMake can place the direct
        # embedding library before hgraph's transitive DSOs. Retain libpython
        # even in that order so their stable-ABI references resolve.
        "  if(UNIX AND NOT APPLE)\n"
        "    target_link_options(\${target} PRIVATE \"LINKER:--no-as-needed\")\n"
        "  endif()\n"
        "  target_link_libraries(\${target} PRIVATE Python::Python)\n"
        "endfunction()\n"
        "function(hgraph_add_python_module name)\n"
        "  cmake_parse_arguments(PARSE_ARGV 1 _hgraph_module\n"
        "    \"STABLE_ABI;PROTECT_STACK;LTO;NOMINSIZE;NOSTRIP\" \"\" \"\")\n"
        "  if(NOT _hgraph_module_STABLE_ABI)\n"
        "    message(FATAL_ERROR \"hgraph_add_python_module currently requires STABLE_ABI\")\n"
        "  endif()\n"
        "  if(Python_VERSION VERSION_LESS 3.12 OR NOT TARGET Python::SABIModule)\n"
        "    message(FATAL_ERROR \"hgraph stable-ABI modules require CPython 3.12+ and Python::SABIModule\")\n"
        "  endif()\n"
        "  add_library(\${name} MODULE \${_hgraph_module_UNPARSED_ARGUMENTS})\n"
        "  nanobind_compile_options(\${name})\n"
        "  nanobind_link_options(\${name})\n"
        "  set_target_properties(\${name} PROPERTIES LINKER_LANGUAGE CXX)\n"
        "  nanobind_extension_abi3(\${name})\n"
        "  target_link_libraries(\${name} PRIVATE hgraph::nanobind)\n"
        "  if(NOT _hgraph_module_PROTECT_STACK)\n"
        "    nanobind_disable_stack_protector(\${name})\n"
        "  endif()\n"
        "  if(NOT _hgraph_module_NOMINSIZE)\n"
        "    nanobind_opt_size(\${name})\n"
        "  endif()\n"
        "  if(NOT _hgraph_module_NOSTRIP)\n"
        "    nanobind_strip(\${name})\n"
        "  endif()\n"
        "  if(_hgraph_module_LTO)\n"
        "    nanobind_lto(\${name})\n"
        "  endif()\n"
        "  nanobind_set_visibility(\${name})\n"
        "  if(APPLE)\n"
        "    set_target_properties(\${name} PROPERTIES INSTALL_RPATH \"@loader_path/${CMAKE_INSTALL_LIBDIR}\")\n"
        "  elseif(UNIX)\n"
        "    set_target_properties(\${name} PROPERTIES INSTALL_RPATH \"$ORIGIN/${CMAKE_INSTALL_LIBDIR}\")\n"
        "  endif()\n"
        "endfunction()\n"
    )
elseif(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    file(APPEND "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
        "if(COMMAND nanobind_build_library AND NOT TARGET ${_hgraph_nanobind_library})\n"
        "  nanobind_build_library(${_hgraph_nanobind_library} AS_SYSINCLUDE)\n"
        "endif()\n"
        "if(TARGET hgraph::options AND TARGET ${_hgraph_nanobind_library})\n"
        "  target_link_libraries(hgraph::options INTERFACE ${_hgraph_nanobind_library})\n"
        "endif()\n"
        "if(TARGET hgraph::options AND DEFINED NB_DIR AND EXISTS \"\${NB_DIR}/include\")\n"
        "  target_include_directories(hgraph::options INTERFACE \"\${NB_DIR}/include\")\n"
        "endif()\n"
        "if(TARGET hgraph::options AND Python_INCLUDE_DIRS)\n"
        "  target_include_directories(hgraph::options INTERFACE \${Python_INCLUDE_DIRS})\n"
        "endif()\n"
    )
endif()
if(HGRAPH_BUILD_SHARED AND HGRAPH_USE_PYARROW_ARROW)
    file(APPEND "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
        # The wheel is also a native SDK. Resolve the exact Arrow runtime
        # supplied by its pyarrow dependency and publish it through unique
        # imported targets: a consumer that constructs Frame/Series values
        # calls Arrow directly, so GNU ld cannot rely on the hgraph DSOs'
        # private DT_NEEDED entries to satisfy those symbols.
        "execute_process(\n"
        "  COMMAND \"\${Python_EXECUTABLE}\" -c \"import pathlib, pyarrow; p = pathlib.Path(pyarrow.__file__).resolve().parent; print(p.as_posix()); print((p / 'include').as_posix())\"\n"
        "  OUTPUT_VARIABLE _hgraph_pyarrow_info\n"
        "  OUTPUT_STRIP_TRAILING_WHITESPACE\n"
        "  RESULT_VARIABLE _hgraph_pyarrow_info_result\n"
        ")\n"
        "if(NOT _hgraph_pyarrow_info_result EQUAL 0)\n"
        "  set(hgraph_FOUND FALSE)\n"
        "  set(hgraph_NOT_FOUND_MESSAGE \"hgraph's shared SDK requires pyarrow\")\n"
        "  return()\n"
        "endif()\n"
        "string(REPLACE \"\\n\" \";\" _hgraph_pyarrow_info \"\${_hgraph_pyarrow_info}\")\n"
        "list(GET _hgraph_pyarrow_info 0 _hgraph_pyarrow_dir)\n"
        "list(GET _hgraph_pyarrow_info 1 _hgraph_arrow_include)\n"
        "if(NOT EXISTS \"\${_hgraph_arrow_include}/arrow/api.h\")\n"
        "  set(hgraph_FOUND FALSE)\n"
        "  set(hgraph_NOT_FOUND_MESSAGE \"hgraph's shared SDK requires pyarrow headers\")\n"
        "  return()\n"
        "endif()\n"
        "if(WIN32)\n"
        "  set(_hgraph_arrow_link \"\${_hgraph_pyarrow_dir}/arrow.lib\")\n"
        "  set(_hgraph_arrow_runtime \"\${_hgraph_pyarrow_dir}/arrow.dll\")\n"
        "  set(_hgraph_arrow_compute_link \"\${_hgraph_pyarrow_dir}/arrow_compute.lib\")\n"
        "  set(_hgraph_arrow_compute_runtime \"\${_hgraph_pyarrow_dir}/arrow_compute.dll\")\n"
        "  set(_hgraph_arrow_acero_link \"\${_hgraph_pyarrow_dir}/arrow_acero.lib\")\n"
        "  set(_hgraph_arrow_acero_runtime \"\${_hgraph_pyarrow_dir}/arrow_acero.dll\")\n"
        "else()\n"
        "  file(GLOB _hgraph_arrow_link LIST_DIRECTORIES false \"\${_hgraph_pyarrow_dir}/libarrow.*\")\n"
        "  file(GLOB _hgraph_arrow_compute_link LIST_DIRECTORIES false \"\${_hgraph_pyarrow_dir}/libarrow_compute.*\")\n"
        "  file(GLOB _hgraph_arrow_acero_link LIST_DIRECTORIES false \"\${_hgraph_pyarrow_dir}/libarrow_acero.*\")\n"
        "  list(SORT _hgraph_arrow_link COMPARE NATURAL)\n"
        "  list(SORT _hgraph_arrow_compute_link COMPARE NATURAL)\n"
        "  list(SORT _hgraph_arrow_acero_link COMPARE NATURAL)\n"
        "  list(GET _hgraph_arrow_link 0 _hgraph_arrow_link)\n"
        "  list(GET _hgraph_arrow_compute_link 0 _hgraph_arrow_compute_link)\n"
        "  list(GET _hgraph_arrow_acero_link 0 _hgraph_arrow_acero_link)\n"
        "  set(_hgraph_arrow_runtime \"\${_hgraph_arrow_link}\")\n"
        "  set(_hgraph_arrow_compute_runtime \"\${_hgraph_arrow_compute_link}\")\n"
        "  set(_hgraph_arrow_acero_runtime \"\${_hgraph_arrow_acero_link}\")\n"
        "endif()\n"
        "foreach(_hgraph_arrow_file IN ITEMS \"\${_hgraph_arrow_link}\" \"\${_hgraph_arrow_runtime}\" \"\${_hgraph_arrow_compute_link}\" \"\${_hgraph_arrow_compute_runtime}\" \"\${_hgraph_arrow_acero_link}\" \"\${_hgraph_arrow_acero_runtime}\")\n"
        "  if(NOT EXISTS \"\${_hgraph_arrow_file}\")\n"
        "    set(hgraph_FOUND FALSE)\n"
        "    set(hgraph_NOT_FOUND_MESSAGE \"hgraph's shared SDK requires pyarrow's Arrow, Compute, and Acero libraries\")\n"
        "    return()\n"
        "  endif()\n"
        "endforeach()\n"
        "if(NOT TARGET hgraph::pyarrow_arrow)\n"
        "  add_library(hgraph::pyarrow_arrow SHARED IMPORTED)\n"
        "  add_library(hgraph::pyarrow_compute SHARED IMPORTED)\n"
        "  add_library(hgraph::pyarrow_acero SHARED IMPORTED)\n"
        "  set_target_properties(hgraph::pyarrow_arrow PROPERTIES IMPORTED_LOCATION \"\${_hgraph_arrow_runtime}\" INTERFACE_INCLUDE_DIRECTORIES \"\${_hgraph_arrow_include}\")\n"
        "  set_target_properties(hgraph::pyarrow_compute PROPERTIES IMPORTED_LOCATION \"\${_hgraph_arrow_compute_runtime}\" INTERFACE_INCLUDE_DIRECTORIES \"\${_hgraph_arrow_include}\")\n"
        "  set_target_properties(hgraph::pyarrow_acero PROPERTIES IMPORTED_LOCATION \"\${_hgraph_arrow_acero_runtime}\" INTERFACE_INCLUDE_DIRECTORIES \"\${_hgraph_arrow_include}\")\n"
        "  if(WIN32)\n"
        "    set_target_properties(hgraph::pyarrow_arrow PROPERTIES IMPORTED_IMPLIB \"\${_hgraph_arrow_link}\")\n"
        "    set_target_properties(hgraph::pyarrow_compute PROPERTIES IMPORTED_IMPLIB \"\${_hgraph_arrow_compute_link}\")\n"
        "    set_target_properties(hgraph::pyarrow_acero PROPERTIES IMPORTED_IMPLIB \"\${_hgraph_arrow_acero_link}\")\n"
        "  endif()\n"
        "endif()\n"
        "if(TARGET hgraph::options)\n"
        "  target_link_libraries(hgraph::options INTERFACE hgraph::pyarrow_arrow hgraph::pyarrow_compute hgraph::pyarrow_acero)\n"
        "endif()\n"
    )
endif()

install(EXPORT hgraphTargets
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraphConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph
)
