cmake_minimum_required(VERSION 3.25)

project(hgraph_cpp_engine)

option(NB_USE_STABLE_ABI "Use nanobind stable ABI for the Python module" ON)
option(HGRAPH_WITH_BACKWARD "Enable backward-cpp stack traces" ON)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (UNIX)
    set(CMAKE_CXX_FLAGS_RELEASE "-g -O3")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O3 -DNDEBUG")
    set(CMAKE_CXX_FLAGS_DEBUG "-g -v")
    set(CMAKE_CXX_FLAGS "-Wall")
elseif (MSVC)
    set(CMAKE_CXX_FLAGS_RELEASE "/O2 /Zi /DNDEBUG")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "/O2 /Zi /DNDEBUG")
    set(CMAKE_CXX_FLAGS_DEBUG "/Zi /Od")
    set(CMAKE_CXX_FLAGS "/W3 /EHsc")
endif ()

if (APPLE)
    # Avoid forcing Homebrew include/lib paths which can break cross-arch builds under cibuildwheel.
    # Toolchain and dependencies are discovered via CMake and FetchContent/Conan instead.
    message(STATUS "Apple platform detected. Not forcing Homebrew include/lib paths.")

    # Surface effective macOS configuration for diagnostics
    message(STATUS "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    message(STATUS "CMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}")
    message(STATUS "CMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}")
    message(STATUS "ENV{MACOSX_DEPLOYMENT_TARGET}=$ENV{MACOSX_DEPLOYMENT_TARGET}")

    # If no deployment target is provided, set a sane default that enables libc++ floating-point to_chars
    if (NOT DEFINED CMAKE_OSX_DEPLOYMENT_TARGET OR CMAKE_OSX_DEPLOYMENT_TARGET STREQUAL "")
        set(CMAKE_OSX_DEPLOYMENT_TARGET "15.0" CACHE STRING "macOS deployment target" FORCE)
        set(ENV{MACOSX_DEPLOYMENT_TARGET} "15.0")
        message(STATUS "CMAKE_OSX_DEPLOYMENT_TARGET was unset. Defaulting to 15.0")
    endif ()
endif ()

# Python and nanobind
# Normalize variables from callers and ALWAYS use FindPython (not FindPython3),
# because nanobind-config.cmake requires that 'find_package(Python ...)' has been invoked.
if (DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
endif ()
# Required: Python interpreter + development module
find_package(Python 3.12 COMPONENTS Interpreter Development.Module REQUIRED)
include_directories(${Python_INCLUDE_DIRS})

# Get Python SOABI to avoid double .so extension in nanobind
execute_process(
        COMMAND "${Python_EXECUTABLE}" -c "import sysconfig; print(sysconfig.get_config_var('SOABI'), end='')"
        OUTPUT_VARIABLE Python_SOABI
        RESULT_VARIABLE _result)
if (_result EQUAL 0 AND Python_SOABI)
    set(Python_SOABI "${Python_SOABI}" CACHE STRING "Python SOABI" FORCE)
endif ()

# Resolve nanobind via the chosen interpreter and help CMake find it
execute_process(
        COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
if (nanobind_ROOT)
    set(nanobind_DIR "${nanobind_ROOT}" CACHE PATH "nanobind CMake package dir" FORCE)
    if (DEFINED CMAKE_PREFIX_PATH)
        list(PREPEND CMAKE_PREFIX_PATH "${nanobind_ROOT}")
    else ()
        set(CMAKE_PREFIX_PATH "${nanobind_ROOT}")
    endif ()
endif ()
find_package(nanobind CONFIG REQUIRED)
include_directories(${NB_DIR}/include)
message(STATUS "Nanobind DIR: ${NB_DIR}/include")

find_package(Threads)

# fmt via package or FetchContent
message(STATUS "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")
find_package(fmt CONFIG QUIET)
if (NOT fmt_FOUND)
    message(STATUS "fmt not found via find_package, using FetchContent")
    include(FetchContent)
    FetchContent_Declare(fmt GIT_REPOSITORY https://github.com/fmtlib/fmt.git GIT_TAG 10.1.0)
    FetchContent_MakeAvailable(fmt)
else ()
    message(STATUS "Found fmt via Conan/system: ${fmt_DIR}")
endif ()


# backward-cpp via package or FetchContent (optional)
if (HGRAPH_WITH_BACKWARD)
    find_package(Backward CONFIG QUIET)
    if (NOT Backward_FOUND)
        message(STATUS "Backward not found via find_package, using FetchContent")
        include(FetchContent)
        set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
        FetchContent_Declare(backward GIT_REPOSITORY https://github.com/bombela/backward-cpp.git GIT_TAG v1.6)
        # On manylinux and constrained envs, disable non-portable features
        set(BACKWARD_ENABLE ON CACHE BOOL "Enable backward" FORCE)
        set(BACKWARD_HAS_BFD OFF CACHE BOOL "Disable BFD" FORCE)
        set(BACKWARD_HAS_DW OFF CACHE BOOL "Disable DW" FORCE)
        set(BACKWARD_HAS_DWARF OFF CACHE BOOL "Disable DWARF" FORCE)
        set(BACKWARD_HAS_UNWIND ON CACHE BOOL "Enable libunwind/backtrace if available" FORCE)
        FetchContent_MakeAvailable(backward)
        if (TARGET backward)
            add_library(Backward::Backward ALIAS backward)
        endif ()
    else ()
        message(STATUS "Found Backward via Conan/system: ${Backward_DIR}")
    endif ()
else ()
    message(STATUS "HGRAPH_WITH_BACKWARD=OFF: Skipping backward-cpp integration")
endif ()

# Git version info is optional here (no .git necessarily inside this repo)
set(GIT_BRANCH "")
set(GIT_COMMIT_HASH "")
set(GIT_COMMIT_DATE "")
if (EXISTS "${CMAKE_SOURCE_DIR}/.git")
    execute_process(COMMAND git rev-parse --abbrev-ref HEAD WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_BRANCH OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND git log -1 --format=%H WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_COMMIT_HASH OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND git log -1 --format=%cD WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_COMMIT_DATE OUTPUT_STRIP_TRAILING_WHITESPACE)
endif ()

message(STATUS "Git current branch: ${GIT_BRANCH}")
message(STATUS "Git commit hash: ${GIT_COMMIT_HASH}")
message(STATUS "Git commit date: ${GIT_COMMIT_DATE}")

# Generate version.h into the binary dir
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/include/hgraph/version.h.in
        ${CMAKE_CURRENT_BINARY_DIR}/generated/version.h
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

set(HGRAPH_INCLUDES
        include/hgraph/hgraph_forward_declarations.h
        include/hgraph/builders/builder.h
        include/hgraph/builders/graph_builder.h
        include/hgraph/builders/input_builder.h
        include/hgraph/builders/node_builder.h
        include/hgraph/builders/output_builder.h
        include/hgraph/nodes/base_python_node.h
        include/hgraph/nodes/component_node.h
        include/hgraph/nodes/last_value_pull_node.h
        include/hgraph/nodes/mesh_node.h
        include/hgraph/nodes/nest_graph_node.h
        include/hgraph/nodes/nested_evaluation_engine.h
        include/hgraph/nodes/nested_node.h
        include/hgraph/nodes/python_node.h
        include/hgraph/nodes/non_associative_reduce_node.h
        include/hgraph/nodes/python_generator_node.h
        include/hgraph/nodes/push_queue_node.h
        include/hgraph/nodes/reduce_node.h
        include/hgraph/nodes/switch_node.h
        include/hgraph/nodes/try_except_node.h
        include/hgraph/nodes/tsd_map_node.h
        include/hgraph/runtime/evaluation_context.h
        include/hgraph/runtime/evaluation_engine.h
        include/hgraph/runtime/graph_executor.h
        include/hgraph/runtime/record_replay.h
        include/hgraph/runtime/observers/evaluation_profiler.h
        include/hgraph/runtime/observers/evaluation_trace.h
        include/hgraph/runtime/observers/inspection_observer.h
        include/hgraph/types/base_time_series.h
        include/hgraph/types/constants.h
        include/hgraph/types/error_type.h
        include/hgraph/types/feature_extension.h
        include/hgraph/types/graph.h
        include/hgraph/types/node.h
        include/hgraph/types/ref.h
        include/hgraph/types/scalar_types.h
        include/hgraph/types/schema_type.h
        include/hgraph/types/time_series_type.h
        include/hgraph/types/traits.h
        include/hgraph/types/ts.h
        include/hgraph/types/ts_signal.h
        include/hgraph/types/tsd.h
        include/hgraph/types/tss.h
        include/hgraph/types/ts_indexed.h
        include/hgraph/types/tsb.h
        include/hgraph/types/tsl.h
        include/hgraph/python/chrono.h
        include/hgraph/python/format.h
        include/hgraph/python/global_state.h
        include/hgraph/python/global_keys.h
        include/hgraph/python/hashable.h
        include/hgraph/python/nb_types_ext.h
        include/hgraph/python/reference_wrapper.h
        include/hgraph/util/date_time.h
        include/hgraph/util/lifecycle.h
        include/hgraph/util/reference_count_subscriber.h
        include/hgraph/util/sender_receiver_state.h
        include/hgraph/util/stack_trace.h
        include/hgraph/util/string_utils.h
        include/hgraph/hgraph_export.h
)
list(TRANSFORM HGRAPH_INCLUDES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")

# Expose includes var to src/cpp subdirectory
set(HGRAPH_INCLUDES ${HGRAPH_INCLUDES} PARENT_SCOPE)

add_subdirectory(src/cpp)

# Optional: Build tests if enabled
# Tests are independent and don't require Python bindings
option(HGRAPH_BUILD_TESTS "Build hgraph C++ unit tests" OFF)
if (HGRAPH_BUILD_TESTS AND NOT CMAKE_CROSSCOMPILING)
    message(STATUS "Building hgraph C++ tests")
    add_subdirectory(tests)
endif()
