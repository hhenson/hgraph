cmake_minimum_required(VERSION 3.25)

set(HGRAPH_ROOT_BUILD_DIR "${CMAKE_BINARY_DIR}")

if (CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(hgraph_tests CXX)

    set(CMAKE_CXX_STANDARD 23)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)

    find_package(Python 3.12 COMPONENTS Interpreter Development Development.Module REQUIRED)
    include_directories(${Python_INCLUDE_DIRS})

    execute_process(
        COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT
    )
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

    get_filename_component(HGRAPH_ROOT_BUILD_DIR "${CMAKE_CURRENT_LIST_DIR}/../../cmake-build-debug" ABSOLUTE)
    if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/conan/build/Debug/generators")
        list(PREPEND CMAKE_PREFIX_PATH "${HGRAPH_ROOT_BUILD_DIR}/conan/build/Debug/generators")
    endif ()
    if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/tpack-build")
        list(PREPEND CMAKE_PREFIX_PATH "${HGRAPH_ROOT_BUILD_DIR}/_deps/tpack-build")
    endif ()
    if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/catch2-src")
        set(FETCHCONTENT_SOURCE_DIR_CATCH2 "${HGRAPH_ROOT_BUILD_DIR}/_deps/catch2-src")
    endif ()
endif ()

# Find Catch2
find_package(Catch2 3 QUIET)
if (NOT Catch2_FOUND)
    message(STATUS "Catch2 not found via find_package, using FetchContent")
    include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v3.5.0
    )
    FetchContent_MakeAvailable(Catch2)
endif()

# Find fmt
find_package(fmt CONFIG QUIET)
if (NOT fmt_FOUND)
    message(STATUS "fmt not found via find_package, using FetchContent")
    include(FetchContent)
    FetchContent_Declare(fmt GIT_REPOSITORY https://github.com/fmtlib/fmt.git GIT_TAG 10.1.0)
    FetchContent_MakeAvailable(fmt)
endif()

set(HGRAPH_TEST_FMT_TARGET fmt::fmt)
if (TARGET fmt::fmt-header-only)
    set(HGRAPH_TEST_FMT_TARGET fmt::fmt-header-only)
endif ()

if (DEFINED HGRAPH_FMT_TARGET)
    set(HGRAPH_TEST_FMT_TARGET ${HGRAPH_FMT_TARGET})
endif ()

find_package(tpack CONFIG QUIET)

if (NOT TARGET Python::Python AND DEFINED Python_EXECUTABLE)
    execute_process(
        COMMAND "${Python_EXECUTABLE}" -c
                "import os, sysconfig; print(os.path.join(sysconfig.get_config_var('LIBDIR') or '', sysconfig.get_config_var('LDLIBRARY') or ''))"
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE HGRAPH_TEST_PYTHON_LIBRARY
    )

    if (EXISTS "${HGRAPH_TEST_PYTHON_LIBRARY}")
        add_library(Python::Python UNKNOWN IMPORTED)
        set_target_properties(Python::Python PROPERTIES
            IMPORTED_LOCATION "${HGRAPH_TEST_PYTHON_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${Python_INCLUDE_DIRS}"
        )
    endif ()
endif ()

# Create test executable
add_executable(hgraph_visitor_tests
    test_visitor_standalone.cpp
    test_discriminated_ptr.cpp
)

target_include_directories(hgraph_visitor_tests PRIVATE
    ../include
)

target_link_libraries(hgraph_visitor_tests PRIVATE
    Catch2::Catch2WithMain
    ${HGRAPH_TEST_FMT_TARGET}
)

# Set C++ standard
target_compile_features(hgraph_visitor_tests PRIVATE cxx_std_23)

set(HGRAPH_TEST_CORE_RUNTIME_SOURCES
    ../src/cpp/types/constants.cpp
    ../src/cpp/types/traits.cpp
    ../src/cpp/types/value/type_meta_bindings.cpp
    ../src/cpp/types/value/type_registry.cpp
    ../src/cpp/util/lifecycle.cpp
    ../src/cpp/util/stack_trace.cpp
    ../src/cpp/util/string_util.cpp
)

set(HGRAPH_TEST_TIME_SERIES_SOURCES
    ../src/cpp/types/time_series/value/atomic.cpp
    ../src/cpp/types/time_series/value/associative.cpp
    ../src/cpp/types/time_series/value/list.cpp
    ../src/cpp/types/time_series/value/record.cpp
    ../src/cpp/types/time_series/value/sequence.cpp
    ../src/cpp/types/time_series/value/builder.cpp
    ../src/cpp/types/time_series/value/value.cpp
    ../src/cpp/types/time_series/ts_value_builder.cpp
    ../src/cpp/types/time_series/ts_value.cpp
    ../src/cpp/types/time_series/ts_input_builder.cpp
    ../src/cpp/types/time_series/ts_input.cpp
    ../src/cpp/types/time_series/ts_input_view.cpp
    ../src/cpp/types/time_series/ts_output.cpp
    ../src/cpp/types/time_series/ts_output_builder.cpp
    ../src/cpp/types/time_series/time_series_state.cpp
    ../src/cpp/types/time_series/active_trie.cpp
    ../src/cpp/types/time_series/ts_type_registry.cpp
)

set(HGRAPH_TEST_V2_RUNTIME_SOURCES
    ../src/cpp/types/boundary_binding.cpp
    ../src/cpp/types/child_graph.cpp
    ../src/cpp/types/evaluation_engine.cpp
    ../src/cpp/types/graph.cpp
    ../src/cpp/types/graph_builder.cpp
    ../src/cpp/types/nested_clock.cpp
    ../src/cpp/types/node.cpp
    ../src/cpp/types/node_builder.cpp
    ../src/cpp/types/python_node_support.cpp
    ../src/cpp/types/ref.cpp
)

add_executable(hgraph_time_series_value_tests
    test_active_trie.cpp
    test_time_series_associative_value.cpp
    test_time_series_atomic_value.cpp
    test_time_series_list_value.cpp
    test_time_series_record_value.cpp
    test_time_series_sequence_value.cpp
    test_time_series_ts_value.cpp
    ${HGRAPH_TEST_CORE_RUNTIME_SOURCES}
    ${HGRAPH_TEST_TIME_SERIES_SOURCES}
    ${HGRAPH_TEST_V2_RUNTIME_SOURCES}
)


target_include_directories(hgraph_time_series_value_tests PRIVATE
    ../include
    ${CMAKE_BINARY_DIR}/generated
)

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/ddvisitor-src/include")
    target_include_directories(hgraph_time_series_value_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/ddvisitor-src/include"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/tpack-src/include")
    target_include_directories(hgraph_time_series_value_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/tpack-src/include"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/dynamic_bitset-src/include")
    target_include_directories(hgraph_time_series_value_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/dynamic_bitset-src/include"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-src/src")
    target_include_directories(hgraph_time_series_value_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-src/src"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-build/src")
    target_include_directories(hgraph_time_series_value_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-build/src"
    )
endif ()

target_link_libraries(hgraph_time_series_value_tests PRIVATE
    Catch2::Catch2WithMain
    ${HGRAPH_TEST_FMT_TARGET}
)

if (TARGET Python::Python)
    target_link_libraries(hgraph_time_series_value_tests PRIVATE Python::Python)
elseif (TARGET Python::Module)
    target_link_libraries(hgraph_time_series_value_tests PRIVATE Python::Module)
endif ()

if (TARGET nanobind::headers)
    target_link_libraries(hgraph_time_series_value_tests PRIVATE nanobind::headers)
endif ()

if (TARGET Threads::Threads)
    target_link_libraries(hgraph_time_series_value_tests PRIVATE Threads::Threads)
endif ()

if (COMMAND nanobind_build_library)
    nanobind_build_library(nanobind-static)
endif ()

if (TARGET nanobind-static)
    target_link_libraries(hgraph_time_series_value_tests PRIVATE nanobind-static)
endif ()

if (TARGET tpack)
    target_link_libraries(hgraph_time_series_value_tests PRIVATE tpack)
elseif (TARGET tpack::tpack)
    target_link_libraries(hgraph_time_series_value_tests PRIVATE tpack::tpack)
endif ()

target_compile_features(hgraph_time_series_value_tests PRIVATE cxx_std_23)

add_executable(hgraph_v2_tests
    test_types_v2_graph.cpp
    test_types_v2_nested.cpp
    ${HGRAPH_TEST_CORE_RUNTIME_SOURCES}
    ${HGRAPH_TEST_TIME_SERIES_SOURCES}
    ${HGRAPH_TEST_V2_RUNTIME_SOURCES}
)

target_include_directories(hgraph_v2_tests PRIVATE
    ../include
    ${CMAKE_BINARY_DIR}/generated
)

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/ddvisitor-src/include")
    target_include_directories(hgraph_v2_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/ddvisitor-src/include"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/tpack-src/include")
    target_include_directories(hgraph_v2_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/tpack-src/include"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/dynamic_bitset-src/include")
    target_include_directories(hgraph_v2_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/dynamic_bitset-src/include"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-src/src")
    target_include_directories(hgraph_v2_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-src/src"
    )
endif ()

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-build/src")
    target_include_directories(hgraph_v2_tests PRIVATE
        "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-build/src"
    )
endif ()

target_link_libraries(hgraph_v2_tests PRIVATE
    Catch2::Catch2WithMain
    ${HGRAPH_TEST_FMT_TARGET}
)

if (TARGET Python::Python)
    target_link_libraries(hgraph_v2_tests PRIVATE Python::Python)
elseif (TARGET Python::Module)
    target_link_libraries(hgraph_v2_tests PRIVATE Python::Module)
endif ()

if (TARGET nanobind::headers)
    target_link_libraries(hgraph_v2_tests PRIVATE nanobind::headers)
endif ()

if (TARGET Threads::Threads)
    target_link_libraries(hgraph_v2_tests PRIVATE Threads::Threads)
endif ()

if (TARGET nanobind-static)
    target_link_libraries(hgraph_v2_tests PRIVATE nanobind-static)
endif ()

if (TARGET tpack)
    target_link_libraries(hgraph_v2_tests PRIVATE tpack)
elseif (TARGET tpack::tpack)
    target_link_libraries(hgraph_v2_tests PRIVATE tpack::tpack)
endif ()

target_compile_features(hgraph_v2_tests PRIVATE cxx_std_23)
execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import sys; print(sys.base_prefix)"
    OUTPUT_STRIP_TRAILING_WHITESPACE
    OUTPUT_VARIABLE HGRAPH_TEST_PYTHON_HOME
)
target_compile_definitions(hgraph_v2_tests PRIVATE
    HGRAPH_TEST_PYTHON_EXECUTABLE="${Python_EXECUTABLE}"
    HGRAPH_TEST_PYTHON_HOME="${HGRAPH_TEST_PYTHON_HOME}"
)

# Register tests with CTest
include(CTest)
include(Catch)
catch_discover_tests(hgraph_visitor_tests)
catch_discover_tests(hgraph_time_series_value_tests)
catch_discover_tests(hgraph_v2_tests)

# Optional: Add custom target to run tests
add_custom_target(run_visitor_tests
    COMMAND hgraph_visitor_tests
    DEPENDS hgraph_visitor_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running visitor pattern tests"
)

add_custom_target(run_time_series_value_tests
    COMMAND hgraph_time_series_value_tests
    DEPENDS hgraph_time_series_value_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running time-series value tests"
)
