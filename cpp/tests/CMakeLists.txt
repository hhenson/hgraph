cmake_minimum_required(VERSION 3.25)
project(hgraph_tests CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

get_filename_component(HGRAPH_REPO_ROOT "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
set(HGRAPH_ROOT_BUILD_DIR "" CACHE PATH "Top-level hgraph build directory providing generated headers and fetched dependencies")

if (HGRAPH_ROOT_BUILD_DIR)
    get_filename_component(HGRAPH_ROOT_BUILD_DIR "${HGRAPH_ROOT_BUILD_DIR}" ABSOLUTE BASE_DIR "${HGRAPH_REPO_ROOT}")
else ()
    get_filename_component(_HGRAPH_PARENT_BUILD_DIR "${CMAKE_BINARY_DIR}" DIRECTORY)
    foreach (_HGRAPH_BUILD_DIR_CANDIDATE
            "${_HGRAPH_PARENT_BUILD_DIR}"
            "${HGRAPH_REPO_ROOT}/build")
        get_filename_component(_HGRAPH_BUILD_DIR_CANDIDATE "${_HGRAPH_BUILD_DIR_CANDIDATE}" ABSOLUTE)
        if (EXISTS "${_HGRAPH_BUILD_DIR_CANDIDATE}/cpp/generated")
            set(HGRAPH_ROOT_BUILD_DIR "${_HGRAPH_BUILD_DIR_CANDIDATE}")
            break()
        endif ()
    endforeach ()
endif ()

if (NOT HGRAPH_ROOT_BUILD_DIR)
    message(FATAL_ERROR
        "Unable to locate the top-level hgraph build directory required by standalone runtime tests.\n"
        "Pass -DHGRAPH_ROOT_BUILD_DIR=/path/to/top-level/build when configuring this test project.")
endif ()

set(HGRAPH_GENERATED_INCLUDE_DIR "${HGRAPH_ROOT_BUILD_DIR}/cpp/generated")
set(HGRAPH_DDVISITOR_INCLUDE_DIR "${HGRAPH_ROOT_BUILD_DIR}/_deps/ddvisitor-src/include")
set(HGRAPH_TPACK_INCLUDE_DIR "${HGRAPH_ROOT_BUILD_DIR}/_deps/tpack-src/include")
set(HGRAPH_DYNAMIC_BITSET_INCLUDE_DIR "${HGRAPH_ROOT_BUILD_DIR}/_deps/dynamic_bitset-src/include")
set(HGRAPH_NANOARROW_INCLUDE_DIR "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-src/src")
set(HGRAPH_NANOARROW_BUILD_INCLUDE_DIR "${HGRAPH_ROOT_BUILD_DIR}/_deps/nanoarrow-build/src")

if (NOT EXISTS "${HGRAPH_GENERATED_INCLUDE_DIR}")
    message(FATAL_ERROR
        "Standalone runtime tests require generated headers from the top-level C++ build.\n"
        "Expected to find: ${HGRAPH_GENERATED_INCLUDE_DIR}\n"
        "Configure the main C++ build in ${HGRAPH_ROOT_BUILD_DIR} first.")
endif ()

if (NOT EXISTS "${HGRAPH_DDVISITOR_INCLUDE_DIR}/ddv/visitable.h")
    message(FATAL_ERROR
        "Standalone runtime tests require ddvisitor headers from the top-level C++ build.\n"
        "Expected to find: ${HGRAPH_DDVISITOR_INCLUDE_DIR}/ddv/visitable.h")
endif ()

if (NOT EXISTS "${HGRAPH_TPACK_INCLUDE_DIR}/tp/tpack.h")
    message(FATAL_ERROR
        "Standalone runtime tests require tpack headers from the top-level C++ build.\n"
        "Expected to find: ${HGRAPH_TPACK_INCLUDE_DIR}/tp/tpack.h")
endif ()

if (NOT EXISTS "${HGRAPH_DYNAMIC_BITSET_INCLUDE_DIR}/sul/dynamic_bitset.hpp")
    message(FATAL_ERROR
        "Standalone runtime tests require dynamic_bitset headers from the top-level C++ build.\n"
        "Expected to find: ${HGRAPH_DYNAMIC_BITSET_INCLUDE_DIR}/sul/dynamic_bitset.hpp")
endif ()

if (NOT EXISTS "${HGRAPH_NANOARROW_INCLUDE_DIR}/nanoarrow/nanoarrow.h")
    message(FATAL_ERROR
        "Standalone runtime tests require nanoarrow headers from the top-level C++ build.\n"
        "Expected to find: ${HGRAPH_NANOARROW_INCLUDE_DIR}/nanoarrow/nanoarrow.h")
endif ()

if (NOT EXISTS "${HGRAPH_NANOARROW_BUILD_INCLUDE_DIR}/nanoarrow/nanoarrow_config.h")
    message(FATAL_ERROR
        "Standalone runtime tests require generated nanoarrow config headers from the top-level C++ build.\n"
        "Expected to find: ${HGRAPH_NANOARROW_BUILD_INCLUDE_DIR}/nanoarrow/nanoarrow_config.h")
endif ()

if (DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
endif ()

if (NOT DEFINED Python_EXECUTABLE AND EXISTS "${HGRAPH_REPO_ROOT}/.venv/bin/python")
    set(Python_EXECUTABLE "${HGRAPH_REPO_ROOT}/.venv/bin/python" CACHE FILEPATH "Python executable for standalone C++ tests" FORCE)
endif ()

find_package(Python 3.12 COMPONENTS Interpreter Development Development.Module REQUIRED)
include_directories(${Python_INCLUDE_DIRS})

execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import sys; print(sys.base_prefix)"
    OUTPUT_STRIP_TRAILING_WHITESPACE
    OUTPUT_VARIABLE HGRAPH_TEST_PYTHON_HOME
)

execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import sysconfig; print(sysconfig.get_path('purelib'))"
    OUTPUT_STRIP_TRAILING_WHITESPACE
    OUTPUT_VARIABLE HGRAPH_TEST_PYTHONPATH
)

execute_process(
    COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE
    OUTPUT_VARIABLE nanobind_ROOT
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
find_package(Threads QUIET)

if (EXISTS "${HGRAPH_ROOT_BUILD_DIR}/_deps/catch2-src")
    set(FETCHCONTENT_SOURCE_DIR_CATCH2 "${HGRAPH_ROOT_BUILD_DIR}/_deps/catch2-src")
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

# Create test executable
add_executable(hgraph_visitor_tests
    test_visitor_standalone.cpp
)

target_link_libraries(hgraph_visitor_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
)

# Set C++ standard
target_compile_features(hgraph_visitor_tests PRIVATE cxx_std_23)

add_executable(hgraph_runtime_tests
    nanobind_intrusive_counter.cpp
    test_evaluation_engine.cpp
    ../src/cpp/runtime/evaluation_engine.cpp
)

target_include_directories(hgraph_runtime_tests PRIVATE
    ../include
    ${NB_DIR}/include
    ${HGRAPH_GENERATED_INCLUDE_DIR}
    ${HGRAPH_DDVISITOR_INCLUDE_DIR}
    ${HGRAPH_DYNAMIC_BITSET_INCLUDE_DIR}
    ${HGRAPH_NANOARROW_INCLUDE_DIR}
    ${HGRAPH_NANOARROW_BUILD_INCLUDE_DIR}
    ${HGRAPH_TPACK_INCLUDE_DIR}
)

target_link_libraries(hgraph_runtime_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
)

if (TARGET Python::Python)
    target_link_libraries(hgraph_runtime_tests PRIVATE Python::Python)
elseif (TARGET Python::Module)
    target_link_libraries(hgraph_runtime_tests PRIVATE Python::Module)
endif ()

if (TARGET nanobind::headers)
    target_link_libraries(hgraph_runtime_tests PRIVATE nanobind::headers)
endif ()

if (COMMAND nanobind_build_library)
    nanobind_build_library(nanobind-static)
endif ()

if (TARGET nanobind-static)
    target_link_libraries(hgraph_runtime_tests PRIVATE nanobind-static)
endif ()

if (TARGET Threads::Threads)
    target_link_libraries(hgraph_runtime_tests PRIVATE Threads::Threads)
endif ()

target_compile_features(hgraph_runtime_tests PRIVATE cxx_std_23)

add_executable(hgraph_v2_cpp_wiring_tests
    test_v2_cpp_wiring.cpp
    time_series_reference_stub.cpp
    ../src/cpp/types/value/type_registry.cpp
    ../src/cpp/types/time_series/ts_type_registry.cpp
)

target_include_directories(hgraph_v2_cpp_wiring_tests PRIVATE
    ../include
    ${HGRAPH_GENERATED_INCLUDE_DIR}
    ${HGRAPH_DDVISITOR_INCLUDE_DIR}
    ${HGRAPH_DYNAMIC_BITSET_INCLUDE_DIR}
    ${HGRAPH_NANOARROW_INCLUDE_DIR}
    ${HGRAPH_NANOARROW_BUILD_INCLUDE_DIR}
    ${HGRAPH_TPACK_INCLUDE_DIR}
)

target_link_libraries(hgraph_v2_cpp_wiring_tests PRIVATE
    Catch2::Catch2WithMain
    fmt::fmt
)

if (TARGET Python::Python)
    target_link_libraries(hgraph_v2_cpp_wiring_tests PRIVATE Python::Python)
elseif (TARGET Python::Module)
    target_link_libraries(hgraph_v2_cpp_wiring_tests PRIVATE Python::Module)
endif ()

if (TARGET nanobind::headers)
    target_link_libraries(hgraph_v2_cpp_wiring_tests PRIVATE nanobind::headers)
endif ()

if (TARGET nanobind-static)
    target_link_libraries(hgraph_v2_cpp_wiring_tests PRIVATE nanobind-static)
endif ()

if (TARGET Threads::Threads)
    target_link_libraries(hgraph_v2_cpp_wiring_tests PRIVATE Threads::Threads)
endif ()

target_compile_definitions(hgraph_v2_cpp_wiring_tests PRIVATE
    HGRAPH_TEST_PYTHON_EXECUTABLE="${Python_EXECUTABLE}"
    HGRAPH_TEST_PYTHON_HOME="${HGRAPH_TEST_PYTHON_HOME}"
    HGRAPH_TEST_PYTHONPATH="${HGRAPH_TEST_PYTHONPATH}"
)

target_compile_features(hgraph_v2_cpp_wiring_tests PRIVATE cxx_std_23)

add_executable(hgraph_v2_memory_utils_tests
    test_v2_memory_utils.cpp
)

target_include_directories(hgraph_v2_memory_utils_tests PRIVATE
    ../include
)

target_link_libraries(hgraph_v2_memory_utils_tests PRIVATE
    Catch2::Catch2WithMain
)

target_compile_features(hgraph_v2_memory_utils_tests PRIVATE cxx_std_23)

add_executable(hgraph_scope_tests
    test_scope.cpp
)

target_include_directories(hgraph_scope_tests PRIVATE
    ../include
)

target_link_libraries(hgraph_scope_tests PRIVATE
    Catch2::Catch2WithMain
)

target_compile_features(hgraph_scope_tests PRIVATE cxx_std_23)

add_executable(hgraph_tagged_ptr_tests
    test_discriminated_ptr.cpp
)

target_include_directories(hgraph_tagged_ptr_tests PRIVATE
    ../include
)

target_link_libraries(hgraph_tagged_ptr_tests PRIVATE
    Catch2::Catch2WithMain
)

target_compile_features(hgraph_tagged_ptr_tests PRIVATE cxx_std_23)

# Enable testing
enable_testing()

# Register tests with CTest
include(CTest)
include(Catch)
catch_discover_tests(hgraph_visitor_tests)
catch_discover_tests(hgraph_runtime_tests)
catch_discover_tests(hgraph_v2_cpp_wiring_tests)
catch_discover_tests(hgraph_v2_memory_utils_tests)
catch_discover_tests(hgraph_scope_tests)
catch_discover_tests(hgraph_tagged_ptr_tests)

# Optional: Add custom target to run tests
add_custom_target(run_visitor_tests
    COMMAND hgraph_visitor_tests
    DEPENDS hgraph_visitor_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running visitor pattern tests"
)

add_custom_target(run_runtime_tests
    COMMAND hgraph_runtime_tests
    DEPENDS hgraph_runtime_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running runtime tests"
)

add_custom_target(run_v2_cpp_wiring_tests
    COMMAND hgraph_v2_cpp_wiring_tests
    DEPENDS hgraph_v2_cpp_wiring_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running v2 cpp wiring tests"
)
