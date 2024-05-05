cmake_minimum_required(VERSION 3.27)
project(hgraph_cpp_engine)

set(CMAKE_CXX_STANDARD 23)

if (UNIX)
    set(PYBIND11_CPP_STANDARD -std=c++23)
    set(CMAKE_CXX_FLAGS_RELEASE "-g -o3")
    set(CMAKE_CXX_FLAGS_DEBUG "-g -v -fsanitize=address")
    set(CMAKE_CXX_FLAGS "-Wall")
endif (UNIX)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    # Set properties specific to macOS
    # Check for Apple Silicon (ARM architecture)
    if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64")
        message(STATUS "Configuring for Apple Silicon (ARM64)")
        include_directories(/opt/homebrew/include)
        link_directories(/opt/homebrew/lib)
    else (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64")
        message(STATUS "Configuring for Intel (x86_64)")
        include_directories(/usr/local/include)
        link_directories(/usr/local/lib)
    endif (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64")
endif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

find_package(spdlog CONFIG REQUIRED)

find_package(Python3 3.12 REQUIRED COMPONENTS Interpreter Development)
include_directories(${Python3_INCLUDE_DIRS})
link_libraries(${Python3_LIBRARIES})

find_package(pybind11 CONFIG)


find_package(Boost REQUIRED)
find_package(Threads)
find_package(range-v3 CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
#find_package(immer CONFIG REQUIRED)

if (EXISTS "${CMAKE_SOURCE_DIR}/.git")
    execute_process(
            COMMAND git rev-parse --abbrev-ref HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_BRANCH
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
            COMMAND git log -1 --format=%H
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
            COMMAND git log -1 --format=%cD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_COMMIT_DATE
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else (EXISTS "${CMAKE_SOURCE_DIR}/.git")
    set(GIT_BRANCH "")
    set(GIT_COMMIT_HASH "")
    set(GIT_COMMIT_DATE "")
endif (EXISTS "${CMAKE_SOURCE_DIR}/.git")

message(STATUS "Git current branch: ${GIT_BRANCH}")
message(STATUS "Git commit hash: ${GIT_COMMIT_HASH}")
message(STATUS "Git commit date: ${GIT_COMMIT_DATE}")

message(STATUS "Generating version.h")
configure_file(
        ${CMAKE_SOURCE_DIR}/include/hgraph/version.h.in
        ${CMAKE_BINARY_DIR}/generated/version.h
)

include_directories(include)

set(HGRAPH_INCLUDES
        include/hgraph/types/meta/type_meta_data.h

        include/hgraph/runtime/evaluation_engine.h
        include/hgraph/types/graph.h
        include/hgraph/types/node.h
        include/hgraph/types/scalar_value.h
        include/hgraph/types/schema.h

        include/hgraph/python/pyb.h
        include/hgraph/python/pyb_wiring.h

        include/hgraph/util/date_time.h
        include/hgraph/util/lifecycle.h
        include/hgraph/util/reference_count_subscriber.h
)

list(TRANSFORM HGRAPH_INCLUDES PREPEND "${CMAKE_SOURCE_DIR}/")

list(APPEND HGRAPH_INCLUDES ${CMAKE_BINARY_DIR}/generated/version.h)

add_subdirectory(src/cpp)
add_subdirectory(tests/cpp)

macro(dump_cmake_variables)
    message(STATUS "================ All cmake variables ==================")
    get_cmake_property(_variableNames VARIABLES)
    foreach (_variableName ${_variableNames})
        message(STATUS "${_variableName}=${${_variableName}}")
    endforeach ()
    message(STATUS "================ All cmake variables DONE ============")
endmacro()


set(DUMP_CMAKE_VARIABLES ON)

if (DUMP_CMAKE_VARIABLES)
    dump_cmake_variables()
endif ()