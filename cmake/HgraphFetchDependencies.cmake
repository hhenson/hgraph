include_guard(GLOBAL)
include(FetchContent)
option(HGRAPH_FETCH_MISSING_DEPENDENCIES "Fetch supported C++ packages when discovery fails" ON)

macro(hgraph_fetch_fmt)
    if(NOT DEFINED HGRAPH_FMT_FETCH_VERSION)
        set(HGRAPH_FMT_FETCH_VERSION 12.2.0)
    endif()
    set(fmt_VERSION "${HGRAPH_FMT_FETCH_VERSION}")
    if(NOT HGRAPH_FETCH_MISSING_DEPENDENCIES AND NOT HGRAPH_BUILD_PYTHON_BINDINGS)
        message(FATAL_ERROR "Missing fmt; install its CMake package or enable HGRAPH_FETCH_MISSING_DEPENDENCIES")
    endif()
    FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG        ${HGRAPH_FMT_FETCH_VERSION}
        GIT_SHALLOW    TRUE
        SYSTEM
    )
    set(FMT_DOC OFF CACHE BOOL "Build fmt documentation" FORCE)
    set(FMT_INSTALL OFF CACHE BOOL "Generate fmt install target" FORCE)
    set(FMT_TEST OFF CACHE BOOL "Build fmt tests" FORCE)
    FetchContent_MakeAvailable(fmt)
    set_property(TARGET fmt PROPERTY POSITION_INDEPENDENT_CODE ON)
    if(TARGET fmt-c)
        set_target_properties(fmt-c PROPERTIES EXCLUDE_FROM_ALL TRUE)
    endif()
endmacro()

macro(hgraph_fetch_spdlog)
    if(NOT DEFINED HGRAPH_SPDLOG_FETCH_VERSION)
        set(HGRAPH_SPDLOG_FETCH_VERSION 1.15.3)
    endif()
    set(spdlog_VERSION "${HGRAPH_SPDLOG_FETCH_VERSION}")
    if(NOT HGRAPH_FETCH_MISSING_DEPENDENCIES AND NOT HGRAPH_BUILD_PYTHON_BINDINGS)
        message(FATAL_ERROR "Missing spdlog; install its CMake package or enable HGRAPH_FETCH_MISSING_DEPENDENCIES")
    endif()
    FetchContent_Declare(
        spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG        v${HGRAPH_SPDLOG_FETCH_VERSION}
        GIT_SHALLOW    TRUE
        SYSTEM
    )
    set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "spdlog uses the project fmt" FORCE)
    set(SPDLOG_INSTALL OFF CACHE BOOL "Generate spdlog install target" FORCE)
    set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "Build spdlog examples" FORCE)
    FetchContent_MakeAvailable(spdlog)
    set_property(TARGET spdlog PROPERTY POSITION_INDEPENDENT_CODE ON)
endmacro()

macro(hgraph_fetch_simdjson)
    if(NOT DEFINED HGRAPH_SIMDJSON_FETCH_VERSION)
        set(HGRAPH_SIMDJSON_FETCH_VERSION 4.6.4)
    endif()
    set(simdjson_VERSION "${HGRAPH_SIMDJSON_FETCH_VERSION}")
    if(NOT HGRAPH_FETCH_MISSING_DEPENDENCIES AND NOT HGRAPH_BUILD_PYTHON_BINDINGS)
        message(FATAL_ERROR "Missing simdjson; install its CMake package or enable HGRAPH_FETCH_MISSING_DEPENDENCIES")
    endif()
    FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG        v${HGRAPH_SIMDJSON_FETCH_VERSION}
        GIT_SHALLOW    TRUE
        SYSTEM
    )
    set(SIMDJSON_INSTALL OFF CACHE BOOL "Generate simdjson install targets" FORCE)
    set(SIMDJSON_DEVELOPER_MODE OFF CACHE BOOL "Build simdjson developer targets" FORCE)
    FetchContent_MakeAvailable(simdjson)
    set_property(TARGET simdjson PROPERTY POSITION_INDEPENDENT_CODE ON)
endmacro()
