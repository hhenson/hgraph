cmake_minimum_required(VERSION 3.25)

project(hgraph_kafka VERSION 0.1.0 LANGUAGES C CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)
include(FetchContent)

option(HGRAPH_KAFKA_FETCH_LIBRDKAFKA
    "Fetch the pinned librdkafka release when a CMake package is unavailable"
    ON)
option(HGRAPH_KAFKA_FORCE_FETCH_LIBRDKAFKA
    "Use the pinned librdkafka release even when a system package is available"
    OFF)
option(HGRAPH_KAFKA_BUILD_PYTHON "Build the optional Python authoring bridge" OFF)

# Load only the limited-API Python targets before the installed hgraph SDK.
# Linking an embedding target such as Python::Python would pin this ABI3
# extension to the interpreter used for the build.
if(HGRAPH_KAFKA_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS
        Interpreter Development.Module Development.SABIModule REQUIRED)
endif()

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()

if(HGRAPH_KAFKA_BUILD_PYTHON)
    get_target_property(_hgraph_kafka_core_links hgraph::options INTERFACE_LINK_LIBRARIES)
    if("Python::Python" IN_LIST _hgraph_kafka_core_links)
        message(FATAL_ERROR
            "The selected hgraph SDK embeds a specific Python interpreter and "
            "cannot produce an ABI3 hgraph-kafka wheel; use the SDK installed "
            "by an hgraph stable-ABI wheel")
    endif()
endif()

if(NOT HGRAPH_KAFKA_FORCE_FETCH_LIBRDKAFKA)
    find_package(RdKafka 2.15 CONFIG QUIET)
endif()
if(NOT TARGET RdKafka::rdkafka AND NOT TARGET rdkafka)
    if(NOT HGRAPH_KAFKA_FETCH_LIBRDKAFKA)
        message(FATAL_ERROR
            "librdkafka >= 2.15 was not found; install RdKafka or enable "
            "HGRAPH_KAFKA_FETCH_LIBRDKAFKA")
    endif()

    set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "" FORCE)
    set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(RDKAFKA_BUILD_TESTS OFF CACHE BOOL "" FORCE)

    # A Python extension statically embeds librdkafka. On macOS, prefer the
    # static codec/TLS libraries as well so the resulting wheel does not retain
    # absolute Homebrew dylib paths. System libraries (curl, zlib, SASL) remain
    # normal platform dependencies.
    if(APPLE AND HGRAPH_KAFKA_BUILD_PYTHON)
        set(OPENSSL_USE_STATIC_LIBS TRUE)
        set(ENABLE_LZ4_EXT OFF CACHE BOOL "" FORCE)
        find_library(HGRAPH_KAFKA_ZSTD_STATIC_LIBRARY NAMES libzstd.a)
        if(NOT HGRAPH_KAFKA_ZSTD_STATIC_LIBRARY)
            message(FATAL_ERROR
                "A static zstd library is required to build a portable macOS wheel")
        endif()
        set(ZSTD_LIBRARY_DEBUG "${HGRAPH_KAFKA_ZSTD_STATIC_LIBRARY}"
            CACHE FILEPATH "" FORCE)
        set(ZSTD_LIBRARY_RELEASE "${HGRAPH_KAFKA_ZSTD_STATIC_LIBRARY}"
            CACHE FILEPATH "" FORCE)
    endif()

    FetchContent_Declare(librdkafka
        GIT_REPOSITORY https://github.com/confluentinc/librdkafka.git
        GIT_TAG v2.15.0
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(librdkafka)
endif()

if(TARGET RdKafka::rdkafka)
    set(HGRAPH_KAFKA_RDKAFKA_TARGET RdKafka::rdkafka)
elseif(TARGET rdkafka)
    add_library(RdKafka::rdkafka ALIAS rdkafka)
    set(HGRAPH_KAFKA_RDKAFKA_TARGET RdKafka::rdkafka)
else()
    message(FATAL_ERROR "librdkafka did not provide its C target")
endif()

add_library(hgraph_kafka
    src/librdkafka_service.cpp
    src/service.cpp
    src/value_builders.cpp
    src/testing/fake_broker.cpp
    src/testing/mock_cluster.cpp
)
add_library(hgraph::kafka ALIAS hgraph_kafka)

get_target_property(HGRAPH_KAFKA_LIBRARY_TYPE hgraph_kafka TYPE)
if(HGRAPH_KAFKA_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
    target_compile_definitions(hgraph_kafka PUBLIC HGRAPH_KAFKA_STATIC_DEFINE)
endif()

target_compile_features(hgraph_kafka PUBLIC cxx_std_23)
target_link_libraries(hgraph_kafka
    PUBLIC hgraph::core
    PRIVATE ${HGRAPH_KAFKA_RDKAFKA_TARGET}
)
target_include_directories(hgraph_kafka
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
set_target_properties(hgraph_kafka PROPERTIES
    EXPORT_NAME kafka
    POSITION_INDEPENDENT_CODE ON
)

if(MSVC)
    target_compile_options(hgraph_kafka PRIVATE /W4 /permissive-)
else()
    target_compile_options(hgraph_kafka PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(HGRAPH_KAFKA_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS Interpreter Development.Module Development.SABIModule REQUIRED)
    if(NOT COMMAND hgraph_add_python_module OR NOT TARGET hgraph::nanobind)
        message(FATAL_ERROR
            "HGRAPH_KAFKA_BUILD_PYTHON requires a Python-enabled installed hgraph SDK")
    endif()
    hgraph_add_python_module(_hgraph_kafka STABLE_ABI NOMINSIZE src/python_module.cpp)
    target_link_libraries(_hgraph_kafka PRIVATE hgraph::kafka)
    if(APPLE)
        set_target_properties(_hgraph_kafka PROPERTIES
            INSTALL_RPATH "@loader_path/../${CMAKE_INSTALL_LIBDIR}")
    elseif(UNIX)
        set_target_properties(_hgraph_kafka PROPERTIES
            INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
    endif()
    install(TARGETS _hgraph_kafka
        COMPONENT Python
        LIBRARY DESTINATION hgraph_kafka
        RUNTIME DESTINATION hgraph_kafka
    )
endif()

install(TARGETS hgraph_kafka
    EXPORT hgraphKafkaTargets
    COMPONENT Development
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT Development FILES_MATCHING PATTERN "*.h")

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraph-kafkaConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph-kafkaConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/hgraph-kafkaConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-kafka
)

install(EXPORT hgraphKafkaTargets
    FILE hgraphKafkaTargets.cmake
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-kafka
    COMPONENT Development
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraph-kafkaConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraph-kafkaConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-kafka
    COMPONENT Development
)
