cmake_minimum_required(VERSION 3.25)

project(hgraph_kafka VERSION 0.8.0 LANGUAGES C CXX)

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

    # A Python extension statically embeds librdkafka. Keep OpenSSL inside the
    # extension on wheel platforms where a package-local dynamic dependency
    # would otherwise require loader-path repair. In particular, Python 3.8+
    # does not search the Windows process PATH for dependent DLLs.
    if(HGRAPH_KAFKA_BUILD_PYTHON AND (APPLE OR WIN32))
        set(OPENSSL_USE_STATIC_LIBS TRUE)
    endif()

    # On macOS, prefer the static codec library as well so the resulting wheel
    # does not retain an absolute Homebrew dylib path. System libraries remain
    # normal platform dependencies.
    if(APPLE AND HGRAPH_KAFKA_BUILD_PYTHON)
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
    # The pinned release still declares compatibility with obsolete CMake
    # versions. Suppress that dependency-owned deprecation only while its
    # project is configured; hgraph-kafka deprecations remain visible.
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 4.0)
        set(_hgraph_kafka_had_policy_version_minimum FALSE)
        if(DEFINED CMAKE_POLICY_VERSION_MINIMUM)
            set(_hgraph_kafka_had_policy_version_minimum TRUE)
            set(_hgraph_kafka_saved_policy_version_minimum "${CMAKE_POLICY_VERSION_MINIMUM}")
        endif()
        if(NOT DEFINED CMAKE_POLICY_VERSION_MINIMUM OR
           CMAKE_POLICY_VERSION_MINIMUM VERSION_LESS 3.10)
            set(CMAKE_POLICY_VERSION_MINIMUM 3.10)
        endif()
        FetchContent_MakeAvailable(librdkafka)
        if(_hgraph_kafka_had_policy_version_minimum)
            set(CMAKE_POLICY_VERSION_MINIMUM "${_hgraph_kafka_saved_policy_version_minimum}")
        else()
            unset(CMAKE_POLICY_VERSION_MINIMUM)
        endif()
        unset(_hgraph_kafka_saved_policy_version_minimum)
        unset(_hgraph_kafka_had_policy_version_minimum)
    else()
        set(_hgraph_kafka_had_cmake_warn_deprecated FALSE)
        if(DEFINED CMAKE_WARN_DEPRECATED)
            set(_hgraph_kafka_had_cmake_warn_deprecated TRUE)
            set(_hgraph_kafka_saved_cmake_warn_deprecated "${CMAKE_WARN_DEPRECATED}")
        endif()
        set(CMAKE_WARN_DEPRECATED OFF)
        FetchContent_MakeAvailable(librdkafka)
        if(_hgraph_kafka_had_cmake_warn_deprecated)
            set(CMAKE_WARN_DEPRECATED "${_hgraph_kafka_saved_cmake_warn_deprecated}")
        else()
            unset(CMAKE_WARN_DEPRECATED)
        endif()
        unset(_hgraph_kafka_saved_cmake_warn_deprecated)
        unset(_hgraph_kafka_had_cmake_warn_deprecated)
    endif()

    if(MSVC)
        # Pinned librdkafka 2.15 has two MSVC-only C diagnostics in its own
        # sources: a va_list flow false positive and a Windows SSPI typedef
        # mismatch. Keep third-party warnings out of hgraph /WX builds without
        # weakening warning policy on hgraph targets.
        target_compile_options(rdkafka PRIVATE /wd4133 /wd4700)
    elseif(CMAKE_C_COMPILER_ID MATCHES "Clang")
        # Pinned librdkafka 2.15 triggers Clang diagnostics in its telemetry,
        # legacy OpenSSL engine, and platform-format code. Scope these to the
        # fetched third-party target so hgraph keeps its normal warning policy.
        target_compile_options(rdkafka PRIVATE
            -Wno-deprecated-declarations
            -Wno-format
            -Wno-implicit-enum-enum-cast
        )
    elseif(CMAKE_C_COMPILER_ID STREQUAL "GNU")
        # GCC diagnoses the same legacy OpenSSL engine calls plus const
        # conversions in librdkafka's C compatibility surface. Both belong to
        # the pinned dependency rather than hgraph targets.
        target_compile_options(rdkafka PRIVATE
            -Wno-deprecated-declarations
            -Wno-discarded-qualifiers
        )
    endif()

    if(HGRAPH_ENABLE_TSAN)
        if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
            # glibc's C11 thrd_create/mtx_* entry points are not intercepted by
            # ThreadSanitizer.  librdkafka worker threads created through that
            # backend therefore enter instrumented code without TSan thread
            # state and crash inside the sanitizer allocator.  Select its
            # pthread-backed implementation after feature detection has
            # generated config.h; pthread lifecycle and synchronization are
            # visible to TSan.
            set(_hgraph_kafka_rdkafka_config
                "${librdkafka_BINARY_DIR}/generated/config.h")
            file(READ "${_hgraph_kafka_rdkafka_config}"
                _hgraph_kafka_rdkafka_config_contents)
            if(_hgraph_kafka_rdkafka_config_contents MATCHES
                    "#define WITH_C11THREADS 1")
                string(REPLACE
                    "#define WITH_C11THREADS 1"
                    "#define WITH_C11THREADS 0"
                    _hgraph_kafka_rdkafka_config_contents
                    "${_hgraph_kafka_rdkafka_config_contents}")
                file(WRITE "${_hgraph_kafka_rdkafka_config}"
                    "${_hgraph_kafka_rdkafka_config_contents}")
            elseif(NOT _hgraph_kafka_rdkafka_config_contents MATCHES
                    "#define WITH_C11THREADS 0")
                message(FATAL_ERROR
                    "The pinned librdkafka config no longer declares "
                    "WITH_C11THREADS; update the TSan compatibility setup")
            endif()
            set(WITH_C11THREADS OFF CACHE INTERNAL
                "Use librdkafka's pthread-backed thread primitives under TSan"
                FORCE)
        endif()

        # Instrument the fetched dependency as well as hgraph.  In addition to
        # checking its accesses, this prevents interceptor-only reports from
        # librdkafka allocation helpers whose surrounding atomic operations
        # would otherwise be invisible to TSan.
        target_compile_options(rdkafka PRIVATE
            -fsanitize=thread
            -fno-omit-frame-pointer
        )
    endif()

    if(MSVC AND OPENSSL_USE_STATIC_LIBS)
        # librdkafka's Windows TLS source embeds default-library directives for
        # the OpenSSL import libraries.  Ignore those names when CMake selected
        # the explicitly linked *_static.lib archives for a self-contained
        # consumer; otherwise MSVC still searches for the absent import libs.
        target_link_options(rdkafka INTERFACE
            /NODEFAULTLIB:libcrypto.lib
            /NODEFAULTLIB:libssl.lib
        )
    endif()
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
    # librdkafka includes Windows headers in the implementation.  Keep their
    # min/max macros from rewriting standard-library calls in this target.
    target_compile_definitions(hgraph_kafka PRIVATE NOMINMAX)
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
