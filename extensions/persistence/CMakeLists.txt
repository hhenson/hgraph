cmake_minimum_required(VERSION 3.25)

project(hgraph_persistence VERSION 0.8.0 LANGUAGES C CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)
include(FetchContent)

option(HGRAPH_PERSISTENCE_BUILD_PYTHON "Build the optional Python bridge module" OFF)
option(HGRAPH_PERSISTENCE_WARNINGS_AS_ERRORS
    "Treat hgraph-persistence compiler warnings as errors"
    ${HGRAPH_WARNINGS_AS_ERRORS})
option(HGRAPH_PERSISTENCE_FETCH_CURL
    "Fetch the pinned curl release when a suitable package is unavailable"
    ON)
option(HGRAPH_PERSISTENCE_FORCE_FETCH_CURL
    "Use the pinned curl release even when a system package is available"
    OFF)

if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS
        Interpreter Development.Module Development.SABIModule REQUIRED)
endif()

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()

include(cmake/hgraph_persistence_arrow.cmake)
if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    hgraph_persistence_find_arrow(PREFER_PYARROW)
else()
    hgraph_persistence_find_arrow()
endif()

# Arrow deliberately exposes only ordinary S3 filesystem writes. The durable
# object-store contract needs real If-None-Match/If-Match requests, so its
# persistence-private S3 strategy uses libcurl's AWS Signature V4 support.
if(HGRAPH_PERSISTENCE_WITH_S3)
    set(HGRAPH_PERSISTENCE_CURL_TARGET "")
    if(TARGET libcurl_static)
        # A parent such as hgraph-web may already own a pinned curl build.
        set(HGRAPH_PERSISTENCE_CURL_TARGET libcurl_static)
    elseif(NOT HGRAPH_PERSISTENCE_FORCE_FETCH_CURL)
        find_package(CURL 7.75 QUIET)
        if(CURL_FOUND AND TARGET CURL::libcurl)
            set(HGRAPH_PERSISTENCE_CURL_TARGET CURL::libcurl)
        endif()
    endif()
    if(NOT HGRAPH_PERSISTENCE_CURL_TARGET)
        if(NOT HGRAPH_PERSISTENCE_FETCH_CURL)
            message(FATAL_ERROR
                "curl >= 7.75 was not found; install its CMake package or enable "
                "HGRAPH_PERSISTENCE_FETCH_CURL")
        endif()

        set(_hgraph_persistence_saved_build_shared_libs ${BUILD_SHARED_LIBS})
        set(_hgraph_persistence_saved_build_static_libs ${BUILD_STATIC_LIBS})
        set(_hgraph_persistence_saved_build_testing ${BUILD_TESTING})
        set(_hgraph_persistence_saved_pic ${CMAKE_POSITION_INDEPENDENT_CODE})
        set(BUILD_SHARED_LIBS OFF)
        set(BUILD_STATIC_LIBS ON)
        set(BUILD_TESTING OFF)
        set(CMAKE_POSITION_INDEPENDENT_CODE ON)

        set(BUILD_CURL_EXE OFF CACHE BOOL "" FORCE)
        set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
        # A static installed hgraph-persistence target still needs libcurl at
        # the final consumer link.  Install the pinned curl SDK beside it so a
        # wheel or ordinary SDK install is transitively complete; shared
        # persistence builds carry curl themselves and do not need the SDK.
        if(NOT _hgraph_persistence_saved_build_shared_libs)
            set(_hgraph_persistence_saved_install_component
                "${CMAKE_INSTALL_DEFAULT_COMPONENT_NAME}")
            set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME Development)
            set(CURL_ENABLE_EXPORT_TARGET ON CACHE BOOL "" FORCE)
            set(CURL_DISABLE_INSTALL OFF CACHE BOOL "" FORCE)
        else()
            set(CURL_ENABLE_EXPORT_TARGET OFF CACHE BOOL "" FORCE)
            set(CURL_DISABLE_INSTALL ON CACHE BOOL "" FORCE)
        endif()
        set(BUILD_LIBCURL_DOCS OFF CACHE BOOL "" FORCE)
        set(BUILD_MISC_DOCS OFF CACHE BOOL "" FORCE)
        set(ENABLE_CURL_MANUAL OFF CACHE BOOL "" FORCE)
        set(USE_NGHTTP2 OFF CACHE BOOL "" FORCE)
        set(CURL_USE_LIBPSL OFF CACHE BOOL "" FORCE)
        set(CURL_USE_LIBSSH2 OFF CACHE BOOL "" FORCE)
        set(CURL_BROTLI OFF CACHE BOOL "" FORCE)
        set(CURL_ZSTD OFF CACHE BOOL "" FORCE)
        set(CURL_ZLIB OFF CACHE BOOL "" FORCE)
        set(USE_LIBIDN2 OFF CACHE BOOL "" FORCE)
        set(CURL_USE_GSSAPI OFF CACHE BOOL "" FORCE)
        set(CURL_DISABLE_LDAP ON CACHE BOOL "" FORCE)
        if(WIN32)
            set(CURL_USE_OPENSSL OFF CACHE BOOL "" FORCE)
            set(CURL_USE_SCHANNEL ON CACHE BOOL "" FORCE)
            set(CURL_USE_SECTRANSP OFF CACHE BOOL "" FORCE)
        elseif(APPLE)
            set(CURL_USE_OPENSSL OFF CACHE BOOL "" FORCE)
            set(CURL_USE_SCHANNEL OFF CACHE BOOL "" FORCE)
            set(CURL_USE_SECTRANSP ON CACHE BOOL "" FORCE)
        else()
            find_package(OpenSSL REQUIRED)
            set(CURL_USE_OPENSSL ON CACHE BOOL "" FORCE)
            set(CURL_USE_SCHANNEL OFF CACHE BOOL "" FORCE)
            set(CURL_USE_SECTRANSP OFF CACHE BOOL "" FORCE)
        endif()
        FetchContent_Declare(hgraph_persistence_curl
            GIT_REPOSITORY https://github.com/curl/curl.git
            GIT_TAG curl-8_11_1
            GIT_SHALLOW TRUE
        )
        FetchContent_MakeAvailable(hgraph_persistence_curl)
        if(TARGET libcurl_static)
            set(HGRAPH_PERSISTENCE_CURL_TARGET libcurl_static)
        elseif(TARGET libcurl)
            set(HGRAPH_PERSISTENCE_CURL_TARGET libcurl)
        else()
            message(FATAL_ERROR "curl did not provide its static library target")
        endif()

        set(BUILD_SHARED_LIBS ${_hgraph_persistence_saved_build_shared_libs})
        set(BUILD_STATIC_LIBS ${_hgraph_persistence_saved_build_static_libs})
        set(BUILD_TESTING ${_hgraph_persistence_saved_build_testing})
        set(CMAKE_POSITION_INDEPENDENT_CODE ${_hgraph_persistence_saved_pic})
        if(DEFINED _hgraph_persistence_saved_install_component)
            set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME
                "${_hgraph_persistence_saved_install_component}")
            unset(_hgraph_persistence_saved_install_component)
        endif()
        set(BUILD_TESTING ${_hgraph_persistence_saved_build_testing}
            CACHE BOOL "Build the testing tree." FORCE)
        unset(_hgraph_persistence_saved_build_shared_libs)
        unset(_hgraph_persistence_saved_build_static_libs)
        unset(_hgraph_persistence_saved_build_testing)
        unset(_hgraph_persistence_saved_pic)
    endif()
endif()

if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    get_target_property(_hgraph_persistence_core_links hgraph::options INTERFACE_LINK_LIBRARIES)
    if("Python::Python" IN_LIST _hgraph_persistence_core_links)
        message(FATAL_ERROR
            "The selected hgraph SDK embeds a specific Python interpreter and "
            "cannot produce an ABI3 hgraph-persistence wheel; use the SDK "
            "installed by an hgraph stable-ABI wheel")
    endif()
endif()

add_library(hgraph_persistence
    src/frame_store.cpp
    src/object_store.cpp
    src/store_location.cpp
    src/impl/object_store_memory.cpp
    src/impl/object_store_local.cpp
    src/impl/object_store_s3.cpp
    src/impl/s3_options.cpp
    src/recording_store.cpp
    src/record_replay_frame_impl.cpp
    src/registration.cpp
)
add_library(hgraph::persistence ALIAS hgraph_persistence)

get_target_property(HGRAPH_PERSISTENCE_LIBRARY_TYPE hgraph_persistence TYPE)
if(HGRAPH_PERSISTENCE_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
    target_compile_definitions(hgraph_persistence PUBLIC HGRAPH_PERSISTENCE_STATIC_DEFINE)
endif()

target_compile_features(hgraph_persistence PUBLIC cxx_std_23)
target_link_libraries(hgraph_persistence PUBLIC hgraph::core)
if(TARGET Arrow::arrow_shared)
    target_link_libraries(hgraph_persistence PRIVATE Arrow::arrow_shared)
else()
    target_link_libraries(hgraph_persistence PRIVATE Arrow::arrow_static)
endif()
if(HGRAPH_PERSISTENCE_WITH_PARQUET)
    target_compile_definitions(hgraph_persistence PRIVATE HGRAPH_PERSISTENCE_WITH_PARQUET=1)
    if(TARGET Parquet::parquet_shared)
        target_link_libraries(hgraph_persistence PRIVATE Parquet::parquet_shared)
    elseif(TARGET Parquet::parquet_static)
        target_link_libraries(hgraph_persistence PRIVATE Parquet::parquet_static)
    endif()
endif()
if(HGRAPH_PERSISTENCE_WITH_S3)
    target_compile_definitions(hgraph_persistence PRIVATE
        HGRAPH_PERSISTENCE_WITH_S3=1
        HGRAPH_PERSISTENCE_WITH_CURL=1
    )
    # A shared extension carries curl. A static installed SDK consumer links
    # its own CURL::libcurl through hgraph-persistenceConfig.cmake.
    target_link_libraries(hgraph_persistence PRIVATE
        $<BUILD_INTERFACE:${HGRAPH_PERSISTENCE_CURL_TARGET}>)
endif()
target_include_directories(hgraph_persistence
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
set_target_properties(hgraph_persistence PROPERTIES
    EXPORT_NAME persistence
    POSITION_INDEPENDENT_CODE ON
)

if(MSVC)
    target_compile_options(hgraph_persistence PRIVATE /W4 /permissive-)
    if(HGRAPH_PERSISTENCE_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_persistence PRIVATE /WX)
    endif()
else()
    target_compile_options(hgraph_persistence PRIVATE -Wall -Wextra -Wpedantic)
    if(HGRAPH_PERSISTENCE_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_persistence PRIVATE -Werror)
    endif()
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(HGRAPH_PERSISTENCE_BUILD_PYTHON)
    if(NOT COMMAND hgraph_add_python_module OR NOT TARGET hgraph::nanobind)
        message(FATAL_ERROR
            "HGRAPH_PERSISTENCE_BUILD_PYTHON requires a Python-enabled installed hgraph SDK")
    endif()
    # The bridge's RUNPATH locates libhgraph_persistence, but ELF RUNPATH is
    # not inherited when that library resolves its own Arrow/Parquet and core
    # dependencies.  Give the shared implementation library an explicit
    # package-relative path as well; otherwise an isolated Linux wheel import
    # depends on some earlier module having loaded libparquet globally.
    if(BUILD_SHARED_LIBS)
        if(APPLE)
            set_target_properties(hgraph_persistence PROPERTIES
                INSTALL_RPATH "@loader_path;@loader_path/../pyarrow")
        elseif(UNIX)
            set_target_properties(hgraph_persistence PROPERTIES
                INSTALL_RPATH "$ORIGIN;$ORIGIN/../pyarrow")
        endif()
    endif()
    hgraph_add_python_module(_hgraph_persistence STABLE_ABI NOMINSIZE src/python_module.cpp)
    target_link_libraries(_hgraph_persistence PRIVATE hgraph::persistence)
    # One more `../` than hgraph_add_python_module's default: the module
    # installs into hgraph_persistence/, and both the core shared runtime
    # (site-packages lib/) and pyarrow's Arrow must resolve from there.
    if(APPLE)
        set_target_properties(_hgraph_persistence PROPERTIES
            INSTALL_RPATH "@loader_path/../${CMAKE_INSTALL_LIBDIR};@loader_path/../pyarrow")
    elseif(UNIX)
        set_target_properties(_hgraph_persistence PROPERTIES
            INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR};$ORIGIN/../pyarrow")
    endif()
    install(TARGETS _hgraph_persistence
        COMPONENT Python
        LIBRARY DESTINATION hgraph_persistence
        RUNTIME DESTINATION hgraph_persistence
    )
endif()

install(TARGETS hgraph_persistence
    EXPORT hgraphPersistenceTargets
    COMPONENT Development
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT Development FILES_MATCHING PATTERN "*.h")

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph-persistenceConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-persistence
)

install(EXPORT hgraphPersistenceTargets
    FILE hgraphPersistenceTargets.cmake
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-persistence
    COMPONENT Development
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraph-persistenceConfigVersion.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph_persistence_arrow.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-persistence
    COMPONENT Development
)
