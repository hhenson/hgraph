cmake_minimum_required(VERSION 3.25)

project(hgraph_web VERSION 0.8.0 LANGUAGES C CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)
include(FetchContent)

option(HGRAPH_WEB_FETCH_NGHTTP2
    "Fetch the pinned nghttp2 release when a CMake package is unavailable"
    ON)
option(HGRAPH_WEB_FETCH_CURL
    "Fetch the pinned curl release when a CMake package is unavailable"
    ON)
option(HGRAPH_WEB_FETCH_BOOST
    "Fetch the pinned Boost release when a CMake package is unavailable"
    ON)
option(HGRAPH_WEB_FORCE_FETCH_DEPENDENCIES
    "Use the pinned third-party releases even when system packages are available"
    OFF)
option(HGRAPH_WEB_BUILD_PYTHON "Build the optional Python authoring bridge" OFF)

# Load only the limited-API Python targets before the installed hgraph SDK.
# Linking an embedding target such as Python::Python would pin this ABI3
# extension to the interpreter used for the build.
if(HGRAPH_WEB_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS
        Interpreter Development.Module Development.SABIModule REQUIRED)
endif()

if(NOT TARGET hgraph::core)
    find_package(hgraph CONFIG REQUIRED)
endif()

if(HGRAPH_WEB_BUILD_PYTHON)
    get_target_property(_hgraph_web_core_links hgraph::options INTERFACE_LINK_LIBRARIES)
    if("Python::Python" IN_LIST _hgraph_web_core_links)
        message(FATAL_ERROR
            "The selected hgraph SDK embeds a specific Python interpreter and "
            "cannot produce an ABI3 hgraph-web wheel; use the SDK installed "
            "by an hgraph stable-ABI wheel")
    endif()
endif()

# ---------------------------------------------------------------------------
# Third-party transport stack (RFC 0024, packaging): OpenSSL everywhere (one
# TLS surface, including Windows), then nghttp2 -> curl -> Boost in that
# order.  curl links the same nghttp2 it uses for client HTTP/2, so the later
# server HTTP/2 activation adds only a translation unit, not a dependency.
# Everything fetched is built static and stays PRIVATE to this extension.

# A Python extension statically embeds its TLS stack. Keep OpenSSL inside the
# extension on wheel platforms where a package-local dynamic dependency would
# otherwise require loader-path repair. In particular, Python 3.8+ does not
# search the Windows process PATH for dependent DLLs.
if(HGRAPH_WEB_BUILD_PYTHON AND (APPLE OR WIN32))
    set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()
find_package(OpenSSL REQUIRED)

# Keep the fetched transport libraries static without disturbing how the
# extension itself (or any sibling target) honors BUILD_SHARED_LIBS, and keep
# their test suites out of this build.
set(_hgraph_web_saved_build_shared_libs ${BUILD_SHARED_LIBS})
set(_hgraph_web_saved_build_testing ${BUILD_TESTING})
set(BUILD_SHARED_LIBS OFF)
set(BUILD_STATIC_LIBS ON)
set(BUILD_TESTING OFF)

# Sibling dependencies (Arrow, the AWS SDK) import system curl and friends at
# the root scope, so a bare TARGET check would adopt whatever they found.
# Each dependency is adopted only when this extension's own versioned find
# succeeded; otherwise the pinned release is fetched.
set(HGRAPH_WEB_NGHTTP2_TARGET "")
if(NOT HGRAPH_WEB_FORCE_FETCH_DEPENDENCIES)
    find_package(nghttp2 CONFIG QUIET)
    if(nghttp2_FOUND AND TARGET nghttp2::nghttp2)
        set(HGRAPH_WEB_NGHTTP2_TARGET nghttp2::nghttp2)
    endif()
endif()
if(NOT HGRAPH_WEB_NGHTTP2_TARGET)
    if(NOT HGRAPH_WEB_FETCH_NGHTTP2)
        message(FATAL_ERROR
            "nghttp2 was not found; install its CMake package or enable "
            "HGRAPH_WEB_FETCH_NGHTTP2")
    endif()
    set(ENABLE_LIB_ONLY ON CACHE BOOL "" FORCE)
    set(ENABLE_DOC OFF CACHE BOOL "" FORCE)
    set(ENABLE_SHARED_LIB OFF CACHE BOOL "" FORCE)
    set(ENABLE_STATIC_LIB ON CACHE BOOL "" FORCE)
    FetchContent_Declare(nghttp2
        GIT_REPOSITORY https://github.com/nghttp2/nghttp2.git
        GIT_TAG v1.64.0
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(nghttp2)
    if(TARGET nghttp2_static)
        set(HGRAPH_WEB_NGHTTP2_TARGET nghttp2_static)
    elseif(TARGET nghttp2)
        set(HGRAPH_WEB_NGHTTP2_TARGET nghttp2)
    else()
        message(FATAL_ERROR "nghttp2 did not provide its library target")
    endif()
    # Point curl's nghttp2 discovery at the fetched tree: the version header
    # is generated into the binary include directory at configure time.
    set(NGHTTP2_INCLUDE_DIR
        "${nghttp2_SOURCE_DIR}/lib/includes;${nghttp2_BINARY_DIR}/lib/includes"
        CACHE STRING "" FORCE)
    set(NGHTTP2_LIBRARY ${HGRAPH_WEB_NGHTTP2_TARGET} CACHE STRING "" FORCE)
endif()

set(HGRAPH_WEB_CURL_TARGET "")
if(NOT HGRAPH_WEB_FORCE_FETCH_DEPENDENCIES)
    # RFC 0024 pins curl >= 8.11: the WebSocket client API left its
    # experimental phase there.
    find_package(CURL 8.11 CONFIG QUIET)
    if(CURL_FOUND AND TARGET CURL::libcurl)
        set(HGRAPH_WEB_CURL_TARGET CURL::libcurl)
    endif()
endif()
if(NOT HGRAPH_WEB_CURL_TARGET)
    if(NOT HGRAPH_WEB_FETCH_CURL)
        message(FATAL_ERROR
            "curl >= 8.11 was not found; install its CMake package or enable "
            "HGRAPH_WEB_FETCH_CURL")
    endif()
    set(BUILD_CURL_EXE OFF CACHE BOOL "" FORCE)
    set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    # The fetched curl links the in-tree nghttp2 target, which cannot appear
    # in curl's own CMake export set; the extension never consumes curl via
    # its export anyway.  Installation must be off entirely: a partial
    # CURLConfig.cmake landing in the hgraph SDK prefix poisons every later
    # find_package(CURL) in that prefix.
    set(CURL_ENABLE_EXPORT_TARGET OFF CACHE BOOL "" FORCE)
    set(CURL_DISABLE_INSTALL ON CACHE BOOL "" FORCE)
    set(BUILD_LIBCURL_DOCS OFF CACHE BOOL "" FORCE)
    set(BUILD_MISC_DOCS OFF CACHE BOOL "" FORCE)
    set(ENABLE_CURL_MANUAL OFF CACHE BOOL "" FORCE)
    # One TLS surface on every platform, including Windows (RFC 0024).
    set(CURL_USE_OPENSSL ON CACHE BOOL "" FORCE)
    set(CURL_USE_SCHANNEL OFF CACHE BOOL "" FORCE)
    set(CURL_USE_SECTRANSP OFF CACHE BOOL "" FORCE)
    set(USE_NGHTTP2 ON CACHE BOOL "" FORCE)
    # The transport needs no auxiliary curl features; keeping them off keeps
    # the wheel dependency-free beyond OpenSSL.
    set(CURL_USE_LIBPSL OFF CACHE BOOL "" FORCE)
    set(CURL_USE_LIBSSH2 OFF CACHE BOOL "" FORCE)
    set(CURL_BROTLI OFF CACHE BOOL "" FORCE)
    set(CURL_ZSTD OFF CACHE BOOL "" FORCE)
    set(USE_LIBIDN2 OFF CACHE BOOL "" FORCE)
    set(CURL_USE_GSSAPI OFF CACHE BOOL "" FORCE)
    set(CURL_DISABLE_LDAP ON CACHE BOOL "" FORCE)
    FetchContent_Declare(curl
        GIT_REPOSITORY https://github.com/curl/curl.git
        GIT_TAG curl-8_11_1
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(curl)
    # Adopt the concrete static target: the CURL::libcurl alias name may
    # already be taken by a sibling's system-curl import.
    if(TARGET libcurl_static)
        set(HGRAPH_WEB_CURL_TARGET libcurl_static)
    elseif(TARGET libcurl)
        set(HGRAPH_WEB_CURL_TARGET libcurl)
    else()
        message(FATAL_ERROR "curl did not provide its static library target")
    endif()
endif()

set(HGRAPH_WEB_BOOST_TARGETS "")
if(NOT HGRAPH_WEB_FORCE_FETCH_DEPENDENCIES)
    find_package(Boost 1.85 CONFIG QUIET COMPONENTS system)
    if(Boost_FOUND AND TARGET Boost::system)
        set(HGRAPH_WEB_BOOST_TARGETS Boost::system)
        if(TARGET Boost::beast)
            list(APPEND HGRAPH_WEB_BOOST_TARGETS Boost::beast Boost::asio)
        else()
            list(APPEND HGRAPH_WEB_BOOST_TARGETS Boost::headers)
        endif()
    endif()
endif()
if(NOT HGRAPH_WEB_BOOST_TARGETS)
    if(NOT HGRAPH_WEB_FETCH_BOOST)
        message(FATAL_ERROR
            "Boost >= 1.85 was not found; install its CMake package or enable "
            "HGRAPH_WEB_FETCH_BOOST")
    endif()
    # The CMake-enabled release archive supports building only the needed
    # subset; Beast and Asio are header-only, so this costs extraction, not
    # compilation.
    set(BOOST_INCLUDE_LIBRARIES beast asio system)
    set(BOOST_ENABLE_CMAKE ON)
    # Keep the fetched Boost out of the install tree; it is a build-time
    # detail of this extension, never part of the hgraph SDK.
    set(BOOST_SKIP_INSTALL_RULES ON)
    FetchContent_Declare(boost
        URL https://github.com/boostorg/boost/releases/download/boost-1.87.0/boost-1.87.0-cmake.tar.xz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(boost)
    if(NOT TARGET Boost::beast)
        message(FATAL_ERROR "Boost did not provide its Beast target")
    endif()
    set(HGRAPH_WEB_BOOST_TARGETS Boost::beast Boost::asio Boost::system)
endif()

set(BUILD_SHARED_LIBS ${_hgraph_web_saved_build_shared_libs})
set(BUILD_TESTING ${_hgraph_web_saved_build_testing})
# A fetched dependency rewrites the shared BUILD_TESTING cache entry during
# its configure; restore the superbuild's value so a reconfigure does not
# silently drop every test target in the tree.
set(BUILD_TESTING ${_hgraph_web_saved_build_testing}
    CACHE BOOL "Build the testing tree." FORCE)
unset(_hgraph_web_saved_build_shared_libs)
unset(_hgraph_web_saved_build_testing)

add_library(hgraph_web
    src/asio_server.cpp
    src/curl_client.cpp
    src/detail/route_table.cpp
    src/service.cpp
    src/value_builders.cpp
    src/testing/fake_transport.cpp
)
add_library(hgraph::web ALIAS hgraph_web)

get_target_property(HGRAPH_WEB_LIBRARY_TYPE hgraph_web TYPE)
if(HGRAPH_WEB_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
    target_compile_definitions(hgraph_web PUBLIC HGRAPH_WEB_STATIC_DEFINE)
endif()

target_compile_features(hgraph_web PUBLIC cxx_std_23)
# The transport targets join this link line with their translation units:
# ${HGRAPH_WEB_BOOST_TARGETS} with asio_server.cpp, ${HGRAPH_WEB_CURL_TARGET}
# with curl_client.cpp, and ${HGRAPH_WEB_NGHTTP2_TARGET} with the HTTP/2
# session (all PRIVATE; RFC 0024).
target_link_libraries(hgraph_web
    PUBLIC hgraph::core
    # The fetched transport libraries are build-tree targets outside this
    # project's export set, so they must not leak into the installed
    # interface.  Beast/Asio are header-only (compiled into these objects); a
    # SHARED hgraph_web links curl/nghttp2/OpenSSL in.  A STATIC install
    # instead needs system curl and OpenSSL at the consumer's final link —
    # the package config records that (the CI SDK consumer uses the shared
    # leg; wheels are always shared).
    PRIVATE $<BUILD_INTERFACE:${HGRAPH_WEB_BOOST_TARGETS}>
            $<BUILD_INTERFACE:${HGRAPH_WEB_CURL_TARGET}>
            OpenSSL::SSL OpenSSL::Crypto
)
target_include_directories(hgraph_web
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
set_target_properties(hgraph_web PROPERTIES
    EXPORT_NAME web
    POSITION_INDEPENDENT_CODE ON
)

if(MSVC)
    # The transport implementation includes Windows headers.  Keep their
    # min/max macros from rewriting standard-library calls in this target.
    # Beast's template instantiation count in the server TU exceeds the
    # default COFF section limit, hence /bigobj.
    target_compile_definitions(hgraph_web PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
    target_compile_options(hgraph_web PRIVATE /W4 /permissive- /bigobj)
else()
    target_compile_options(hgraph_web PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(MSVC AND OPENSSL_USE_STATIC_LIBS)
    # Windows TLS objects embed default-library directives for the OpenSSL
    # import libraries.  Ignore those names when CMake selected the statically
    # linked archives for a self-contained consumer; otherwise MSVC still
    # searches for the absent import libs.
    target_link_options(hgraph_web INTERFACE
        /NODEFAULTLIB:libcrypto.lib
        /NODEFAULTLIB:libssl.lib
    )
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(HGRAPH_WEB_BUILD_PYTHON)
    find_package(Python 3.12 COMPONENTS Interpreter Development.Module Development.SABIModule REQUIRED)
    if(NOT COMMAND hgraph_add_python_module OR NOT TARGET hgraph::nanobind)
        message(FATAL_ERROR
            "HGRAPH_WEB_BUILD_PYTHON requires a Python-enabled installed hgraph SDK")
    endif()
    hgraph_add_python_module(_hgraph_web STABLE_ABI NOMINSIZE src/python_module.cpp)
    target_link_libraries(_hgraph_web PRIVATE hgraph::web)
    if(APPLE)
        set_target_properties(_hgraph_web PROPERTIES
            INSTALL_RPATH "@loader_path/../${CMAKE_INSTALL_LIBDIR}")
    elseif(UNIX)
        set_target_properties(_hgraph_web PROPERTIES
            INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
    endif()
    install(TARGETS _hgraph_web
        COMPONENT Python
        LIBRARY DESTINATION hgraph_web
        RUNTIME DESTINATION hgraph_web
    )
endif()

install(TARGETS hgraph_web
    EXPORT hgraphWebTargets
    COMPONENT Development
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT Development FILES_MATCHING PATTERN "*.h")

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraph-webConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/hgraph-webConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/hgraph-webConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-web
)

install(EXPORT hgraphWebTargets
    FILE hgraphWebTargets.cmake
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-web
    COMPONENT Development
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraph-webConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraph-webConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph-web
    COMPONENT Development
)
