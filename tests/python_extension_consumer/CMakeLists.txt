cmake_minimum_required(VERSION 3.26)

project(hgraph_python_extension_consumer LANGUAGES CXX)

find_package(Python 3.12 COMPONENTS Interpreter Development.Module Development.SABIModule REQUIRED)

execute_process(
    COMMAND "${Python_EXECUTABLE}" -c
            "import sysconfig; print(sysconfig.get_path('purelib'))"
    OUTPUT_VARIABLE HGRAPH_PYTHON_CONSUMER_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE HGRAPH_PYTHON_CONSUMER_PREFIX_RESULT
)
if(NOT HGRAPH_PYTHON_CONSUMER_PREFIX_RESULT EQUAL 0 OR HGRAPH_PYTHON_CONSUMER_PREFIX STREQUAL "")
    message(FATAL_ERROR "Could not determine Python's site-packages prefix")
endif()
list(APPEND CMAKE_PREFIX_PATH "${HGRAPH_PYTHON_CONSUMER_PREFIX}")
file(GLOB HGRAPH_PYTHON_CONSUMER_CONFIG_HINTS
    LIST_DIRECTORIES true
    "${HGRAPH_PYTHON_CONSUMER_PREFIX}/lib*/cmake/hgraph"
)

find_package(hgraph 0.4 CONFIG REQUIRED
    HINTS ${HGRAPH_PYTHON_CONSUMER_CONFIG_HINTS}
)
if(NOT COMMAND hgraph_add_python_module)
    message(FATAL_ERROR "The installed hgraph SDK does not provide hgraph_add_python_module")
endif()

hgraph_add_python_module(_hgraph_consumer STABLE_ABI NOMINSIZE NOSTRIP module.cpp)
target_link_libraries(_hgraph_consumer PRIVATE hgraph::core)
