# Conan preamble for CMake: ensure Python/nanobind are discoverable BEFORE including the Conan provider
# Usage:
#   -DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=${CMAKE_SOURCE_DIR}/conan_preamble.cmake
# This guarantees any find_package(nanobind) inside the Conan provider happens
# after Python is located.

# Accept either Python_EXECUTABLE or Python3_EXECUTABLE from the caller and normalize
if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
endif()

# Find the desired Python (Interpreter + Development.Module so Python::Module is available)
find_package(Python 3.12 COMPONENTS Interpreter Development.Module REQUIRED)

# Propagate the interpreter to caches so downstream packages use the same one
set(Python_EXECUTABLE "${Python_EXECUTABLE}" CACHE FILEPATH "Selected Python interpreter" FORCE)
set(Python3_EXECUTABLE "${Python_EXECUTABLE}" CACHE FILEPATH "Selected Python3 interpreter" FORCE)

# Pre-resolve nanobind CMake package directory using the chosen interpreter, and make it visible
execute_process(
    COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE _NB_DIR
)
if(_NB_DIR)
    # Help CMake locate nanobind before the Conan provider tries to find it
    set(nanobind_DIR "${_NB_DIR}" CACHE PATH "nanobind CMake package dir" FORCE)
    if(DEFINED CMAKE_PREFIX_PATH)
        list(PREPEND CMAKE_PREFIX_PATH "${_NB_DIR}")
    else()
        set(CMAKE_PREFIX_PATH "${_NB_DIR}")
    endif()
endif()

# Include the Conan provider now that Python/nanobind are set up
include(${CMAKE_SOURCE_DIR}/conan_provider.cmake)
