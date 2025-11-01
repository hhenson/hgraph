# Conan preamble for CMake: ensure Python/nanobind are discoverable BEFORE including the Conan provider
# Usage:
#   Pass this file via -DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=/path/to/cmake/conan_preamble.cmake
#   so that any find_package(nanobind) calls inside the Conan provider happen after Python is found.

# Accept either Python_EXECUTABLE or Python3_EXECUTABLE from the caller
if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
endif()

# Find the desired Python (with Development.Module to ensure Python::Module is available)
find_package(Python 3.12 COMPONENTS Interpreter Development.Module REQUIRED)

# Propagate the interpreter to child scopes so downstream packages use the same one
set(Python_EXECUTABLE "${Python_EXECUTABLE}" CACHE FILEPATH "Selected Python interpreter" FORCE)
set(Python3_EXECUTABLE "${Python_EXECUTABLE}" CACHE FILEPATH "Selected Python3 interpreter" FORCE)

# Finally include the Conan provider so it can call find_package(nanobind) using the resolved interpreter
include(${CMAKE_SOURCE_DIR}/conan_provider.cmake)
