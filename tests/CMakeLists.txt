add_subdirectory(cpp)

# Exercise SDK dependency discovery without fetching or compiling dependencies.
foreach(_arrow_kind IN ITEMS shared static)
    foreach(_arrow_preload IN ITEMS none core core_compute core_acero all)
        add_test(NAME hgraph.arrow_dependencies.${_arrow_kind}.${_arrow_preload}
            COMMAND "${CMAKE_COMMAND}"
                -S "${CMAKE_CURRENT_SOURCE_DIR}/cmake/arrow_dependencies"
                -B "${CMAKE_CURRENT_BINARY_DIR}/arrow_dependencies/${_arrow_kind}-${_arrow_preload}"
                "-DHGRAPH_CMAKE_DIR=${PROJECT_SOURCE_DIR}/cmake"
                "-DARROW_KIND=${_arrow_kind}"
                "-DPRELOAD=${_arrow_preload}")
    endforeach()
endforeach()
if(HGRAPH_USE_PYARROW_ARROW)
    foreach(_arrow_preload IN ITEMS none core core_compute core_acero all)
        add_test(NAME hgraph.arrow_dependencies.pyarrow.${_arrow_preload}
            COMMAND "${CMAKE_COMMAND}"
                -S "${CMAKE_CURRENT_SOURCE_DIR}/cmake/arrow_dependencies"
                -B "${CMAKE_CURRENT_BINARY_DIR}/arrow_dependencies/pyarrow-${_arrow_preload}"
                "-DHGRAPH_CMAKE_DIR=${PROJECT_SOURCE_DIR}/cmake"
                -DARROW_KIND=shared
                "-DPRELOAD=${_arrow_preload}"
                -DHGRAPH_USE_PYARROW_ARROW=ON
                "-DPython_EXECUTABLE=${Python_EXECUTABLE}")
    endforeach()
endif()

# The Python-facing tests live in python/tests and are registered by
# python/CMakeLists.txt (the stub-era tests/python pytest suite targeted the
# removed experimental module).
