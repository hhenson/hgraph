project(_hgraph)

set(HGRAPH_SOURCES
        api/python/py_value.cpp
        api/python/py_ts_type_registry.cpp

        python/_hgraph_module.cpp
        python/_hgraph_nodes.cpp
        python/_hgraph_types.cpp
        python/global_state.cpp
        python/global_keys.cpp

        types/constants.cpp
        types/traits.cpp
        types/value/type_registry.cpp
        types/value/type_meta_bindings.cpp
        types/time_series/value/atomic.cpp
        types/time_series/value/associative.cpp
        types/time_series/value/list.cpp
        types/time_series/value/record.cpp
        types/time_series/value/sequence.cpp
        types/time_series/value/builder.cpp
        types/time_series/value/value.cpp
        types/time_series/ts_input.cpp
        types/time_series/ts_input_view.cpp
        types/time_series/ts_output.cpp
        types/time_series/ts_output_builder.cpp
        types/time_series/ts_input_builder.cpp
        types/time_series/ts_value_builder.cpp
        types/time_series/ts_value.cpp
        types/time_series/time_series_state.cpp
        types/time_series/active_trie.cpp
        types/time_series/ts_type_registry.cpp
        types/v2/boundary_binding.cpp
        types/v2/child_graph.cpp
        types/v2/evaluation_engine.cpp
        types/v2/graph.cpp
        types/v2/graph_builder.cpp
        types/v2/nested_clock.cpp
        types/v2/node.cpp
        types/v2/node_builder.cpp
        types/v2/python_node_support.cpp
        types/v2/ref.cpp

        util/lifecycle.cpp
        util/stack_trace.cpp
        util/string_util.cpp
)

add_definitions(-Dhgraph_EXPORTS)

nanobind_add_module(${PROJECT_NAME} STABLE_ABI ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

# numpy C API — required for zero-copy array views (PyArray_SimpleNewFromData).
if (Python_NumPy_INCLUDE_DIRS)
    target_include_directories(${PROJECT_NAME} PRIVATE ${Python_NumPy_INCLUDE_DIRS})
else ()
    execute_process(
        COMMAND "${Python_EXECUTABLE}" -c "import numpy; print(numpy.get_include())"
        OUTPUT_VARIABLE _NUMPY_INC OUTPUT_STRIP_TRAILING_WHITESPACE RESULT_VARIABLE _NUMPY_RC)
    if (_NUMPY_RC EQUAL 0 AND EXISTS "${_NUMPY_INC}")
        target_include_directories(${PROJECT_NAME} PRIVATE "${_NUMPY_INC}")
    else ()
        message(FATAL_ERROR "numpy headers not found — numpy>=1.23 is required (pyproject.toml [build-system].requires)")
    endif ()
endif ()

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
target_link_libraries(${PROJECT_NAME} PUBLIC ${HGRAPH_FMT_TARGET} ddvisitor dynamic_bitset ${HGRAPH_NANOARROW_TARGET})

# Propagate HGRAPH_WITH_BACKWARD to the compiler for conditional compilation
if (HGRAPH_WITH_BACKWARD)
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_WITH_BACKWARD=1)
else ()
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_WITH_BACKWARD=0)
endif ()

# Suppress benign fmt warning on GCC/Clang for this target only (avoid touching ALIAS/IMPORTED fmt targets)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${PROJECT_NAME} PRIVATE -Wno-tautological-compare)
endif ()

# Link Backward only if enabled and available
if (HGRAPH_WITH_BACKWARD AND TARGET Backward::Backward)
    target_link_libraries(${PROJECT_NAME} PRIVATE Backward::Backward)
endif ()
#target_compile_definitions(${PROJECT_NAME} PRIVATE BACKWARD_HAS_BACKTRACE=1)

# Windows: link required debug helper libraries for backward-cpp stack traces
if (WIN32)
    if (HGRAPH_WITH_BACKWARD)
        target_link_libraries(${PROJECT_NAME} PRIVATE Dbghelp Imagehlp)
    endif ()
endif ()

# Install directive for scikit-build-core
# Allow customization of Python module name and install subdirectory
set(HGRAPH_PYMOD_NAME "_hgraph" CACHE STRING "Python extension module name")
set(HGRAPH_PYMOD_INSTALL_SUBDIR "hgraph" CACHE STRING "Install subdir under site-packages")
set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "${HGRAPH_PYMOD_NAME}")
# Install into the package directory (so wheel ends up with hgraph/_hgraph.*)
# Cover Windows by also specifying RUNTIME destination for .pyd files
# (Diagnostics removed to keep logs clean)
install(TARGETS ${PROJECT_NAME}
        LIBRARY DESTINATION ${HGRAPH_PYMOD_INSTALL_SUBDIR}
        RUNTIME DESTINATION ${HGRAPH_PYMOD_INSTALL_SUBDIR}
)

option(HGRAPH_GENERATE_STUBS "Generate nanobind .pyi stubs for the Python module" OFF)
if (HGRAPH_GENERATE_STUBS)
    nanobind_add_stub(
            "${PROJECT_NAME}_stub"
            MODULE ${PROJECT_NAME}
            OUTPUT "${PROJECT_NAME}.pyi"
            PYTHON_PATH $<TARGET_FILE_DIR:${PROJECT_NAME}>
            DEPENDS ${PROJECT_NAME}
    )
endif ()
