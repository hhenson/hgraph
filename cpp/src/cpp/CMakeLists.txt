project(_hgraph)

# API Version Selection (1 = V1, 2 = V2)
# V1: Current implementation with BaseTimeSeriesInput/Output inheritance
# V2: New unified time-series with TSValue shared state (planned)
option(HGRAPH_API_VERSION "API version to build (1 or 2)" "1")

# Shared API sources (version-independent)
set(HGRAPH_API_SHARED_SOURCES
        api/python/py_evaluation_clock.cpp
        api/python/py_evaluation_engine.cpp
        api/python/py_graph.cpp
        api/python/py_node.cpp
        api/python/py_special_nodes.cpp
        api/python/py_schema.cpp
)

# V1 API sources (time-series type wrappers)
set(HGRAPH_API_V1_SOURCES
        api/python/v1/py_time_series.cpp
        api/python/v1/py_ts.cpp
        api/python/v1/py_tsb.cpp
        api/python/v1/py_tsd.cpp
        api/python/v1/py_tsl.cpp
        api/python/v1/py_tss.cpp
        api/python/v1/py_tsw.cpp
        api/python/v1/py_ref.cpp
        api/python/v1/py_signal.cpp
        api/python/v1/wrapper_factory.cpp
)

# V2 API sources
set(HGRAPH_API_V2_SOURCES
        api/python/v2/py_time_series.cpp
        api/python/v2/py_ts.cpp
        api/python/v2/py_tsb.cpp
        api/python/v2/py_tsd.cpp
        api/python/v2/py_tsl.cpp
        api/python/v2/py_tss.cpp
        api/python/v2/py_tsw.cpp
        api/python/v2/py_ref.cpp
        api/python/v2/py_signal.cpp
        api/python/v2/wrapper_factory.cpp
)

# V2 time-series type sources
set(HGRAPH_TS_V2_SOURCES
        types/time_series/v2/ts_output.cpp
        types/time_series/v2/ts_input.cpp
        types/time_series/v2/access_strategy.cpp
)

# V1 Node sources
set(HGRAPH_NODES_V1_SOURCES
        nodes/v1/base_python_node.cpp
        nodes/v1/component_node.cpp
        nodes/v1/last_value_pull_node.cpp
        nodes/v1/mesh_node.cpp
        nodes/v1/context_node.cpp
        nodes/v1/nested_evaluation_engine.cpp
        nodes/v1/nested_graph_node.cpp
        nodes/v1/nested_node.cpp
        nodes/v1/non_associative_reduce_node.cpp
        nodes/v1/python_generator_node.cpp
        nodes/v1/python_node.cpp
        nodes/v1/push_queue_node.cpp
        nodes/v1/reduce_node.cpp
        nodes/v1/switch_node.cpp
        nodes/v1/try_except_node.cpp
        nodes/v1/tsd_map_node.cpp
        types/v1/node.cpp
)

# V2 Node sources
set(HGRAPH_NODES_V2_SOURCES
        nodes/v2/base_python_node.cpp
        nodes/v2/component_node.cpp
        nodes/v2/last_value_pull_node.cpp
        nodes/v2/mesh_node.cpp
        nodes/v2/context_node.cpp
        nodes/v2/nested_evaluation_engine.cpp
        nodes/v2/nested_graph_node.cpp
        nodes/v2/nested_node.cpp
        nodes/v2/non_associative_reduce_node.cpp
        nodes/v2/python_generator_node.cpp
        nodes/v2/python_node.cpp
        nodes/v2/push_queue_node.cpp
        nodes/v2/reduce_node.cpp
        nodes/v2/switch_node.cpp
        nodes/v2/try_except_node.cpp
        nodes/v2/tsd_map_node.cpp
        types/v2/node.cpp
)

# Select API version sources
if(HGRAPH_API_VERSION STREQUAL "2")
    set(HGRAPH_API_SOURCES ${HGRAPH_API_SHARED_SOURCES} ${HGRAPH_API_V2_SOURCES})
    set(HGRAPH_NODES_SOURCES ${HGRAPH_NODES_V2_SOURCES})
    set(HGRAPH_TS_SOURCES ${HGRAPH_TS_V2_SOURCES})
    message(STATUS "Building with V2 time-series API")
else()
    set(HGRAPH_API_SOURCES ${HGRAPH_API_SHARED_SOURCES} ${HGRAPH_API_V1_SOURCES})
    set(HGRAPH_NODES_SOURCES ${HGRAPH_NODES_V1_SOURCES})
    set(HGRAPH_TS_SOURCES "")  # V1 doesn't use the new time-series types
    message(STATUS "Building with V1 time-series API (default)")
endif()

set(HGRAPH_SOURCES
        ${HGRAPH_API_SOURCES}
        ${HGRAPH_NODES_SOURCES}
        ${HGRAPH_TS_SOURCES}
        builders/builder.cpp
        builders/graph_builder.cpp
        builders/input_builder.cpp
        builders/node_builder.cpp
        builders/output_builder.cpp
        builders/nodes/python_node_builder.cpp
        builders/nodes/python_generator_node_builder.cpp
        builders/nodes/tsd_map_node_builder.cpp
        builders/nodes/reduce_node_builder.cpp
        builders/nodes/context_node_builder.cpp
        builders/nodes/base_nested_graph_node_builder.cpp
        builders/nodes/nested_graph_node_builder.cpp
        builders/nodes/component_node_builder.cpp
        builders/nodes/try_except_node_builder.cpp
        builders/nodes/switch_node_builder.cpp
        builders/nodes/tsd_non_associative_reduce_node_builder.cpp
        builders/nodes/mesh_node_builder.cpp
        builders/nodes/last_value_pull_node_builder.cpp
        builders/time_series_types/time_series_signal_input_builder.cpp
        builders/time_series_types/time_series_value_input_builder.cpp
        builders/time_series_types/specialized_ref_builders.cpp
        builders/time_series_types/time_series_list_input_builder.cpp
        builders/time_series_types/time_series_bundle_input_builder.cpp
        builders/time_series_types/time_series_set_input_builder.cpp
        builders/time_series_types/time_series_window_input_builder.cpp
        builders/time_series_types/time_series_dict_input_builder.cpp
        builders/time_series_types/time_series_value_output_builder.cpp
        builders/time_series_types/cpp_time_series_builder.cpp
        builders/time_series_types/time_series_list_output_builder.cpp
        builders/time_series_types/time_series_bundle_output_builder.cpp
        builders/time_series_types/time_series_set_output_builder.cpp
        builders/time_series_types/time_series_window_output_builder.cpp
        builders/time_series_types/time_series_dict_output_builder.cpp

        python/_hgraph_builder.cpp
        python/_hgraph_module.cpp
        python/_hgraph_nodes.cpp
        python/_hgraph_runtime.cpp
        python/_hgraph_types.cpp
        python/_hgraph_utils.cpp
        python/global_state.cpp
        python/global_keys.cpp

        runtime/evaluation_context.cpp
        runtime/evaluation_engine.cpp
        runtime/graph_executor.cpp
        runtime/record_replay.cpp
        runtime/observers/evaluation_profiler.cpp
        runtime/observers/evaluation_trace.cpp
        runtime/observers/inspection_observer.cpp
        runtime/observers/observers_bindings.cpp

        types/base_time_series.cpp
        types/constants.cpp
        types/error_type.cpp
        types/feature_extension.cpp
        types/graph.cpp
        types/ref.cpp
        types/scalar_types.cpp
        types/schema_type.cpp
        types/time_series_type.cpp
        types/traits.cpp
        types/ts.cpp
        types/ts_signal.cpp
        types/tsd.cpp
        types/tss.cpp
        types/ts_indexed.cpp
        types/tsb.cpp
        types/tsl.cpp
        types/tsw.cpp
        types/value_python_conversion.cpp
        types/type_meta_bindings.cpp
        types/ts_type_meta.cpp
        types/ts_type_meta_bindings.cpp

        util/lifecycle.cpp
        util/sender_receiver_state.cpp
        util/stack_trace.cpp
        util/string_util.cpp
)

add_definitions(-Dhgraph_EXPORTS)

nanobind_add_module(${PROJECT_NAME} STABLE_ABI ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
target_link_libraries(${PROJECT_NAME} PUBLIC fmt::fmt ddvisitor)

# Propagate API version to compiler for conditional compilation
if(HGRAPH_API_VERSION STREQUAL "2")
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_API_V2=1)
else()
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_API_V1=1)
endif()

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