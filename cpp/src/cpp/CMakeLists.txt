project(_hgraph)

set(HGRAPH_SOURCES
        api/python/py_evaluation_clock.cpp
        api/python/py_evaluation_engine.cpp
        api/python/py_graph.cpp
        api/python/py_node.cpp
        api/python/py_special_nodes.cpp
        api/python/py_time_series.cpp
        api/python/py_ref.cpp
        api/python/py_signal.cpp
        api/python/py_ts.cpp
        api/python/py_tsb.cpp
        api/python/py_tsd.cpp
        api/python/py_tsl.cpp
        api/python/py_tss.cpp
        api/python/py_tsw.cpp
        api/python/py_value.cpp
        api/python/py_ts_type_registry.cpp
        api/python/py_ts_runtime_internal.cpp
        api/python/wrapper_factory.cpp
        api/python/wrapper_factory_ts_views.cpp
        builders/builder.cpp
        builders/graph_builder.cpp
        builders/node_builder.cpp
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
        nodes/base_python_node.cpp
        nodes/component_node.cpp
        nodes/last_value_pull_node.cpp
        nodes/mesh_node.cpp
        nodes/context_node.cpp
        nodes/nested_evaluation_engine.cpp
        nodes/nested_graph_node.cpp
        nodes/nested_node.cpp
        nodes/non_associative_reduce_node.cpp
        nodes/python_generator_node.cpp
        nodes/python_node.cpp
        nodes/push_queue_node.cpp
        nodes/reduce_node.cpp
        nodes/switch_node.cpp
        nodes/try_except_node.cpp
        nodes/tsd_map_node.cpp

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

        types/constants.cpp
        types/error_type.cpp
        types/feature_extension.cpp
        types/graph.cpp
        types/node.cpp
        types/ref.cpp
        types/scalar_types.cpp
        types/schema_type.cpp
        types/traits.cpp
        types/value/type_registry.cpp
        types/value/type_meta_bindings.cpp
        types/value/view_range.cpp
        types/time_series/link_observer_registry.cpp
        types/time_series/link_target.cpp
        types/time_series/python_value_cache_stats.cpp
        types/time_series/ref_link.cpp
        types/time_series/ts_input.cpp
        types/time_series/ts_meta_schema_cache.cpp
        types/time_series/ts_ops.cpp
        types/time_series/ts_ops/link_notification.cpp
        types/time_series/ts_ops/link_notification_dispatch.cpp
        types/time_series/ts_ops/path_meta_utils.cpp
        types/time_series/ts_ops/path_meta_utils_tsd_history.cpp
        types/time_series/ts_ops/view_resolution.cpp
        types/time_series/ts_ops/view_resolution_paths_mapping.cpp
        types/time_series/ts_ops/view_resolution_paths_static.cpp
        types/time_series/ts_ops/view_resolution_paths_time.cpp
        types/time_series/ts_ops/view_resolution_resolve.cpp
        types/time_series/ts_ops/view_resolution_binding.cpp
        types/time_series/ts_ops/bridge_delta_ops.cpp
        types/time_series/ts_ops/projection_ops_tsd_key_set.cpp
        types/time_series/ts_ops/view_data_ops.cpp
        types/time_series/ts_ops/core_ops.cpp
        types/time_series/ts_ops/core_ops_modified.cpp
        types/time_series/ts_ops/core_ops_valid.cpp
        types/time_series/ts_ops/core_ops_all_valid.cpp
        types/time_series/ts_ops/python_ops.cpp
        types/time_series/ts_ops/python_view_helpers.cpp
        types/time_series/ts_ops/core_ops_value_delta.cpp
        types/time_series/ts_ops/python_ops_delta.cpp
        types/time_series/ts_ops/python_ops_delta_containers.cpp
        types/time_series/ts_ops/python_ops_delta_tsd.cpp
        types/time_series/ts_ops/python_ops_delta_tsd_map.cpp
        types/time_series/ts_ops/python_ops_delta_tsd_ref_payload.cpp
        types/time_series/ts_ops/python_ops_delta_tsd_postprocess.cpp
        types/time_series/ts_ops/python_ops_from_python.cpp
        types/time_series/ts_ops/python_ops_from_python_ref.cpp
        types/time_series/ts_ops/python_ops_from_python_tsw.cpp
        types/time_series/ts_ops/python_ops_from_python_tss.cpp
        types/time_series/ts_ops/python_ops_from_python_static_containers.cpp
        types/time_series/ts_ops/python_ops_from_python_helpers.cpp
        types/time_series/ts_ops/python_ops_from_python_tsd.cpp
        types/time_series/ts_ops/python_ops_from_python_tsd_ref.cpp
        types/time_series/ts_ops/bind_copy_api_ts_ops.cpp
        types/time_series/ts_ops/bind_copy_api.cpp
        types/time_series/ts_ops/bind_copy_api_binding.cpp
        types/time_series/ts_ops/bind_copy_api_copy.cpp
        types/time_series/ts_ops/window_ops.cpp
        types/time_series/ts_ops/set_ops.cpp
        types/time_series/ts_ops/dict_ops.cpp
        types/time_series/ts_output.cpp
        types/time_series/ts_type_registry.cpp
        types/time_series/ts_value.cpp
        types/time_series/ts_view.cpp
        types/time_series/view_data.cpp

        util/lifecycle.cpp
        util/sender_receiver_state.cpp
        util/stack_trace.cpp
        util/string_util.cpp
)

add_definitions(-Dhgraph_EXPORTS)

nanobind_add_module(${PROJECT_NAME} STABLE_ABI ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
target_link_libraries(${PROJECT_NAME} PUBLIC fmt::fmt ddvisitor dynamic_bitset ${HGRAPH_NANOARROW_TARGET})

# Propagate HGRAPH_WITH_BACKWARD to the compiler for conditional compilation
if (HGRAPH_WITH_BACKWARD)
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_WITH_BACKWARD=1)
else ()
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_WITH_BACKWARD=0)
endif ()

if (HGRAPH_ENABLE_PY_CACHE_STATS)
    target_compile_definitions(${PROJECT_NAME} PRIVATE HGRAPH_ENABLE_PY_CACHE_STATS=1)
endif ()

# Suppress benign fmt warning on GCC/Clang for this target only (avoid touching ALIAS/IMPORTED fmt targets)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${PROJECT_NAME} PRIVATE -Wno-tautological-compare)
endif ()

# Opt-in strict warnings for hgraph sources.
if (HGRAPH_STRICT_WARNINGS)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${PROJECT_NAME} PRIVATE -Wextra -Wpedantic -Werror)
    elseif (MSVC)
        target_compile_options(${PROJECT_NAME} PRIVATE /W4 /WX)
    endif ()
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
