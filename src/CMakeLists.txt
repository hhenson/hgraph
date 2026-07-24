set(HGRAPH_RUNTIME_SOURCES
    hgraph/version.cpp
    hgraph/runtime/context_node.cpp
    hgraph/runtime/diagnostic_path.cpp
    hgraph/runtime/evaluation_clock.cpp
    hgraph/runtime/evaluation_profiler.cpp
    hgraph/runtime/inspector.cpp
    hgraph/runtime/evaluation_trace.cpp
    hgraph/runtime/executor.cpp
    hgraph/runtime/feedback_node.cpp
    hgraph/runtime/global_state.cpp
    hgraph/runtime/graph.cpp
    hgraph/runtime/logger.cpp
    hgraph/runtime/map_node.cpp
    hgraph/runtime/tsl_map_node.cpp
    hgraph/runtime/mesh_node.cpp
    hgraph/runtime/nested_graph_node.cpp
    hgraph/runtime/node.cpp
    hgraph/runtime/node_error.cpp
    hgraph/runtime/ordered_reduce_node.cpp
    hgraph/runtime/push_source_node.cpp
    hgraph/runtime/race_tsd_node.cpp
    hgraph/runtime/reduce_node.cpp
    hgraph/runtime/service_node.cpp
    hgraph/runtime/shared_output_node.cpp
    hgraph/runtime/switch_node.cpp
    hgraph/runtime/try_except_node.cpp
    hgraph/types/frame.cpp
    hgraph/types/series.cpp
    hgraph/types/temporal.cpp
    hgraph/types/time_zone_provider.cpp
    hgraph/types/type_pointer.cpp
    hgraph/types/metadata/debug_descriptor.cpp
    hgraph/types/metadata/ts_data_atomic_ops.cpp
    hgraph/types/metadata/ts_data_dynamic_list_ops.cpp
    hgraph/types/metadata/ts_data_fixed_structured_ops.cpp
    hgraph/types/metadata/ts_data_fixed_structured_plan.cpp
    hgraph/types/metadata/ts_data_plan_factory.cpp
    hgraph/types/metadata/ts_data_slot_ops.cpp
    hgraph/types/metadata/ts_data_window_ops.cpp
    hgraph/types/metadata/type_record_registry.cpp
    hgraph/types/metadata/type_realization.cpp
    hgraph/types/metadata/type_registry.cpp
    hgraph/types/metadata/value_plan_factory.cpp
    hgraph/types/primitive_types.cpp
    hgraph/types/time_series/endpoint_owner.cpp
    hgraph/types/time_series/endpoint_schema.cpp
    hgraph/types/time_series/ts_data/base_view.cpp
    hgraph/types/time_series/ts_data/dict_view.cpp
    hgraph/types/time_series/ts_data/indexed_view.cpp
    hgraph/types/time_series/ts_data/ops.cpp
    hgraph/types/time_series/ts_data/proxy.cpp
    hgraph/types/time_series/ts_data/set_view.cpp
    hgraph/types/time_series/ts_data/storage.cpp
    hgraph/types/time_series/ts_data/types.cpp
    hgraph/types/time_series/ts_data/window_view.cpp
    hgraph/types/time_series/ts_delta.cpp
    hgraph/types/time_series/ts_type_ref.cpp
    hgraph/types/time_series/ts_input.cpp
    hgraph/types/time_series/ts_input/base_view.cpp
    hgraph/types/time_series/ts_input/bundle_view.cpp
    hgraph/types/time_series/ts_input/dict_view.cpp
    hgraph/types/time_series/ts_input/list_view.cpp
    hgraph/types/time_series/ts_input/set_view.cpp
    hgraph/types/time_series/ts_input/target_link.cpp
    hgraph/types/time_series/ts_input/target_link_ops.cpp
    hgraph/types/time_series/ts_input/window_view.cpp
    hgraph/types/time_series/ts_output.cpp
    hgraph/types/time_series/ts_output/alternative.cpp
    hgraph/types/time_series/ts_output/base_view.cpp
    hgraph/types/time_series/ts_output/bundle_view.cpp
    hgraph/types/time_series/ts_output/dict_view.cpp
    hgraph/types/time_series/ts_output/list_view.cpp
    hgraph/types/time_series/ts_output/mutation_view.cpp
    hgraph/types/time_series/ts_output/set_view.cpp
    hgraph/types/time_series/ts_output/window_view.cpp
    hgraph/types/time_series_reference.cpp
    hgraph/types/value/any_ops.cpp
    hgraph/types/value/compact_container_ops.cpp
    hgraph/types/value/json_codec.cpp
    hgraph/types/value/table_codec.cpp
    hgraph/types/value/value_view.cpp
    hgraph/types/value/value_conversion.cpp
    hgraph/types/value/value_type_ref.cpp
    hgraph/util/date_time.cpp
)

if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    list(APPEND HGRAPH_RUNTIME_SOURCES hgraph/python/bridge_state.cpp)
endif()

set(HGRAPH_WIRING_SOURCES
    hgraph/types/graph_wiring.cpp
    hgraph/types/operator_dispatch.cpp
    hgraph/types/record_replay.cpp
    hgraph/types/registry_reset.cpp
    hgraph/types/service_runtime.cpp
    hgraph/types/type_pattern.cpp
    hgraph/types/wiring_observer.cpp
)

set(HGRAPH_STDLIB_SOURCES
    hgraph/lib/std/lower.cpp
    hgraph/lib/std/operators/arithmetic_impl.cpp
    hgraph/lib/std/operators/collection_impl.cpp
    hgraph/lib/std/operators/comparison_impl.cpp
    hgraph/lib/std/operators/container_impl.cpp
    hgraph/lib/std/operators/control_impl.cpp
    hgraph/lib/std/operators/conversion_impl.cpp
    hgraph/lib/std/operators/higher_order_impl.cpp
    hgraph/lib/std/operators/io_impl.cpp
    hgraph/lib/std/operators/data_frame_impl.cpp
    hgraph/lib/std/operators/json_impl.cpp
    hgraph/lib/std/operators/logical_impl.cpp
    hgraph/lib/std/operators/numpy_impl.cpp
    hgraph/lib/std/operators/record_replay_frame_impl.cpp
    hgraph/lib/std/operators/record_replay_memory_impl.cpp
    hgraph/lib/std/operators/registration.cpp
    hgraph/lib/std/operators/stream_impl.cpp
    hgraph/lib/std/operators/series_impl.cpp
    hgraph/lib/std/operators/string_impl.cpp
    hgraph/lib/std/operators/table_impl.cpp
    hgraph/lib/std/operators/temporal_impl.cpp
    hgraph/lib/std/operators/convert_target.cpp
)

set(HGRAPH_WINDOWS_UNIFIED_SHARED OFF)
if(MSVC AND HGRAPH_BUILD_SHARED)
    # A single export macro cannot describe dependencies between three Windows
    # DLLs: compiling each component with hgraph_EXPORTS makes runtime objects
    # emit wiring vtables, while exporting every unannotated template symbol
    # exceeds MSVC's 65,535-object library limit. Keep the public component
    # targets, but place their implementation in one explicitly exported DLL.
    set(HGRAPH_WINDOWS_UNIFIED_SHARED ON)
    add_library(hgraph_runtime INTERFACE)
    add_library(hgraph_wiring INTERFACE)
    add_library(hgraph_stdlib SHARED
        ${HGRAPH_RUNTIME_SOURCES}
        ${HGRAPH_WIRING_SOURCES}
        ${HGRAPH_STDLIB_SOURCES}
    )
else()
    add_library(hgraph_runtime ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_RUNTIME_SOURCES})
    add_library(hgraph_wiring ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_WIRING_SOURCES})
    add_library(hgraph_stdlib ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_STDLIB_SOURCES})
endif()
add_library(hgraph_core INTERFACE)

add_library(hgraph::runtime ALIAS hgraph_runtime)
add_library(hgraph::wiring ALIAS hgraph_wiring)
add_library(hgraph::stdlib ALIAS hgraph_stdlib)

if(HGRAPH_WINDOWS_UNIFIED_SHARED)
    target_link_libraries(hgraph_stdlib PUBLIC hgraph::options)
    target_link_libraries(hgraph_runtime INTERFACE hgraph_stdlib hgraph::options)
    target_link_libraries(hgraph_wiring INTERFACE hgraph_stdlib hgraph::options)
else()
    target_link_libraries(hgraph_runtime PUBLIC hgraph::options)
    target_link_libraries(hgraph_wiring PUBLIC hgraph_runtime hgraph::options)
    target_link_libraries(hgraph_stdlib PUBLIC hgraph_wiring hgraph::options)
endif()
target_link_libraries(hgraph_stdlib PRIVATE $<BUILD_INTERFACE:Boost::math>)
target_link_libraries(hgraph_core INTERFACE hgraph_stdlib hgraph::options)

if(HGRAPH_BUILD_SHARED)
    if(HGRAPH_WINDOWS_UNIFIED_SHARED)
        target_link_libraries(hgraph_stdlib PRIVATE hgraph_private_dependencies)
        target_compile_definitions(hgraph_stdlib PRIVATE hgraph_EXPORTS)
    else()
        set_target_properties(hgraph_runtime hgraph_wiring hgraph_stdlib PROPERTIES
            WINDOWS_EXPORT_ALL_SYMBOLS ON
        )
        target_link_libraries(hgraph_runtime PRIVATE hgraph_private_dependencies)
        target_link_libraries(hgraph_wiring PRIVATE hgraph_private_dependencies)
        target_link_libraries(hgraph_stdlib PRIVATE hgraph_private_dependencies)
        target_compile_definitions(hgraph_runtime PRIVATE hgraph_EXPORTS)
        target_compile_definitions(hgraph_wiring PRIVATE hgraph_EXPORTS)
        target_compile_definitions(hgraph_stdlib PRIVATE hgraph_EXPORTS)
    endif()

    if(HGRAPH_BUILD_PYTHON_BINDINGS)
        if(APPLE)
            set(_hgraph_core_install_rpath "@loader_path;@loader_path/../pyarrow")
        elseif(UNIX)
            set(_hgraph_core_install_rpath "$ORIGIN;$ORIGIN/../pyarrow")
        else()
            set(_hgraph_core_install_rpath "")
        endif()
        if(_hgraph_core_install_rpath)
            set_target_properties(hgraph_runtime hgraph_wiring hgraph_stdlib PROPERTIES
                INSTALL_RPATH "${_hgraph_core_install_rpath}"
            )
        endif()
    endif()
endif()

if(MSVC)
    target_compile_options(hgraph_stdlib PRIVATE /bigobj)
    if(NOT HGRAPH_WINDOWS_UNIFIED_SHARED)
        target_compile_options(hgraph_runtime PRIVATE /bigobj)
        target_compile_options(hgraph_wiring PRIVATE /bigobj)
    endif()
endif()

hgraph_enable_private_pch(hgraph_stdlib)
if(NOT HGRAPH_WINDOWS_UNIFIED_SHARED)
    hgraph_enable_private_pch(hgraph_runtime)
    hgraph_enable_private_pch(hgraph_wiring)
endif()

set_target_properties(hgraph_runtime PROPERTIES EXPORT_NAME runtime)
set_target_properties(hgraph_wiring PROPERTIES EXPORT_NAME wiring)
set_target_properties(hgraph_stdlib PROPERTIES EXPORT_NAME stdlib)
