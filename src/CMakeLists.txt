set(HGRAPH_RUNTIME_SOURCES
    hgraph/version.cpp
    hgraph/util/sha256.cpp
    hgraph/manifest/schema_descriptor.cpp
    hgraph/manifest/graph_manifest.cpp
    hgraph/runtime/context_node.cpp
    hgraph/runtime/distributed_boundary.cpp
    hgraph/runtime/distributed_process.cpp
    hgraph/runtime/distributed_protocol.cpp
    hgraph/runtime/distributed_transport.cpp
    hgraph/runtime/distributed_worker.cpp
    hgraph/runtime/spawn_worker.cpp
    hgraph/runtime/diagnostic_path.cpp
    hgraph/runtime/evaluation_clock.cpp
    hgraph/runtime/evaluation_profiler.cpp
    hgraph/runtime/graph_diagnostics.cpp
    hgraph/runtime/evaluation_trace.cpp
    hgraph/runtime/executor.cpp
    hgraph/runtime/checkpoint_codec.cpp
    hgraph/runtime/component_checkpoint.cpp
    hgraph/runtime/graph_checkpoint_coordinator.cpp
    hgraph/runtime/window_expiry_subscriptions.cpp
    hgraph/runtime/feedback_node.cpp
    hgraph/runtime/global_state.cpp
    hgraph/runtime/graph.cpp
    hgraph/runtime/window_expiry_service.cpp
    hgraph/runtime/logger.cpp
    hgraph/runtime/map_node.cpp
    hgraph/runtime/tsl_map_node.cpp
    hgraph/runtime/mesh_node.cpp
    hgraph/runtime/nested_graph_node.cpp
    hgraph/runtime/node.cpp
    hgraph/runtime/node_error.cpp
    hgraph/runtime/canonical_error.cpp
    hgraph/runtime/ordered_reduce_node.cpp
    hgraph/runtime/prepared_output_route.cpp
    hgraph/runtime/push_source_node.cpp
    hgraph/runtime/race_tsd_node.cpp
    hgraph/runtime/reduce_node.cpp
    hgraph/runtime/registry_snapshot.cpp
    hgraph/runtime/service_node.cpp
    hgraph/runtime/shared_output_node.cpp
    hgraph/runtime/switch_node.cpp
    hgraph/runtime/try_except_node.cpp
    hgraph/types/temporal.cpp
    hgraph/types/time_zone_provider.cpp
    hgraph/types/type_pointer.cpp
    hgraph/types/utils/counted_mutex.cpp
    hgraph/types/utils/stable_slot_store.cpp
    hgraph/types/utils/slot_observer.cpp
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
    hgraph/types/metadata/schema_table.cpp
    hgraph/types/metadata/type_registry.cpp
    hgraph/types/metadata/type_names.cpp
    hgraph/types/metadata/value_plan_factory.cpp
    hgraph/types/python_ops.cpp
    hgraph/types/primitive_types.cpp
    hgraph/types/time_series/endpoint_owner.cpp
    hgraph/types/time_series/endpoint_schema.cpp
    hgraph/types/time_series/ts_data/base_view.cpp
    hgraph/types/time_series/ts_data/checkpoint.cpp
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
    hgraph/types/time_series/ts_output/impl/nullable.cpp
    hgraph/types/time_series/ts_output/base_view.cpp
    hgraph/types/time_series/ts_output/bundle_view.cpp
    hgraph/types/time_series/ts_output/dict_view.cpp
    hgraph/types/time_series/ts_output/list_view.cpp
    hgraph/types/time_series/ts_output/set_view.cpp
    hgraph/types/time_series/ts_output/window_view.cpp
    hgraph/types/time_series_reference.cpp
    hgraph/types/value/any_ops.cpp
    hgraph/types/value/compact_container_ops.cpp
    hgraph/types/value/impl/pooled_polymorphic_value_type.cpp
    hgraph/types/value/shared_value_pool.cpp
    hgraph/types/value/binary_codec.cpp
    hgraph/types/value/binary_compression.cpp
    hgraph/types/value/binary_session.cpp
    hgraph/types/value/json_codec.cpp
    hgraph/types/value/python_repr.cpp
    hgraph/types/value/value_view.cpp
    hgraph/types/value/value_conversion.cpp
    hgraph/types/value/value_type_ref.cpp
    hgraph/util/date_time.cpp
)

if(HGRAPH_WITH_ARROW)
    list(APPEND HGRAPH_RUNTIME_SOURCES
        hgraph/types/frame.cpp hgraph/types/series.cpp hgraph/types/value/table_codec.cpp)
endif()

if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    list(APPEND HGRAPH_RUNTIME_SOURCES
        hgraph/python/bridge_state.cpp
        hgraph/python/impl/container_conversions.cpp
        hgraph/python/impl/conversion.cpp
        hgraph/python/impl/object_semantics.cpp
        hgraph/python/impl/python_ops.cpp
        hgraph/python/impl/realized_conversions.cpp
        hgraph/python/impl/retained_value.cpp
        hgraph/python/impl/ts_data_family_conversions.cpp
        hgraph/python/impl/ts_data_structured_conversions.cpp
        hgraph/python/impl/ts_input_conversions.cpp
        hgraph/python/impl/ts_data_conversion.cpp
    )
endif()

set(HGRAPH_WIRING_SOURCES
    hgraph/types/graph_wiring.cpp
    hgraph/types/wiring_inspection.cpp
    hgraph/types/operator_dispatch.cpp
    hgraph/types/record_replay.cpp
    hgraph/types/table_config.cpp
    hgraph/types/registry_reset.cpp
    hgraph/types/impl/request_reply_transport.cpp
    hgraph/types/service_runtime.cpp
    hgraph/types/type_pattern.cpp
    hgraph/types/wiring_observer.cpp
)

set(HGRAPH_STDLIB_SOURCES
    hgraph/runtime/distributed_map_wiring.cpp
    hgraph/runtime/distributed_map_checkpoint.cpp
    hgraph/runtime/spawn.cpp
    hgraph/lib/std/lower.cpp
    hgraph/lib/std/standard_scalar_bindings.cpp
    hgraph/lib/std/operators/arithmetic_impl.cpp
    hgraph/lib/std/operators/arithmetic_impl_numeric.cpp
    hgraph/lib/std/operators/arithmetic_impl_division.cpp
    hgraph/lib/std/operators/arithmetic_impl_temporal.cpp
    hgraph/lib/std/operators/arithmetic_impl_container.cpp
    hgraph/lib/std/operators/arithmetic_impl_aggregate.cpp
    hgraph/lib/std/operators/collection_impl.cpp
    hgraph/lib/std/operators/collection_impl_mapping.cpp
    hgraph/lib/std/operators/collection_impl_sequence.cpp
    hgraph/lib/std/operators/collection_impl_aggregate.cpp
    hgraph/lib/std/operators/collection_impl_set.cpp
    hgraph/lib/std/operators/comparison_impl.cpp
    hgraph/lib/std/operators/comparison_impl_equality.cpp
    hgraph/lib/std/operators/comparison_impl_ordering.cpp
    hgraph/lib/std/operators/comparison_impl_extremum.cpp
    hgraph/lib/std/operators/container_impl.cpp
    hgraph/lib/std/operators/control_impl.cpp
    hgraph/lib/std/operators/conversion_impl.cpp
    hgraph/lib/std/operators/conversion_impl_scalar.cpp
    hgraph/lib/std/operators/conversion_impl_collection.cpp
    hgraph/lib/std/operators/conversion_impl_combine.cpp
    hgraph/lib/std/operators/higher_order_impl.cpp
    hgraph/lib/std/operators/io_impl.cpp
    hgraph/lib/std/operators/data_frame_impl.cpp
    hgraph/lib/std/operators/json_impl.cpp
    hgraph/lib/std/operators/logical_impl.cpp
    hgraph/lib/std/operators/record_replay_memory_impl.cpp
    hgraph/lib/std/operators/registration.cpp
    hgraph/lib/std/operators/stream_impl.cpp
    hgraph/lib/std/operators/stream_impl_flow.cpp
    hgraph/lib/std/operators/stream_impl_window.cpp
    hgraph/lib/std/operators/series_impl.cpp
    hgraph/lib/std/operators/string_impl.cpp
    hgraph/lib/std/operators/table_impl.cpp
    hgraph/lib/std/operators/temporal_impl.cpp
    hgraph/lib/std/operators/temporal_impl_components.cpp
    hgraph/lib/std/operators/temporal_impl_instants.cpp
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
    if(HGRAPH_WITH_ARROW)
        set(_unified hgraph_stdlib)
        add_library(hgraph_runtime INTERFACE)
        target_link_libraries(hgraph_runtime INTERFACE $<LINK_ONLY:hgraph_stdlib> hgraph::options)
    else()
        set(_unified hgraph_runtime)
        set(HGRAPH_STDLIB_SOURCES "")
    endif()
    add_library(${_unified} SHARED ${HGRAPH_RUNTIME_SOURCES} ${HGRAPH_WIRING_SOURCES} ${HGRAPH_STDLIB_SOURCES})
    target_link_libraries(${_unified} PUBLIC hgraph::options)
    add_library(hgraph_wiring INTERFACE)
    target_link_libraries(hgraph_wiring INTERFACE hgraph_runtime hgraph::options)
    set(_compiled_targets ${_unified})
else()
    add_library(hgraph_runtime ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_RUNTIME_SOURCES})
    add_library(hgraph_wiring ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_WIRING_SOURCES})
    target_link_libraries(hgraph_runtime PUBLIC hgraph::options)
    target_link_libraries(hgraph_wiring PUBLIC hgraph_runtime hgraph::options)
    set(_compiled_targets hgraph_runtime hgraph_wiring)
    if(HGRAPH_WITH_ARROW)
        add_library(hgraph_stdlib ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_STDLIB_SOURCES})
        target_link_libraries(hgraph_stdlib PUBLIC hgraph_wiring hgraph::options)
        list(APPEND _compiled_targets hgraph_stdlib)
    endif()
endif()

add_library(hgraph_core INTERFACE)
if(HGRAPH_WITH_ARROW)
    add_library(hgraph::stdlib ALIAS hgraph_stdlib)
    set_target_properties(hgraph_stdlib PROPERTIES EXPORT_NAME stdlib)
    target_link_libraries(hgraph_core INTERFACE hgraph_stdlib)
    # stdlib's public frame operators include Arrow headers. Runtime consumers
    # using just the wiring and value interfaces need no Arrow headers or flags.
    foreach(_arrow_target IN LISTS _hgraph_arrow_targets)
        target_link_libraries(hgraph_stdlib PUBLIC "$<BUILD_INTERFACE:${_arrow_target}>")
    endforeach()
    set(_arrow_includes "$<BUILD_INTERFACE:$<TARGET_PROPERTY:Arrow::arrow_shared,INTERFACE_INCLUDE_DIRECTORIES>>")
    target_include_directories(hgraph_stdlib SYSTEM BEFORE PUBLIC "${_arrow_includes}")
    if(NOT HGRAPH_WINDOWS_UNIFIED_SHARED)
        target_link_libraries(hgraph_runtime PRIVATE Arrow::arrow_shared ArrowCompute::arrow_compute_shared)
        target_include_directories(hgraph_runtime SYSTEM BEFORE PRIVATE "${_arrow_includes}")
    endif()
else()
    target_link_libraries(hgraph_core INTERFACE hgraph_wiring)
endif()

if(BUILD_TESTING AND HGRAPH_ENABLE_DEBUGGER_SMOKE_TESTS AND UNIX)
    # Debugger navigation needs endpoint types and notification targets whose
    # definitions live in runtime translation units, not in the fixture.
    # Keep Release optimization and IPO while supplying their debug information.
    target_compile_options(hgraph_runtime PRIVATE -g)
endif()

add_library(hgraph::runtime ALIAS hgraph_runtime)
add_library(hgraph::wiring ALIAS hgraph_wiring)
if(HGRAPH_BUILD_SHARED)
    if(UNIX AND NOT APPLE)
        include(CheckCXXCompilerFlag)
        check_cxx_compiler_flag(
            "-fno-semantic-interposition"
            HGRAPH_HAS_NO_SEMANTIC_INTERPOSITION
        )

        if(HGRAPH_HAS_NO_SEMANTIC_INTERPOSITION)
            # ELF makes every default-visible C++ method preemptible unless the
            # compiler is told otherwise. hgraph's public methods are also used
            # heavily inside these DSOs; preserving interposition there forces
            # PLT calls and prevents local optimization throughout the runtime
            # hot path. The public symbols remain exported for SDK consumers,
            # but hgraph does not support replacing its C++ ABI via LD_PRELOAD.
            foreach(_hgraph_target IN LISTS _compiled_targets)
                target_compile_options(${_hgraph_target} PRIVATE
                    $<$<CONFIG:Release>:-fno-semantic-interposition>
                    $<$<CONFIG:RelWithDebInfo>:-fno-semantic-interposition>
                    $<$<CONFIG:MinSizeRel>:-fno-semantic-interposition>
                )
            endforeach()
        endif()

        if(NOT HGRAPH_BUILD_PYTHON_BINDINGS AND NOT HGRAPH_ENABLE_PYTHON_USER_NODES)
            # Keep the library layering enforceable on Linux. Mach-O rejects an
            # undefined symbol when the shared library is linked; ELF defers it
            # to load time. So a call from a LOWER library into a HIGHER one
            # (hgraph_wiring naming an hgraph_stdlib symbol, which is exactly
            # what reset_all_registries() did) links cleanly here and fails only
            # on macOS — far from the edit that caused it, and invisible to a
            # Linux-only shared CI leg. Ask the ELF linker for the same answer.
            #
            # Python-enabled builds are excluded: their runtime legitimately
            # leaves interpreter symbols to be resolved by the process that
            # imports the extension module.
            foreach(_hgraph_target IN LISTS _compiled_targets)
                target_link_options(${_hgraph_target} PRIVATE LINKER:--no-undefined)
            endforeach()
        endif()

    endif()

    if(UNIX)
        # IPO applies to ALL UNIX shared builds, macOS included: unlike the
        # ELF-only interposition flags above, cross-TU inlining is a
        # platform-neutral win — measured ~26% on the mac native per-node
        # hot loop (per-node tick 98ns with IPO vs 130ns without on the
        # python-boundary micro-bench). Preserve the public DSO boundaries
        # while optimizing calls within each component as a whole.
        include(CheckIPOSupported)
        check_ipo_supported(
            RESULT HGRAPH_HAS_INTERPROCEDURAL_OPTIMIZATION
            OUTPUT HGRAPH_INTERPROCEDURAL_OPTIMIZATION_ERROR
            LANGUAGES CXX
        )
        if(HGRAPH_HAS_INTERPROCEDURAL_OPTIMIZATION)
            set_target_properties(${_compiled_targets} PROPERTIES
                INTERPROCEDURAL_OPTIMIZATION_RELEASE ON
                INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO ON
                INTERPROCEDURAL_OPTIMIZATION_MINSIZEREL ON
            )
        endif()
    endif()

    if(NOT HGRAPH_WINDOWS_UNIFIED_SHARED)
        set_target_properties(${_compiled_targets} PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
    endif()
    foreach(_hgraph_target IN LISTS _compiled_targets)
        target_link_libraries(${_hgraph_target} PRIVATE hgraph_private_dependencies)
        target_compile_definitions(${_hgraph_target} PRIVATE hgraph_EXPORTS)
    endforeach()

    # The shared components reference each other through their install names
    # (@rpath/... on macOS, bare sonames on ELF), so each carries a relative
    # rpath to its own directory: an installed SDK then resolves from any
    # prefix without an absolute path baked in. The packager's
    # CMAKE_INSTALL_RPATH, if any, comes first; the ../pyarrow entry supports
    # the wheel layout, where Arrow is loaded from the pyarrow package.
    if(APPLE)
        set(_hgraph_core_install_rpath "@loader_path")
        set(_hgraph_core_pyarrow_rpath "@loader_path/../pyarrow")
    elseif(UNIX)
        set(_hgraph_core_install_rpath "$ORIGIN")
        set(_hgraph_core_pyarrow_rpath "$ORIGIN/../pyarrow")
    else()
        set(_hgraph_core_install_rpath "")
        set(_hgraph_core_pyarrow_rpath "")
    endif()
    if(_hgraph_core_install_rpath)
        if(HGRAPH_BUILD_PYTHON_BINDINGS)
            list(APPEND _hgraph_core_install_rpath "${_hgraph_core_pyarrow_rpath}")
        endif()
        if(CMAKE_INSTALL_RPATH)
            list(PREPEND _hgraph_core_install_rpath ${CMAKE_INSTALL_RPATH})
            list(REMOVE_DUPLICATES _hgraph_core_install_rpath)
        endif()
        set_target_properties(${_compiled_targets} PROPERTIES
            INSTALL_RPATH "${_hgraph_core_install_rpath}"
        )
    endif()
endif()

if(MSVC)
    # HGRAPH_CLASS_EXPORT deliberately omits consumer-side dllimport for the
    # complete class while retaining producer-side exports. C4251/C4275 then
    # describe private representation that is not part of the supported DLL
    # boundary. Keep this documented exception on the producing targets only.
    foreach(_hgraph_target IN LISTS _compiled_targets)
        target_compile_options(${_hgraph_target} PRIVATE /bigobj /wd4251 /wd4275)
    endforeach()
endif()

foreach(_hgraph_target IN LISTS _compiled_targets)
    hgraph_enable_private_pch(${_hgraph_target})
endforeach()

set_target_properties(hgraph_runtime PROPERTIES EXPORT_NAME runtime)
set_target_properties(hgraph_wiring PROPERTIES EXPORT_NAME wiring)
