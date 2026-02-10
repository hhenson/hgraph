"""
C++ Runtime Integration Module

This module configures the hgraph system to use the C++ runtime implementation
when the 'use_cpp' feature is enabled. It registers C++ builder factories and
node builders to replace the default Python implementations.

The C++ runtime is activated via:
- Environment variable: HGRAPH_USE_CPP=true
- Configuration file: use_cpp: true

If the feature is not enabled, this module does nothing and the Python runtime is used.
"""

from hgraph._feature_switch import is_feature_enabled

if is_feature_enabled("use_cpp"):
    print("\n>>>>>>>>>>>>>>>>>>>"
          "\nC++ Runtime enabled"
          "\n<<<<<<<<<<<<<<<<<<<")
    try:
        from datetime import date, datetime, timedelta
        import atexit
        import hgraph._hgraph as _hgraph
        import hgraph

        # Clear thread-local nb::object caches before interpreter shutdown
        # to prevent SIGSEGV from destroying Python objects after GC cleanup.
        atexit.register(_hgraph._clear_thread_local_caches)


        def _get_value_schema_for_scalar_type(scalar_type):
            """Get the Value type schema for a scalar type."""
            from hgraph._hgraph import value
            schema_map = {
                bool: value.scalar_type_meta_bool,
                int: value.scalar_type_meta_int64,
                float: value.scalar_type_meta_double,
                str: value.scalar_type_meta_string,
                date: value.scalar_type_meta_date,
                datetime: value.scalar_type_meta_datetime,
                timedelta: value.scalar_type_meta_timedelta,
            }
            schema_fn = schema_map.get(scalar_type.py_type)
            if schema_fn is not None:
                return schema_fn()
            # Fallback: check if scalar_type has cpp_type, otherwise use Python object schema
            if hasattr(scalar_type, 'cpp_type') and scalar_type.cpp_type is not None:
                return scalar_type.cpp_type
            # Default to object (nb::object) schema for unknown types
            return value.get_scalar_type_meta(scalar_type.py_type)


        def _setup_constants():
            """Replace Python date/time constants and core enums with C++ versions."""
            hgraph.MIN_DT = _hgraph.MIN_DT
            hgraph.MAX_DT = _hgraph.MAX_DT
            hgraph.MIN_ST = _hgraph.MIN_ST
            hgraph.MAX_ET = _hgraph.MAX_ET
            hgraph.MIN_TD = _hgraph.MIN_TD

            hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_SIGNATURE = _hgraph.NodeSignature
            hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_TYPE_ENUM = _hgraph.NodeTypeEnum
            hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.INJECTABLE_TYPES_ENUM = (
                _hgraph.InjectableTypesEnum
            )

            hgraph._runtime._evaluation_engine.EvaluationMode = _hgraph.EvaluationMode
            hgraph._runtime._evaluation_engine.EvaluationLifeCycleObserver = _hgraph.EvaluationLifeCycleObserver

            hgraph.TimeSeriesReference._BUILDER = _hgraph.TimeSeriesReference.make
            hgraph.TimeSeriesReference._INSTANCE_OF = lambda obj: isinstance(obj, _hgraph.TimeSeriesReference)

            hgraph._builder._graph_builder.EDGE_TYPE = _hgraph.Edge


        def _setup_graph_builders():
            """Register C++ GraphBuilder and GraphEngine factories."""

            def _make_cpp_graph_builder(node_builders, edges):
                """Convert Python Edge dataclass instances to C++ _hgraph.Edge"""
                cpp_edges = []
                for e in edges:
                    try:
                        cpp_edges.append(
                            _hgraph.Edge(int(e.src_node), list(e.output_path), int(e.dst_node), list(e.input_path))
                        )
                    except Exception:
                        # If it's already a C++ Edge, keep as-is
                        cpp_edges.append(e)
                return _hgraph.GraphBuilder(list(node_builders), cpp_edges)

            hgraph._builder._graph_builder.GraphBuilder.register(_hgraph.GraphBuilder)
            hgraph.GraphBuilderFactory.declare(_make_cpp_graph_builder)

            def _make_cpp_graph_engine(graph_builder, run_mode, observers):
                """Create a C++ GraphEngine from a Python GraphBuilder"""
                if hasattr(run_mode, "name"):
                    run_mode = _hgraph.EvaluationMode[run_mode.name]
                else:
                    run_mode = _hgraph.EvaluationMode(int(run_mode))
                return _hgraph.GraphExecutor(graph_builder, run_mode, observers)

            hgraph.GraphEngineFactory.declare(_make_cpp_graph_engine)


        def _setup_ts_builder_factory():
            """Register the C++ TimeSeriesBuilderFactory with all type-specific builders."""

            class HgCppFactory(hgraph.TimeSeriesBuilderFactory):
                def make_error_builder(self, value_tp):
                    return self.make_output_builder(value_tp)

                def make_input_builder(self, value_tp):
                    """Create a CppTimeSeriesInputBuilder from TSMeta."""
                    if type(value_tp) is hgraph.HgCONTEXTTypeMetaData:
                        return self.make_input_builder(value_tp.ts_type)
                    ts_meta = value_tp.cpp_type
                    if ts_meta is None:
                        raise RuntimeError(f"Cannot get TSMeta for input type {value_tp}")
                    return _hgraph.CppTimeSeriesInputBuilder(ts_meta)

                def make_output_builder(self, value_tp):
                    """Create a CppTimeSeriesOutputBuilder from TSMeta."""
                    ts_meta = value_tp.cpp_type
                    if ts_meta is None:
                        raise RuntimeError(f"Cannot get TSMeta for output type {value_tp}")
                    return _hgraph.CppTimeSeriesOutputBuilder(ts_meta)

            def _ts_input_builder_type_for(scalar_type):
                return _hgraph.InputBuilder_TS_Value

            def _ts_output_builder_for_tp(scalar_type):
                schema = _get_value_schema_for_scalar_type(scalar_type)
                return lambda: _hgraph.OutputBuilder_TS_Value(schema)

            def _tss_input_builder_type_for(scalar_type):
                schema = _get_value_schema_for_scalar_type(scalar_type)
                return lambda: _hgraph.InputBuilder_TSS(schema)

            def _tss_output_builder_for_tp(scalar_type):
                schema = _get_value_schema_for_scalar_type(scalar_type)
                return lambda: _hgraph.OutputBuilder_TSS(schema)

            def _tsd_input_builder_type_for(key_scalar_type):
                key_schema = _get_value_schema_for_scalar_type(key_scalar_type)
                return lambda ts_builder: _hgraph.InputBuilder_TSD(ts_builder, key_schema)

            def _tsd_output_builder_for_tp(key_scalar_type):
                key_schema = _get_value_schema_for_scalar_type(key_scalar_type)
                return lambda ts_builder, ts_ref_builder: _hgraph.OutputBuilder_TSD(ts_builder, ts_ref_builder, key_schema)

            def _tsw_input_builder_for(scalar_type):
                element_schema = _get_value_schema_for_scalar_type(scalar_type)
                return _hgraph.InputBuilder_TSW(element_schema)

            def _tsw_output_builder_for(scalar_type, size: int, min_size: int):
                element_schema = _get_value_schema_for_scalar_type(scalar_type)
                return _hgraph.OutputBuilder_TSW(size, min_size, element_schema)

            def _ttsw_output_builder_for(scalar_type, size: timedelta, min_size: timedelta):
                element_schema = _get_value_schema_for_scalar_type(scalar_type)
                return _hgraph.OutputBuilder_TTSW(size, min_size, element_schema)

            hgraph.TimeSeriesBuilderFactory.declare(HgCppFactory())


        def _setup_node_builders():
            """Register all C++ node builder classes and factories."""

            hgraph._wiring._wiring_node_class.PythonWiringNodeClass.BUILDER_CLASS = _hgraph.PythonNodeBuilder
            hgraph._wiring._wiring_node_class.PythonGeneratorWiringNodeClass.BUILDER_CLASS = (
                _hgraph.PythonGeneratorNodeBuilder
            )

            # SetDelta factory
            def _create_set_delta(added, removed, tp):
                from hgraph._impl._types._tss import PythonSetDelta
                return PythonSetDelta[tp](
                    added=frozenset() if added is None else frozenset(added),
                    removed=frozenset() if removed is None else frozenset(removed)
                )

            hgraph.set_set_delta_factory(_create_set_delta)

            # Generic factory for nested node builders with standard 9-arg signature
            def _make_nested_factory(cpp_class, normalize_ids=False):
                def factory(signature, scalars, input_builder, output_builder, error_builder,
                            recordable_state_builder=None, nested_graph=None,
                            input_node_ids=None, output_node_id=None, **_kwargs):
                    if normalize_ids:
                        input_node_ids_ = dict(input_node_ids) if input_node_ids is not None else {}
                        output_node_id_ = -1 if output_node_id in (None, 0) else int(output_node_id)
                    else:
                        input_node_ids_ = input_node_ids
                        output_node_id_ = output_node_id
                    return cpp_class(
                        signature, scalars, input_builder, output_builder, error_builder,
                        recordable_state_builder, nested_graph, input_node_ids_, output_node_id_,
                    )
                return factory

            # TsdMap builder (extra args: multiplexed_args, key_arg, key_tp)
            def _create_tsd_map_builder_factory(
                signature, scalars, input_builder, output_builder, error_builder,
                recordable_state_builder, nested_graph, input_node_ids, output_node_id,
                multiplexed_args, key_arg, key_tp,
            ):
                input_node_ids = dict(input_node_ids) if input_node_ids is not None else {}
                output_node_id = -1 if output_node_id is None else int(output_node_id)
                multiplexed_args = set(multiplexed_args) if multiplexed_args is not None else set()
                return _hgraph.TsdMapNodeBuilder(
                    signature, scalars, input_builder, output_builder, error_builder,
                    recordable_state_builder, nested_graph, input_node_ids, output_node_id,
                    multiplexed_args, "" if key_arg is None else key_arg,
                )

            # Reduce builder (no recordable_state_builder param)
            def _create_reduce_builder_factory(
                signature, scalars, input_builder, output_builder, error_builder,
                nested_graph, input_node_ids, output_node_id,
            ):
                return _hgraph.ReduceNodeBuilder(
                    signature, scalars, input_builder, output_builder, error_builder,
                    None, nested_graph, input_node_ids, output_node_id,
                )

            # Switch builder (extra args: nested_graphs, reload_on_ticked, key type schema)
            def _create_switch_builder_factory(
                signature, scalars, input_builder, output_builder, error_builder,
                nested_graphs, input_node_ids, output_node_id,
                reload_on_ticked, recordable_state_builder=None,
            ):
                switch_input_type = signature.time_series_inputs["key"]
                key_type_schema = _get_value_schema_for_scalar_type(switch_input_type.value_scalar_tp)
                return _hgraph.SwitchNodeBuilder(
                    signature, scalars, input_builder, output_builder, error_builder,
                    recordable_state_builder, key_type_schema, nested_graphs,
                    input_node_ids, output_node_id, reload_on_ticked,
                )

            # Mesh builder (extra args: multiplexed_args, key_arg, key_tp, context_path)
            def _create_mesh_builder_factory(
                signature, scalars, input_builder, output_builder, error_builder,
                recordable_state_builder, nested_graph, input_node_ids, output_node_id,
                multiplexed_args, key_arg, key_tp, context_path,
            ):
                return _hgraph.MeshNodeBuilder(
                    signature, scalars, input_builder, output_builder, error_builder,
                    recordable_state_builder, nested_graph, input_node_ids, output_node_id,
                    multiplexed_args, "" if key_arg is None else key_arg, context_path,
                )

            # Push queue builder (keyword-only, different signature)
            def _create_push_queue_builder(
                *, signature, scalars, input_builder, output_builder, error_builder, eval_fn, **kwargs
            ):
                return _hgraph.PythonNodeBuilder(
                    signature=signature, scalars=scalars, input_builder=input_builder,
                    output_builder=output_builder, error_builder=error_builder,
                    recordable_state_builder=None, eval_fn=eval_fn, start_fn=None, stop_fn=None,
                )

            # Context node builder (keyword-only, simpler signature)
            def _create_context_builder(
                *, signature, scalars, input_builder, output_builder, error_builder,
                recordable_state_builder=None, **kwargs,
            ):
                return _hgraph.ContextNodeBuilder(
                    signature=signature, scalars=scalars, input_builder=input_builder,
                    output_builder=output_builder, error_builder=error_builder,
                    recordable_state_builder=recordable_state_builder,
                )

            # --- Register all builder classes ---

            hgraph._wiring._wiring_node_class.TsdMapWiringNodeClass.BUILDER_CLASS = _create_tsd_map_builder_factory
            hgraph._wiring._wiring_node_class.TsdReduceWiringNodeClass.BUILDER_CLASS = _create_reduce_builder_factory

            hgraph._wiring._wiring_node_class._component_node_class.ComponentNodeClass.BUILDER_CLASS = (
                _make_nested_factory(_hgraph.ComponentNodeBuilder)
            )

            hgraph._wiring._wiring_node_class.SwitchWiringNodeClass.BUILDER_CLASS = _create_switch_builder_factory

            hgraph._wiring._wiring_node_class.TryExceptWiringNodeClass.BUILDER_CLASS = (
                _make_nested_factory(_hgraph.TryExceptNodeBuilder, normalize_ids=True)
            )

            hgraph._wiring._wiring_node_class._reduce_wiring_node.TsdNonAssociativeReduceWiringNodeClass.BUILDER_CLASS = (
                _make_nested_factory(_hgraph.TsdNonAssociativeReduceNodeBuilder)
            )

            from hgraph._wiring._wiring_node_class import _nested_graph_wiring_node as _ng
            _ng.NestedGraphWiringNodeClass.BUILDER_CLASS = _make_nested_factory(
                _hgraph.NestedGraphNodeBuilder, normalize_ids=True
            )

            hgraph._wiring._wiring_node_class.MeshWiringNodeClass.BUILDER_CLASS = _create_mesh_builder_factory

            # TimeSeriesReferenceOutput virtual subclass registration
            from hgraph._types._ref_type import TimeSeriesReferenceOutput as _TSRO
            _TSRO.register(_hgraph.TimeSeriesReferenceOutput)

            # Service/adaptor builders share the nested graph builder with ID normalization
            _service_builder = _make_nested_factory(_hgraph.NestedGraphNodeBuilder, normalize_ids=True)
            hgraph._wiring._wiring_node_class._service_impl_node_class.ServiceImplNodeClass.BUILDER_CLASS = (
                _service_builder
            )
            from hgraph._wiring._wiring_node_class import _adaptor_impl_node_class as _ai
            from hgraph._wiring._wiring_node_class import _service_adaptor_impl_node_class as _sai
            _ai.AdaptorImplNodeClass.BUILDER_CLASS = _service_builder
            _sai.ServiceAdaptorImplNodeClass.BUILDER_CLASS = _service_builder

            from hgraph._wiring._wiring_node_class import _python_wiring_node_classes as _pwc
            _pwc.PythonPushQueueWiringNodeClass.BUILDER_CLASS = _create_push_queue_builder

            hgraph._wiring._wiring_node_class._pull_source_node_class.PythonLastValuePullWiringNodeClass.BUILDER_CLASS = (
                _hgraph.LastValuePullNodeBuilder
            )

            hgraph._wiring._context_wiring.ContextNodeClass.BUILDER_CLASS = _create_context_builder


        def _setup_observers():
            """Register C++ observer implementations."""
            from hgraph.test._cpp_observers import setup_cpp_observers
            setup_cpp_observers()


        # --- Execute all setup ---
        _setup_constants()
        _setup_graph_builders()
        _setup_ts_builder_factory()
        _setup_node_builders()
        _setup_observers()

    except ImportError as e:
        import warnings

        warnings.warn(
            f"C++ runtime feature 'use_cpp' is enabled but _hgraph module could not be imported: {e}. "
            "Falling back to Python runtime.",
            RuntimeWarning,
        )
