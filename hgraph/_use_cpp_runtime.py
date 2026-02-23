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
        import hgraph._hgraph as _hgraph
        import hgraph

        # Replace Python date/time constants with C++ versions
        hgraph.MIN_DT = _hgraph.MIN_DT
        hgraph.MAX_DT = _hgraph.MAX_DT
        hgraph.MIN_ST = _hgraph.MIN_ST
        hgraph.MAX_ET = _hgraph.MAX_ET
        hgraph.MIN_TD = _hgraph.MIN_TD

        # Replace core type enums with C++ versions
        hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_SIGNATURE = _hgraph.NodeSignature
        hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_TYPE_ENUM = _hgraph.NodeTypeEnum
        hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.INJECTABLE_TYPES_ENUM = (
            _hgraph.InjectableTypesEnum
        )

        hgraph._runtime._evaluation_engine.EvaluationMode = _hgraph.EvaluationMode
        hgraph._runtime._evaluation_engine.EvaluationLifeCycleObserver = _hgraph.EvaluationLifeCycleObserver

        # TimeSeriesReference builders
        hgraph.TimeSeriesReference._BUILDER = _hgraph.TimeSeriesReference.make
        hgraph.TimeSeriesReference._INSTANCE_OF = lambda obj: isinstance(obj, _hgraph.TimeSeriesReference)

        # Edge type
        hgraph._builder._graph_builder.EDGE_TYPE = _hgraph.Edge


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


        # Register C++ GraphBuilder as a virtual subclass of Python GraphBuilder
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


        # === TimeSeriesBuilderFactory ===


        class HgCppFactory(hgraph.TimeSeriesBuilderFactory):
            """
            Node endpoint schemas are now inferred from NodeSignature in C++.

            C++ node builders keep InputBuilder/OutputBuilder constructor slots for
            ABI compatibility, but they no longer construct runtime endpoints from
            these builders. Return None to keep legacy BaseTimeSeries builders out
            of active runtime paths.
            """

            def make_error_builder(self, value_tp):
                return None

            def make_input_builder(self, value_tp):
                return None

            def make_output_builder(self, value_tp):
                return None


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

        # Register the TimeSeriesBuilderFactory
        hgraph.TimeSeriesBuilderFactory.declare(HgCppFactory())

        # === Node Builder Classes ===

        hgraph._wiring._wiring_node_class.PythonWiringNodeClass.BUILDER_CLASS = _hgraph.PythonNodeBuilder
        hgraph._wiring._wiring_node_class.PythonGeneratorWiringNodeClass.BUILDER_CLASS = (
            _hgraph.PythonGeneratorNodeBuilder
        )


        def _create_set_delta(added, removed, tp):
            # Use PythonSetDelta since C++ SetDelta templates are no longer available
            from hgraph._impl._types._tss import PythonSetDelta
            return PythonSetDelta[tp](
                added=frozenset() if added is None else frozenset(added),
                removed=frozenset() if removed is None else frozenset(removed)
            )


        hgraph.set_set_delta_factory(_create_set_delta)


        def _create_tsd_map_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder,
            nested_graph,
            input_node_ids,
            output_node_id,
            multiplexed_args,
            key_arg,
            key_tp,
        ):
            input_node_ids = dict(input_node_ids) if input_node_ids is not None else {}
            output_node_id = -1 if output_node_id is None else int(output_node_id)
            multiplexed_args = set(multiplexed_args) if multiplexed_args is not None else set()
            # Non-templated TsdMapNodeBuilder - key type is handled dynamically
            return _hgraph.TsdMapNodeBuilder(
                signature,
                scalars,
                nested_graph,
                input_node_ids,
                output_node_id,
                multiplexed_args,
                "" if key_arg is None else key_arg,
            )


        hgraph._wiring._wiring_node_class.TsdMapWiringNodeClass.BUILDER_CLASS = _create_tsd_map_builder_factory


        def _create_reduce_node_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            nested_graph,
            input_node_ids,
            output_node_id,
        ):
            # Non-templated ReduceNodeBuilder - key type is handled dynamically via TSD input
            return _hgraph.ReduceNodeBuilder(
                signature,
                scalars,
                nested_graph,
                input_node_ids,
                output_node_id,
            )


        hgraph._wiring._wiring_node_class.TsdReduceWiringNodeClass.BUILDER_CLASS = _create_reduce_node_builder_factory


        def _create_component_node_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder,
            nested_graph,
            input_node_ids,
            output_node_id,
        ):
            return _hgraph.ComponentNodeBuilder(
                signature,
                scalars,
                nested_graph,
                input_node_ids,
                output_node_id,
            )


        hgraph._wiring._wiring_node_class._component_node_class.ComponentNodeClass.BUILDER_CLASS = (
            _create_component_node_builder_factory
        )


        def _create_switch_node_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            nested_graphs,
            input_node_ids,
            output_node_id,
            reload_on_ticked,
            recordable_state_builder=None,
        ):
            # Get key type schema for Value-based key storage
            switch_input_type = signature.time_series_inputs["key"]
            key_type_schema = _get_value_schema_for_scalar_type(switch_input_type.value_scalar_tp)
            return _hgraph.SwitchNodeBuilder(
                signature,
                scalars,
                key_type_schema,
                nested_graphs,
                input_node_ids,
                output_node_id,
                reload_on_ticked,
            )


        hgraph._wiring._wiring_node_class.SwitchWiringNodeClass.BUILDER_CLASS = _create_switch_node_builder_factory


        def _create_try_except_node_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder=None,
            nested_graph=None,
            input_node_ids=None,
            output_node_id=0,
        ):
            input_node_ids = dict(input_node_ids) if input_node_ids is not None else {}
            output_node_id = -1 if output_node_id in (None, 0) else int(output_node_id)
            return _hgraph.TryExceptNodeBuilder(
                signature,
                scalars,
                nested_graph,
                input_node_ids,
                output_node_id,
            )


        hgraph._wiring._wiring_node_class.TryExceptWiringNodeClass.BUILDER_CLASS = (
            _create_try_except_node_builder_factory
        )


        def _create_tsd_non_associative_reduce_node_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder=None,
            nested_graph=None,
            input_node_ids=None,
            output_node_id=0,
            context_path=None,
        ):
            return _hgraph.TsdNonAssociativeReduceNodeBuilder(
                signature,
                scalars,
                nested_graph,
                input_node_ids,
                output_node_id,
            )


        hgraph._wiring._wiring_node_class._reduce_wiring_node.TsdNonAssociativeReduceWiringNodeClass.BUILDER_CLASS = (
            _create_tsd_non_associative_reduce_node_builder_factory
        )


        def _create_nested_graph_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder=None,
            nested_graph=None,
            input_node_ids=None,
            output_node_id=None,
        ):
            input_node_ids = dict(input_node_ids) if input_node_ids is not None else {}
            output_node_id = -1 if output_node_id in (None, 0) else int(output_node_id)
            return _hgraph.NestedGraphNodeBuilder(
                signature,
                scalars,
                nested_graph,
                input_node_ids,
                output_node_id,
            )


        from hgraph._wiring._wiring_node_class import _nested_graph_wiring_node as _ng

        _ng.NestedGraphWiringNodeClass.BUILDER_CLASS = _create_nested_graph_builder_factory


        def _create_mesh_node_builder_factory(
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder,
            nested_graph,
            input_node_ids,
            output_node_id,
            multiplexed_args,
            key_arg,
            key_tp,
            context_path,
        ):
            # Non-templated MeshNodeBuilder - key type is handled dynamically via keys input
            return _hgraph.MeshNodeBuilder(
                signature,
                scalars,
                nested_graph,
                input_node_ids,
                output_node_id,
                multiplexed_args,
                "" if key_arg is None else key_arg,
                context_path,
            )


        hgraph._wiring._wiring_node_class.MeshWiringNodeClass.BUILDER_CLASS = _create_mesh_node_builder_factory

        # Register C++ TimeSeriesReferenceOutput as virtual subclass
        from hgraph._types._ref_type import TimeSeriesReferenceOutput as _TSRO

        _TSRO.register(_hgraph.TimeSeriesReferenceOutput)


        def _service_impl_nested_graph_builder(
            *,
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder,
            nested_graph,
        ):
            return _hgraph.NestedGraphNodeBuilder(
                signature,
                scalars,
                nested_graph,
                {},
                -1,
            )


        hgraph._wiring._wiring_node_class._service_impl_node_class.ServiceImplNodeClass.BUILDER_CLASS = (
            _service_impl_nested_graph_builder
        )

        from hgraph._wiring._wiring_node_class import _adaptor_impl_node_class as _ai
        from hgraph._wiring._wiring_node_class import _service_adaptor_impl_node_class as _sai

        _ai.AdaptorImplNodeClass.BUILDER_CLASS = _service_impl_nested_graph_builder
        _sai.ServiceAdaptorImplNodeClass.BUILDER_CLASS = _service_impl_nested_graph_builder


        def _create_python_push_queue_builder(
            *, signature, scalars, input_builder, output_builder, error_builder, eval_fn, **kwargs
        ):
            return _hgraph.PythonNodeBuilder(
                signature=signature,
                scalars=scalars,
                eval_fn=eval_fn,
                start_fn=None,
                stop_fn=None,
            )


        from hgraph._wiring._wiring_node_class import _python_wiring_node_classes as _pwc

        _pwc.PythonPushQueueWiringNodeClass.BUILDER_CLASS = _create_python_push_queue_builder

        hgraph._wiring._wiring_node_class._pull_source_node_class.PythonLastValuePullWiringNodeClass.BUILDER_CLASS = (
            _hgraph.LastValuePullNodeBuilder
        )


        def _context_node_builder(
            *,
            signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            recordable_state_builder=None,
            **kwargs,
        ):
            return _hgraph.ContextNodeBuilder(
                signature=signature,
                scalars=scalars,
            )


        hgraph._wiring._context_wiring.ContextNodeClass.BUILDER_CLASS = _context_node_builder
        
        # Setup C++ observer implementations
        from hgraph.test._cpp_observers import setup_cpp_observers
        setup_cpp_observers()

    except ImportError as e:
        import warnings

        warnings.warn(
            f"C++ runtime feature 'use_cpp' is enabled but _hgraph module could not be imported: {e}. "
            "Falling back to Python runtime.",
            RuntimeWarning,
        )
