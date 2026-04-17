"""
C++ runtime integration.

This module wires the extension runtime surface into Python. Legacy C++
builders, runtime types, and observers are intentionally not referenced here.
"""

from hgraph._feature_switch import is_feature_enabled

if is_feature_enabled("use_cpp"):
    print("\n>>>>>>>>>>>>>>>>>>>\nC++ Runtime enabled\n<<<<<<<<<<<<<<<<<<<")
    try:
        import warnings

        import hgraph
        import hgraph._hgraph as _hgraph

        from hgraph._wiring import _context_wiring
        from hgraph._operators import _debug_tools as _debug_tools
        from hgraph._operators import _graph_operators as _graph_operators
        from hgraph._impl._operators import _tsd_operators as _tsd_operators
        from hgraph._operators import _time_series_conversion as _ts_conversion
        from hgraph._wiring._wiring_node_class import _component_node_class as _component
        from hgraph._wiring._wiring_node_class import _map_wiring_node as _map
        from hgraph._wiring._wiring_node_class import _mesh_wiring_node as _mesh
        from hgraph._wiring._wiring_node_class import _nested_graph_wiring_node as _ng
        from hgraph._wiring._wiring_node_class import _pull_source_node_class as _pull_source
        from hgraph._wiring._wiring_node_class import _python_wiring_node_classes as _pwc
        from hgraph._wiring._wiring_node_class import _reduce_wiring_node as _reduce
        from hgraph._wiring._wiring_node_class import _service_impl_node_class as _service_impl
        from hgraph._wiring._wiring_node_class import _switch_wiring_node as _switch

        hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_SIGNATURE = _hgraph.NodeSignature
        hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_TYPE_ENUM = _hgraph.NodeTypeEnum
        hgraph._runtime._evaluation_engine.EvaluationMode = _hgraph.EvaluationMode

        hgraph.TimeSeriesReference._BUILDER = _hgraph.TimeSeriesReference.make
        hgraph.TimeSeriesReference._INSTANCE_OF = lambda obj: isinstance(obj, _hgraph.TimeSeriesReference)

        hgraph._builder._graph_builder.EDGE_TYPE = _hgraph.Edge
        hgraph._builder._graph_builder.GraphBuilder.register(_hgraph.GraphBuilder)
        hgraph._builder._node_builder.NodeBuilder.register(_hgraph.NodeBuilder)

        hgraph.const = _hgraph.const
        hgraph.debug_print = _hgraph.debug_print
        hgraph.nothing = _hgraph.nothing
        hgraph.null_sink = _hgraph.null_sink
        _ts_conversion.const = _hgraph.const
        _debug_tools.debug_print = _hgraph.debug_print
        _graph_operators.nothing = _hgraph.nothing
        _graph_operators.null_sink = _hgraph.null_sink
        _tsd_operators.tsd_get_items = _hgraph.tsd_get_items
        _tsd_operators.tsd_get_item_default = _hgraph.tsd_get_item_default

        def _unsupported_cpp_builder(feature: str):
            def _raise(*args, **kwargs):
                raise NotImplementedError(
                    f"{feature} is not implemented in the C++ runtime. "
                    "Use HGRAPH_USE_CPP=0 for the Python runtime or implement the missing builder."
                )

            return _raise

        def _make_cpp_graph_builder(node_builders, edges):
            builders = tuple(node_builders)
            graph_edges = tuple(edges)
            if all(isinstance(builder, _hgraph.NodeBuilder) for builder in builders):
                return _hgraph.GraphBuilder(list(builders), list(graph_edges))

            unsupported = sorted({type(builder).__name__ for builder in builders if not isinstance(builder, _hgraph.NodeBuilder)})
            raise NotImplementedError(
                "Graph contains non-runtime node builders in C++ mode: "
                f"{unsupported}. Use HGRAPH_USE_CPP=0 or implement the missing builders."
            )

        def _make_cpp_graph_engine(graph_builder, run_mode, observers, cleanup_on_error=True):
            if isinstance(graph_builder, _hgraph.GraphBuilder):
                return _hgraph.GraphExecutor(
                    graph_builder=graph_builder,
                    run_mode=run_mode,
                    observers=list(observers or ()),
                    cleanup_on_error=cleanup_on_error,
                )
            raise NotImplementedError(
                "Only _hgraph.GraphBuilder is supported in C++ mode. "
                "Use HGRAPH_USE_CPP=0 for the Python runtime."
            )

        hgraph.GraphBuilderFactory.declare(_make_cpp_graph_builder)
        hgraph.GraphEngineFactory.declare(_make_cpp_graph_engine)

        def _create_set_delta(added, removed, tp):
            from hgraph._impl._types._tss import PythonSetDelta

            return PythonSetDelta[tp](
                added=frozenset() if added is None else frozenset(added),
                removed=frozenset() if removed is None else frozenset(removed),
            )

        hgraph.set_set_delta_factory(_create_set_delta)

        hgraph._wiring._wiring_node_class.PythonWiringNodeClass.BUILDER_CLASS = _hgraph.NodeBuilder
        hgraph._wiring._wiring_node_class.PythonGeneratorWiringNodeClass.BUILDER_CLASS = _hgraph.NodeBuilder
        _pwc.PythonPushQueueWiringNodeClass.BUILDER_CLASS = _hgraph.NodeBuilder

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

            if isinstance(nested_graph, _hgraph.GraphBuilder):
                return _hgraph.build_nested_node(
                    signature,
                    scalars,
                    input_builder,
                    output_builder,
                    error_builder,
                    nested_graph,
                    input_node_ids,
                    output_node_id,
                    True,
                )

            raise NotImplementedError(
                "try_except requires a nested graph builder in C++ mode. "
                "Use HGRAPH_USE_CPP=0 for the Python runtime or implement the missing builder."
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

            if isinstance(nested_graph, _hgraph.GraphBuilder):
                return _hgraph.build_nested_node(
                    signature,
                    scalars,
                    input_builder,
                    output_builder,
                    error_builder,
                    nested_graph,
                    input_node_ids,
                    output_node_id,
                    False,
                )

            raise NotImplementedError(
                "nested graph nodes require a nested graph builder in C++ mode. "
                "Use HGRAPH_USE_CPP=0 for the Python runtime or implement the missing builder."
            )

        hgraph._wiring._wiring_node_class.TryExceptWiringNodeClass.BUILDER_CLASS = (
            _create_try_except_node_builder_factory
        )
        _ng.NestedGraphWiringNodeClass.BUILDER_CLASS = _create_nested_graph_builder_factory
        _pull_source.PythonLastValuePullWiringNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("last_value_source_node")
        _context_wiring.ContextNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("context output nodes")
        _component.ComponentNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("component nodes")
        _map.TsdMapWiringNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("map_ / TSD map nodes")
        _reduce.TsdReduceWiringNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("reduce nodes")
        _reduce.TsdNonAssociativeReduceWiringNodeClass.BUILDER_CLASS = _unsupported_cpp_builder(
            "non-associative reduce nodes"
        )
        _mesh.MeshWiringNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("mesh nodes")
        _switch.SwitchWiringNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("switch nodes")
        _service_impl.ServiceImplNodeClass.BUILDER_CLASS = _unsupported_cpp_builder("service implementation nodes")

    except ImportError as e:
        warnings.warn(
            f"C++ runtime feature 'use_cpp' is enabled but _hgraph module could not be imported: {e}. "
            "Falling back to Python runtime.",
            RuntimeWarning,
        )
