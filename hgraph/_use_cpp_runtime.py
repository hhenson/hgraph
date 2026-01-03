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


        def _raise_un_implemented(value_tp):
            raise NotImplementedError(f"Missing builder for {value_tp}")


        class HgCppFactory(hgraph.TimeSeriesBuilderFactory):
            def make_error_builder(self, value_tp):
                return self.make_output_builder(value_tp)

            def _make_tsw_input_builder(self, value_tp):
                return _tsw_input_builder_for(value_tp.value_scalar_tp)

            def make_input_builder(self, value_tp):
                return {
                    hgraph.HgSignalMetaData: lambda: _hgraph.InputBuilder_TS_Signal(),
                    hgraph.HgTSTypeMetaData: lambda: _ts_input_builder_type_for(value_tp.value_scalar_tp)(),
                    hgraph.HgTSWTypeMetaData: lambda: self._make_tsw_input_builder(value_tp),
                    hgraph.HgTSLTypeMetaData: lambda: _hgraph.InputBuilder_TSL(
                        self.make_input_builder(value_tp.value_tp), value_tp.size_tp.py_type.SIZE
                    ),
                    hgraph.HgTSBTypeMetaData: lambda: _hgraph.InputBuilder_TSB(
                        _hgraph.TimeSeriesSchema(
                            keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys()), scalar_type=tp
                        )
                        if (tp := value_tp.bundle_schema_tp.py_type.scalar_type())
                        else _hgraph.TimeSeriesSchema(keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys())),
                        [self.make_input_builder(tp) for tp in value_tp.bundle_schema_tp.meta_data_schema.values()],
                    ),
                    hgraph.HgREFTypeMetaData: lambda: self._make_ref_input_builder(value_tp),
                    hgraph.HgTSSTypeMetaData: lambda: _tss_input_builder_type_for(value_tp.value_scalar_tp)(),
                    hgraph.HgTSDTypeMetaData: lambda: _tsd_input_builder_type_for(value_tp.key_tp)(
                        self.make_input_builder(value_tp.value_tp),
                    ),
                    hgraph.HgCONTEXTTypeMetaData: lambda: self.make_input_builder(value_tp.ts_type),
                }.get(type(value_tp), lambda: hgraph._impl._builder._ts_builder._throw(value_tp))()

            def _make_tsw_output_builder(self, value_tp):
                time_range = getattr(value_tp.size_tp.py_type, "TIME_RANGE", None)
                is_time_based = time_range is not None
                if is_time_based:
                    return _ttsw_output_builder_for(
                        value_tp.value_scalar_tp,
                        value_tp.size_tp.py_type.TIME_RANGE,
                        value_tp.min_size_tp.py_type.TIME_RANGE
                        if getattr(value_tp.min_size_tp.py_type, "TIME_RANGE", None)
                        else value_tp.min_size_tp.py_type.TIME_RANGE,
                    )
                else:
                    return _tsw_output_builder_for(
                        value_tp.value_scalar_tp,
                        value_tp.size_tp.py_type.SIZE,
                        value_tp.min_size_tp.py_type.SIZE
                        if getattr(value_tp.min_size_tp.py_type, "FIXED_SIZE", True)
                        else 0,
                    )

            def make_output_builder(self, value_tp):
                return {
                    hgraph.HgTSTypeMetaData: lambda: _ts_output_builder_for_tp(value_tp.value_scalar_tp)(),
                    hgraph.HgTSLTypeMetaData: lambda: _hgraph.OutputBuilder_TSL(
                        self.make_output_builder(value_tp.value_tp), value_tp.size_tp.py_type.SIZE
                    ),
                    hgraph.HgTSBTypeMetaData: lambda: _hgraph.OutputBuilder_TSB(
                        schema=_hgraph.TimeSeriesSchema(
                            keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys()), scalar_type=tp
                        )
                        if (tp := value_tp.bundle_schema_tp.py_type.scalar_type())
                        else _hgraph.TimeSeriesSchema(keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys())),
                        output_builders=[
                            self.make_output_builder(tp) for tp in value_tp.bundle_schema_tp.meta_data_schema.values()
                        ],
                    ),
                    hgraph.HgREFTypeMetaData: lambda: self._make_ref_output_builder(value_tp),
                    hgraph.HgTSSTypeMetaData: lambda: _tss_output_builder_for_tp(value_tp.value_scalar_tp)(),
                    hgraph.HgTSDTypeMetaData: lambda: _tsd_output_builder_for_tp(value_tp.key_tp)(
                        self.make_output_builder(value_tp.value_tp),
                        self.make_output_builder(value_tp.value_tp.as_reference()),
                    ),
                    hgraph.HgTSWTypeMetaData: lambda: self._make_tsw_output_builder(value_tp),
                }.get(type(value_tp), lambda: hgraph._impl._builder._ts_builder._throw(value_tp))()
            
            def _make_ref_input_builder(self, ref_tp):
                """Create specialized C++ reference input builder based on what's being referenced"""
                referenced_tp = ref_tp.value_tp
                
                def _make_child_ref_builder(child_tp):
                    """Wrap child type in REF if not already a REF type"""
                    if type(child_tp) is hgraph.HgREFTypeMetaData:
                        # Already a reference type, use its builder directly
                        return self._make_ref_input_builder(child_tp)
                    else:
                        # Wrap in REF
                        child_ref_tp = hgraph.HgREFTypeMetaData(child_tp)
                        return self._make_ref_input_builder(child_ref_tp)
                
                def _make_ts_value_ref():
                    """REF[TS[...]] - value/scalar reference"""
                    return _hgraph.InputBuilder_TS_Value_Ref()
                
                def _make_tsl_ref():
                    """REF[TSL[...]] - list reference"""
                    return _hgraph.InputBuilder_TSL_Ref(
                        _make_child_ref_builder(referenced_tp.value_tp),
                        referenced_tp.size_tp.py_type.SIZE
                    )
                
                def _make_tsb_ref():
                    """REF[TSB[...]] - bundle reference"""
                    return _hgraph.InputBuilder_TSB_Ref(
                        _hgraph.TimeSeriesSchema(
                            tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                            tp
                        ) if (tp := referenced_tp.bundle_schema_tp.py_type.scalar_type()) is not None
                        else _hgraph.TimeSeriesSchema(tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys())),
                        [_make_child_ref_builder(tp) for tp in referenced_tp.bundle_schema_tp.meta_data_schema.values()]
                    )
                
                def _make_tsd_ref():
                    """REF[TSD[...]] - dict reference"""
                    return _hgraph.InputBuilder_TSD_Ref()
                
                def _make_tss_ref():
                    """REF[TSS[...]] - set reference"""
                    return _hgraph.InputBuilder_TSS_Ref()
                
                def _make_tsw_ref():
                    """REF[TSW[...]] - window reference"""
                    return _hgraph.InputBuilder_TSW_Ref()
                
                # Use dictionary lookup for type-based dispatch (matching Python pattern)
                return {
                    hgraph.HgTSTypeMetaData: _make_ts_value_ref,
                    hgraph.HgTSLTypeMetaData: _make_tsl_ref,
                    hgraph.HgTSBTypeMetaData: _make_tsb_ref,
                    hgraph.HgTSDTypeMetaData: _make_tsd_ref,
                    hgraph.HgTSSTypeMetaData: _make_tss_ref,
                    hgraph.HgTSWTypeMetaData: _make_tsw_ref,
                }.get(type(referenced_tp), _make_ts_value_ref)()
            
            def _make_ref_output_builder(self, ref_tp):
                """Create specialized C++ reference output builder based on what's being referenced"""
                referenced_tp = ref_tp.value_tp
                
                def _make_child_ref_builder(child_tp):
                    """Wrap child type in REF if not already a REF type"""
                    if type(child_tp) is hgraph.HgREFTypeMetaData:
                        # Already a reference type, use its builder directly
                        return self._make_ref_output_builder(child_tp)
                    else:
                        # Wrap in REF
                        child_ref_tp = hgraph.HgREFTypeMetaData(child_tp)
                        return self._make_ref_output_builder(child_ref_tp)
                
                def _make_ts_value_ref():
                    """REF[TS[...]] - value/scalar reference"""
                    return _hgraph.OutputBuilder_TS_Value_Ref()
                
                def _make_tsl_ref():
                    """REF[TSL[...]] - list reference"""
                    return _hgraph.OutputBuilder_TSL_Ref(
                        _make_child_ref_builder(referenced_tp.value_tp),
                        referenced_tp.size_tp.py_type.SIZE
                    )
                
                def _make_tsb_ref():
                    """REF[TSB[...]] - bundle reference"""
                    return _hgraph.OutputBuilder_TSB_Ref(
                        _hgraph.TimeSeriesSchema(
                            tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                            tp
                        ) if (tp := referenced_tp.bundle_schema_tp.py_type.scalar_type()) is not None
                        else _hgraph.TimeSeriesSchema(tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys())),
                        [_make_child_ref_builder(tp) for tp in referenced_tp.bundle_schema_tp.meta_data_schema.values()]
                    )
                
                def _make_tsd_ref():
                    """REF[TSD[...]] - dict reference"""
                    return _hgraph.OutputBuilder_TSD_Ref()
                
                def _make_tss_ref():
                    """REF[TSS[...]] - set reference"""
                    return _hgraph.OutputBuilder_TSS_Ref()
                
                def _make_tsw_ref():
                    """REF[TSW[...]] - window reference"""
                    return _hgraph.OutputBuilder_TSW_Ref()
                
                # Use dictionary lookup for type-based dispatch (matching Python pattern)
                return {
                    hgraph.HgTSTypeMetaData: _make_ts_value_ref,
                    hgraph.HgTSLTypeMetaData: _make_tsl_ref,
                    hgraph.HgTSBTypeMetaData: _make_tsb_ref,
                    hgraph.HgTSDTypeMetaData: _make_tsd_ref,
                    hgraph.HgTSSTypeMetaData: _make_tss_ref,
                    hgraph.HgTSWTypeMetaData: _make_tsw_ref,
                }.get(type(referenced_tp), _make_ts_value_ref)()


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

        def _ts_input_builder_type_for(scalar_type):
            """Return factory for non-templated TimeSeriesValueInput builder."""
            # Return callable to match old factory pattern
            return _hgraph.InputBuilder_TS_Value


        def _ts_output_builder_for_tp(scalar_type):
            """Return factory for non-templated TimeSeriesValueOutput builder with schema."""
            schema = _get_value_schema_for_scalar_type(scalar_type)
            # Return callable to match old factory pattern
            return lambda: _hgraph.OutputBuilder_TS_Value(schema)


        def _tss_input_builder_type_for(scalar_type):
            """Return factory for non-templated TimeSeriesSetInput builder with element schema."""
            schema = _get_value_schema_for_scalar_type(scalar_type)
            return lambda: _hgraph.InputBuilder_TSS(schema)


        def _tss_output_builder_for_tp(scalar_type):
            """Return factory for non-templated TimeSeriesSetOutput builder with element schema."""
            schema = _get_value_schema_for_scalar_type(scalar_type)
            return lambda: _hgraph.OutputBuilder_TSS(schema)


        def _tsd_input_builder_type_for(key_scalar_type):
            """Return factory for non-templated TimeSeriesDictInput builder with key TypeMeta."""
            key_schema = _get_value_schema_for_scalar_type(key_scalar_type)
            return lambda ts_builder: _hgraph.InputBuilder_TSD(ts_builder, key_schema)


        def _tsd_output_builder_for_tp(key_scalar_type):
            """Return factory for non-templated TimeSeriesDictOutput builder with key TypeMeta."""
            key_schema = _get_value_schema_for_scalar_type(key_scalar_type)
            return lambda ts_builder, ts_ref_builder: _hgraph.OutputBuilder_TSD(ts_builder, ts_ref_builder, key_schema)


        def _tsw_input_builder_for(scalar_type):
            """Non-templated TSW input builder - uses TypeMeta for element type"""
            element_schema = _get_value_schema_for_scalar_type(scalar_type)
            return _hgraph.InputBuilder_TSW(element_schema)


        def _tsw_output_builder_for(scalar_type, size: int, min_size: int):
            """Non-templated fixed-size TSW output builder - uses TypeMeta for element type"""
            element_schema = _get_value_schema_for_scalar_type(scalar_type)
            return _hgraph.OutputBuilder_TSW(size, min_size, element_schema)


        def _ttsw_output_builder_for(scalar_type, size: timedelta, min_size: timedelta):
            """Non-templated time-based TSW output builder - uses TypeMeta for element type"""
            element_schema = _get_value_schema_for_scalar_type(scalar_type)
            return _hgraph.OutputBuilder_TTSW(size, min_size, element_schema)


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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder,
                output_builder,
                error_builder,
                None,
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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
                input_builder=input_builder,
                output_builder=output_builder,
                error_builder=error_builder,
                recordable_state_builder=None,
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
                input_builder=input_builder,
                output_builder=output_builder,
                error_builder=error_builder,
                recordable_state_builder=recordable_state_builder,
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
