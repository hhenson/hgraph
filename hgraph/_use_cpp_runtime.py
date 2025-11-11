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

        # Register the graph engine via a thin Python wrapper that coerces args
        class _PyCppGraphExecutor:
            def __init__(self, graph, run_mode, observers=None):
                # Coerce run_mode from Python enum to C++ enum reliably
                def _to_cpp_mode(mode):
                    try:
                        if hasattr(mode, "name"):
                            return _hgraph.EvaluationMode[mode.name]
                        return _hgraph.EvaluationMode(int(mode))
                    except Exception:
                        return mode

                # Adapter to wrap Python observers so they are acceptable to C++ nanobind type
                class _ObserverAdapter(_hgraph.EvaluationLifeCycleObserver):
                    def __init__(self, delegate):
                        # Properly initialize nanobind intrusive base
                        _hgraph.EvaluationLifeCycleObserver.__init__(self)
                        self._d = delegate

                    def on_before_start_graph(self, graph):
                        if hasattr(self._d, "on_before_start_graph"):
                            self._d.on_before_start_graph(graph)

                    def on_after_start_graph(self, graph):
                        if hasattr(self._d, "on_after_start_graph"):
                            self._d.on_after_start_graph(graph)

                    def on_before_start_node(self, node):
                        if hasattr(self._d, "on_before_start_node"):
                            self._d.on_before_start_node(node)

                    def on_after_start_node(self, node):
                        if hasattr(self._d, "on_after_start_node"):
                            self._d.on_after_start_node(node)

                    def on_before_graph_evaluation(self, graph):
                        if hasattr(self._d, "on_before_graph_evaluation"):
                            self._d.on_before_graph_evaluation(graph)

                    def on_after_graph_evaluation(self, graph):
                        if hasattr(self._d, "on_after_graph_evaluation"):
                            self._d.on_after_graph_evaluation(graph)

                    def on_before_node_evaluation(self, node):
                        if hasattr(self._d, "on_before_node_evaluation"):
                            self._d.on_before_node_evaluation(node)

                    def on_after_node_evaluation(self, node):
                        if hasattr(self._d, "on_after_node_evaluation"):
                            self._d.on_after_node_evaluation(node)

                    def on_before_stop_node(self, node):
                        if hasattr(self._d, "on_before_stop_node"):
                            self._d.on_before_stop_node(node)

                    def on_after_stop_node(self, node):
                        if hasattr(self._d, "on_after_stop_node"):
                            self._d.on_after_stop_node(node)

                    def on_before_stop_graph(self, graph):
                        if hasattr(self._d, "on_before_stop_graph"):
                            self._d.on_before_stop_graph(graph)

                    def on_after_stop_graph(self, graph):
                        if hasattr(self._d, "on_after_stop_graph"):
                            self._d.on_after_stop_graph(graph)

                cpp_mode = _to_cpp_mode(run_mode)
                # NOTE: Passing observers across the Python/C++ boundary is currently unstable.
                # To avoid constructor mismatches, we construct without observers.
                # Observers are still active in the Python runtime; with C++ runtime they are currently ignored.
                self._impl = _hgraph.GraphExecutorImpl(
                    graph,
                    cpp_mode,
                    [],
                )

            @property
            def run_mode(self):
                return self._impl.run_mode

            @property
            def graph(self):
                return self._impl.graph

            def run(self, start_time, end_time):
                return self._impl.run(start_time, end_time)

        hgraph.GraphEngineFactory.declare(_PyCppGraphExecutor)


        # === TimeSeriesBuilderFactory ===


        def _raise_un_implemented(value_tp):
            raise NotImplementedError(f"Missing builder for {value_tp}")


        class HgCppFactory(hgraph.TimeSeriesBuilderFactory):
            def make_error_builder(self, value_tp):
                return self.make_output_builder(value_tp)

            def _make_tsw_input_builder(self, value_tp):
                return _tsw_input_builder_type_for(value_tp.value_scalar_tp)()

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
                    return _ttsw_output_builder_for_tp(value_tp.value_scalar_tp)(
                        value_tp.size_tp.py_type.TIME_RANGE,
                        value_tp.min_size_tp.py_type.TIME_RANGE
                        if getattr(value_tp.min_size_tp.py_type, "TIME_RANGE", None)
                        else value_tp.min_size_tp.py_type.TIME_RANGE,
                    )
                else:
                    return _tsw_output_builder_for_tp(value_tp.value_scalar_tp)(
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
                
                # Use dictionary lookup for type-based dispatch (matching Python pattern)
                return {
                    hgraph.HgTSTypeMetaData: lambda: _hgraph.InputBuilder_TS_Value_Ref(),
                    hgraph.HgTSLTypeMetaData: lambda: _hgraph.InputBuilder_TSL_Ref(
                        _make_child_ref_builder(referenced_tp.value_tp),
                        referenced_tp.size_tp.py_type.SIZE
                    ),
                    hgraph.HgTSBTypeMetaData: lambda: _hgraph.InputBuilder_TSB_Ref(
                        _hgraph.TimeSeriesSchema(
                            tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                            tp
                        ) if (tp := referenced_tp.bundle_schema_tp.py_type.scalar_type()) is not None
                        else _hgraph.TimeSeriesSchema(tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys())),
                        [_make_child_ref_builder(tp) for tp in referenced_tp.bundle_schema_tp.meta_data_schema.values()]
                    ),
                    hgraph.HgTSDTypeMetaData: lambda: _hgraph.InputBuilder_TSD_Ref(),
                    hgraph.HgTSSTypeMetaData: lambda: _hgraph.InputBuilder_TSS_Ref(),
                    hgraph.HgTSWTypeMetaData: lambda: _hgraph.InputBuilder_TSW_Ref(),
                }.get(type(referenced_tp), lambda: _hgraph.InputBuilder_TS_Ref())()
            
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
                
                # Use dictionary lookup for type-based dispatch (matching Python pattern)
                return {
                    hgraph.HgTSTypeMetaData: lambda: _hgraph.OutputBuilder_TS_Value_Ref(),
                    hgraph.HgTSLTypeMetaData: lambda: _hgraph.OutputBuilder_TSL_Ref(
                        _make_child_ref_builder(referenced_tp.value_tp),
                        referenced_tp.size_tp.py_type.SIZE
                    ),
                    hgraph.HgTSBTypeMetaData: lambda: _hgraph.OutputBuilder_TSB_Ref(
                        _hgraph.TimeSeriesSchema(
                            tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                            tp
                        ) if (tp := referenced_tp.bundle_schema_tp.py_type.scalar_type()) is not None
                        else _hgraph.TimeSeriesSchema(tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys())),
                        [_make_child_ref_builder(tp) for tp in referenced_tp.bundle_schema_tp.meta_data_schema.values()]
                    ),
                    hgraph.HgTSDTypeMetaData: lambda: _hgraph.OutputBuilder_TSD_Ref(),
                    hgraph.HgTSSTypeMetaData: lambda: _hgraph.OutputBuilder_TSS_Ref(),
                    hgraph.HgTSWTypeMetaData: lambda: _hgraph.OutputBuilder_TSW_Ref(),
                }.get(type(referenced_tp), lambda: _hgraph.OutputBuilder_TS_Ref())()


        def _ts_input_builder_type_for(scalar_type):
            return {
                bool: _hgraph.InputBuilder_TS_Bool,
                int: _hgraph.InputBuilder_TS_Int,
                float: _hgraph.InputBuilder_TS_Float,
                date: _hgraph.InputBuilder_TS_Date,
                datetime: _hgraph.InputBuilder_TS_DateTime,
                timedelta: _hgraph.InputBuilder_TS_TimeDelta,
            }.get(scalar_type.py_type, _hgraph.InputBuilder_TS_Object)


        def _ts_output_builder_for_tp(scalar_type):
            return {
                bool: _hgraph.OutputBuilder_TS_Bool,
                int: _hgraph.OutputBuilder_TS_Int,
                float: _hgraph.OutputBuilder_TS_Float,
                date: _hgraph.OutputBuilder_TS_Date,
                datetime: _hgraph.OutputBuilder_TS_DateTime,
                timedelta: _hgraph.OutputBuilder_TS_TimeDelta,
            }.get(scalar_type.py_type, _hgraph.OutputBuilder_TS_Object)


        def _tss_input_builder_type_for(scalar_type):
            return {
                bool: _hgraph.InputBuilder_TSS_Bool,
                int: _hgraph.InputBuilder_TSS_Int,
                float: _hgraph.InputBuilder_TSS_Float,
                date: _hgraph.InputBuilder_TSS_Date,
                datetime: _hgraph.InputBuilder_TSS_DateTime,
                timedelta: _hgraph.InputBuilder_TSS_TimeDelta,
            }.get(scalar_type.py_type, _hgraph.InputBuilder_TSS_Object)


        def _tss_output_builder_for_tp(scalar_type):
            return {
                bool: _hgraph.OutputBuilder_TSS_Bool,
                int: _hgraph.OutputBuilder_TSS_Int,
                float: _hgraph.OutputBuilder_TSS_Float,
                date: _hgraph.OutputBuilder_TSS_Date,
                datetime: _hgraph.OutputBuilder_TSS_DateTime,
                timedelta: _hgraph.OutputBuilder_TSS_TimeDelta,
            }.get(scalar_type.py_type, _hgraph.OutputBuilder_TSS_Object)


        def _tsd_input_builder_type_for(scalar_type):
            return {
                bool: _hgraph.InputBuilder_TSD_Bool,
                int: _hgraph.InputBuilder_TSD_Int,
                float: _hgraph.InputBuilder_TSD_Float,
                date: _hgraph.InputBuilder_TSD_Date,
                datetime: _hgraph.InputBuilder_TSD_DateTime,
                timedelta: _hgraph.InputBuilder_TSD_TimeDelta,
            }.get(scalar_type.py_type, _hgraph.InputBuilder_TSD_Object)


        def _tsd_output_builder_for_tp(scalar_type):
            return {
                bool: _hgraph.OutputBuilder_TSD_Bool,
                int: _hgraph.OutputBuilder_TSD_Int,
                float: _hgraph.OutputBuilder_TSD_Float,
                date: _hgraph.OutputBuilder_TSD_Date,
                datetime: _hgraph.OutputBuilder_TSD_DateTime,
                timedelta: _hgraph.OutputBuilder_TSD_TimeDelta,
            }.get(scalar_type.py_type, _hgraph.OutputBuilder_TSD_Object)


        def _tsw_input_builder_type_for(scalar_type):
            """Unified TSW input builder - works for both fixed-size and timedelta windows"""
            tp = scalar_type.py_type
            return {
                bool: _hgraph.InputBuilder_TSW_Bool,
                int: _hgraph.InputBuilder_TSW_Int,
                float: _hgraph.InputBuilder_TSW_Float,
                date: _hgraph.InputBuilder_TSW_Date,
                datetime: _hgraph.InputBuilder_TSW_DateTime,
                timedelta: _hgraph.InputBuilder_TSW_TimeDelta,
            }.get(tp, _hgraph.InputBuilder_TSW_Object)


        def _tsw_output_builder_for_tp(scalar_type):
            tp = scalar_type.py_type
            mapping = {
                bool: getattr(_hgraph, "OutputBuilder_TSW_Bool", None),
                int: getattr(_hgraph, "OutputBuilder_TSW_Int", None),
                float: getattr(_hgraph, "OutputBuilder_TSW_Float", None),
                date: getattr(_hgraph, "OutputBuilder_TSW_Date", None),
                datetime: getattr(_hgraph, "OutputBuilder_TSW_DateTime", None),
                timedelta: getattr(_hgraph, "OutputBuilder_TSW_TimeDelta", None),
            }
            builder_cls = mapping.get(tp, getattr(_hgraph, "OutputBuilder_TSW_Object", None))

            def ctor(size: int, min_size: int):
                if builder_cls is None:
                    return _raise_un_implemented(f"TSW OutputBuilder for type {tp}")
                return builder_cls(size, min_size)

            return ctor


        def _ttsw_output_builder_for_tp(scalar_type):
            """Time-based (timedelta) TSW output builders"""
            tp = scalar_type.py_type

            def ctor(size: timedelta, min_size: timedelta):
                mapping = {
                    bool: _hgraph.OutputBuilder_TTSW_Bool,
                    int: _hgraph.OutputBuilder_TTSW_Int,
                    float: _hgraph.OutputBuilder_TTSW_Float,
                    date: _hgraph.OutputBuilder_TTSW_Date,
                    datetime: _hgraph.OutputBuilder_TTSW_DateTime,
                    timedelta: _hgraph.OutputBuilder_TTSW_TimeDelta,
                }
                builder_cls = mapping.get(tp, _hgraph.OutputBuilder_TTSW_Object)
                return builder_cls(size, min_size)

            return ctor


        # Register the TimeSeriesBuilderFactory
        hgraph.TimeSeriesBuilderFactory.declare(HgCppFactory())

        # === Node Builder Classes ===

        hgraph._wiring._wiring_node_class.PythonWiringNodeClass.BUILDER_CLASS = _hgraph.PythonNodeBuilder
        hgraph._wiring._wiring_node_class.PythonGeneratorWiringNodeClass.BUILDER_CLASS = (
            _hgraph.PythonGeneratorNodeBuilder
        )


        def _create_set_delta(added, removed, tp):
            sd_tp = {
                bool: _hgraph.SetDelta_bool,
                int: _hgraph.SetDelta_int,
                float: _hgraph.SetDelta_float,
                date: _hgraph.SetDelta_date,
                datetime: _hgraph.SetDelta_date_time,
                timedelta: _hgraph.SetDelta_time_delta,
            }.get(tp, None)
            if sd_tp is None:
                return _hgraph.SetDelta_object(added, removed, tp)
            return sd_tp(added, removed)


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
            key_tp = key_tp.py_type
            return {
                bool: _hgraph.TsdMapNodeBuilder_bool,
                int: _hgraph.TsdMapNodeBuilder_int,
                float: _hgraph.TsdMapNodeBuilder_float,
                date: _hgraph.TsdMapNodeBuilder_date,
                datetime: _hgraph.TsdMapNodeBuilder_date_time,
                timedelta: _hgraph.TsdMapNodeBuilder_time_delta,
            }.get(key_tp, _hgraph.TsdMapNodeBuilder_object)(
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
            ts_input_type = signature.time_series_inputs["ts"]
            key_tp = ts_input_type.key_tp.py_type
            return {
                bool: _hgraph.ReduceNodeBuilder_bool,
                int: _hgraph.ReduceNodeBuilder_int,
                float: _hgraph.ReduceNodeBuilder_float,
                date: _hgraph.ReduceNodeBuilder_date,
                datetime: _hgraph.ReduceNodeBuilder_date_time,
                timedelta: _hgraph.ReduceNodeBuilder_time_delta,
            }.get(key_tp, _hgraph.ReduceNodeBuilder_object)(
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
            switch_input_type = signature.time_series_inputs["key"]
            key_tp = switch_input_type.value_scalar_tp.py_type
            return {
                bool: _hgraph.SwitchNodeBuilder_bool,
                int: _hgraph.SwitchNodeBuilder_int,
                float: _hgraph.SwitchNodeBuilder_float,
                date: _hgraph.SwitchNodeBuilder_date,
                datetime: _hgraph.SwitchNodeBuilder_date_time,
                timedelta: _hgraph.SwitchNodeBuilder_time_delta,
            }.get(key_tp, _hgraph.SwitchNodeBuilder_object)(
                signature,
                scalars,
                input_builder,
                output_builder,
                error_builder,
                recordable_state_builder,
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
            return {
                bool: _hgraph.MeshNodeBuilder_bool,
                int: _hgraph.MeshNodeBuilder_int,
                float: _hgraph.MeshNodeBuilder_float,
                date: _hgraph.MeshNodeBuilder_date,
                datetime: _hgraph.MeshNodeBuilder_date_time,
                timedelta: _hgraph.MeshNodeBuilder_time_delta,
            }.get(key_tp.py_type, _hgraph.MeshNodeBuilder_object)(
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
