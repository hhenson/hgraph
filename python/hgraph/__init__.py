"""hgraph - the hgraph API over the C++ runtime.

Mirrors the Python hgraph package surface: TS/TSS/TSD/TSL/TSB types, the
@graph decorator, run_graph, eval_node, and every registered operator as a
module-level function (via PEP 562 - `from hgraph import add_, const,
filter_, ...` all resolve through the C++ operator registry).

Agreed divergences from Python hgraph are recorded in
docs/source/developer_guide/parity_matrix.rst (e.g. REF is value-only)."""
import _hgraph
from datetime import date, datetime, time, timedelta

from ._types import Series
from ._types import (TS, TSS, TSD, TSL, TSB, Size, TimeSeriesSchema, CONTEXT, REQUIRED, SCALAR, SCALAR_1, SCALAR_2, TSW, KeyValue, AUTO_RESOLVE, with_signature,
                     KEYABLE_SCALAR, TIME_SERIES_TYPE, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2, OUT, SIZE,
                     NUMBER, NUMBER_2,
                     DEFAULT, REF, K, V, SCHEMA, TS_SCHEMA,
                     SIGNAL, WINDOW_SIZE, WINDOW_SIZE_MIN, WindowSize, Array, ts_schema, ENUM)
from ._compat import (CmpResult, DivideByZero, exception_time_series, try_except,
                      TryExceptResult, TryExceptTsdMapResult, NodeError,
                      OperatorWiringNodeClass, BoolResult, CompoundScalar, JSON, TimeSeriesReference,
                      NodeException, accumulate, average, center_of_mass_to_alpha, span_to_alpha,
                      to_json_builder, from_json_builder)
from ._wiring import filter_by
from ._wiring import convert
from ._wiring import collect
from ._wiring import emit
from ._wiring import cast_, downcast_ref
from ._wiring import ParseError, IncorrectTypeBinding, RequirementsNotMetWiringError
from ._wiring import evaluate_graph, EvaluationLifeCycleObserver, GraphConfiguration, TSB_OUT, TSD_OUT, TSS_OUT, operator, dispatch, dispatch_
from ._wiring import RecordReplayContext, set_record_replay_model, RECORDABLE_STATE, TS_OUT
from .nodes import pass_through_node
from ._wiring import pass_through, no_key
from ._feature_switch import is_feature_enabled
from ._signature import (WiringNodeType, WiringNodeSignature, extract_signature,
                         extract_kwargs)
from ._wiring import _PyNode as WiringNodeClass
from ._wiring import _PyNode as PythonWiringNodeClass  # upstream-parity name kept alongside PR #15's WiringNodeClass
from ._wiring import _GraphFn as GraphWiringNodeClass
from ._wiring import _Generator as PythonGeneratorWiringNodeClass
from ._wiring import GlobalContext, GlobalState, set_record_replay_config, set_as_of, set_table_schema_date_key, set_table_schema_as_of_key, evaluate_const, utc_now, get_recorded_value, get_recorder_api, get_recording_label, set_recorder_api, set_recording_label, EvaluationClock, TSW_OUT, get_context, equal_lambdas, callable_shape_key
from ._wiring._state import set_time_zone_provider
from ._wiring import WiringPort, WiringGraphContext, graph, run_graph, eval_node, wire, operator_function, map_, reduce, mesh_, MeshWiringPort, get_mesh, REMOVE, REMOVE_IF_EXISTS, feedback, delayed_binding, switch_, passive, compute_node, sink_node, generator, lift, lower, STATE, SCHEDULER, CLOCK, EvaluationEngineApi, LOGGER, NODE, Node, DebugContext, component, record_replay_scope, RecordReplayEnum, comparison_summary, push_queue, EvaluationMode, context, WiringError, reference_service, subscription_service, request_reply_service, register_service, service_impl, adaptor, adaptor_impl, service_adaptor, service_adaptor_impl, register_adaptor, from_graph, to_graph, impl_input, impl_output, get_service_inputs, set_service_output

MIN_ST = _hgraph.MIN_ST
MIN_TD = _hgraph.MIN_TD
MIN_DT = MIN_ST - MIN_TD   # the engine epoch (hgraph's minimum datetime)
MAX_DT = _hgraph.MAX_DT
MAX_ET = _hgraph.MAX_ET    # the maximum end time (last processable instant)
IN_MEMORY = _hgraph.IN_MEMORY
IN_MEMORY_DENSE = _hgraph.IN_MEMORY_DENSE
DATA_FRAME = _hgraph.DATA_FRAME
frame_store_contains = _hgraph.frame_store_contains
frame_store_read = _hgraph.frame_store_read

TimeSeries = _hgraph.TimeSeries
Graph = _hgraph.Graph
_hgraph._set_removed_sentinel(REMOVE)
_hgraph._set_remove_if_exists_sentinel(REMOVE_IF_EXISTS)
_hgraph._set_requirements_error(RequirementsNotMetWiringError)
from ._wiring import Removed as _Removed, _SetDelta, _simplify_delta
_hgraph._set_removed_class(_Removed)
_hgraph._set_set_delta_class(_SetDelta)
_hgraph._set_delta_shaper(_simplify_delta)
_hgraph._set_cmp_result_enum(CmpResult)
_hgraph._set_divide_by_zero_enum(DivideByZero)

from ._wiring import _Combine as _CombineClass
combine = _CombineClass()

from ._types import (Frame, TABLE, COMPOUND_SCALAR, COMPOUND_SCALAR_1,
                     compound_scalar, register_native_scalar_type,
                     register_python_object_type)
from ._frame import (frame_metadata, has_frame_metadata, without_frame_metadata,
                     with_frame_metadata)
from ._table import (ToTableMode, TableSchema, make_table_schema, table_schema,
                     table_shape, table_shape_from_schema, shape_of_table_type,
                     get_table_schema_date_key, get_table_schema_as_of_key)

from ._wiring import Removed

CivilDateTime = _hgraph.CivilDateTime
Period = _hgraph.Period
ZoneId = _hgraph.ZoneId
ZonedDateTime = _hgraph.ZonedDateTime
InstantRange = _hgraph.InstantRange
CivilDateRange = _hgraph.CivilDateRange
InstantRangeSet = _hgraph.InstantRangeSet
CivilDateRangeSet = _hgraph.CivilDateRangeSet
MonthEndPolicy = _hgraph.MonthEndPolicy
AmbiguousTimePolicy = _hgraph.AmbiguousTimePolicy
NonexistentTimePolicy = _hgraph.NonexistentTimePolicy
Boundary = _hgraph.Boundary
from . import temporal

_OPERATOR_NAMES = frozenset(_hgraph.operator_names())
drop_dups = operator_function("dedup")


def __getattr__(name):
    if name in _OPERATOR_NAMES:
        fn = operator_function(name)
        globals()[name] = fn  # cache
        return fn
    if name == "WindowResult":
        from ._compat import _window_result
        globals()[name] = _window_result()  # lazy: its annotations subscript TS[...]
        return globals()[name]
    raise AttributeError(f"module 'hgraph' has no attribute '{name}'")


def __dir__():
    return sorted(set(globals()) | _OPERATOR_NAMES)


__all__ = [
    "TS", "TSS", "TSD", "TSL", "TSB", "Size", "TimeSeriesSchema", "CONTEXT", "REQUIRED", "WiringError", "TimeSeries",
    "NUMBER", "NUMBER_2",
    "WiringPort", "CmpResult", "DivideByZero", "NodeError", "exception_time_series", "try_except", "lift", "lower",
    "TryExceptResult", "TryExceptTsdMapResult", "OperatorWiringNodeClass", "graph", "run_graph", "eval_node", "wire", "map_", "reduce", "mesh_", "MeshWiringPort", "get_mesh", "REMOVE", "REMOVE_IF_EXISTS", "feedback", "delayed_binding", "switch_", "passive", "compute_node", "sink_node", "generator", "STATE", "SCHEDULER", "CLOCK", "EvaluationEngineApi", "NODE", "Node", "component", "record_replay_scope", "RecordReplayEnum", "comparison_summary", "push_queue", "EvaluationMode", "context",
    "MIN_ST", "MIN_TD", "MIN_DT", "MAX_DT", "MAX_ET", "IN_MEMORY", "IN_MEMORY_DENSE", "DATA_FRAME",
    "date", "datetime", "time", "timedelta", "default_path",
    "utc_now", "get_recorded_value", "get_recorder_api", "get_recording_label", "set_recorder_api", "set_recording_label", "EvaluationClock", "TSW_OUT", "get_context", "equal_lambdas", "is_feature_enabled",
    "GlobalContext", "GlobalState", "set_as_of", "set_table_schema_date_key", "set_table_schema_as_of_key",
    "set_record_replay_config", "set_time_zone_provider", "frame_store_contains", "frame_store_read", "evaluate_const",
    "CivilDateTime", "Period", "ZoneId", "ZonedDateTime", "InstantRange", "CivilDateRange",
    "InstantRangeSet", "CivilDateRangeSet", "MonthEndPolicy", "AmbiguousTimePolicy",
    "NonexistentTimePolicy", "Boundary", "temporal",
    "Frame", "with_frame_metadata", "frame_metadata", "has_frame_metadata", "without_frame_metadata", "register_native_scalar_type", "register_python_object_type", "TABLE", "COMPOUND_SCALAR", "COMPOUND_SCALAR_1", "ToTableMode", "TableSchema", "make_table_schema", "table_schema",
    "table_shape", "table_shape_from_schema", "shape_of_table_type", "drop_dups",
    "get_table_schema_date_key", "get_table_schema_as_of_key",
    "ParseError", "IncorrectTypeBinding", "RequirementsNotMetWiringError",
    "WiringNodeType", "WiringNodeSignature", "extract_signature", "extract_kwargs",
    "WiringGraphContext",
    "WiringNodeClass", "PythonWiringNodeClass", "GraphWiringNodeClass", "PythonGeneratorWiringNodeClass",
    "evaluate_graph", "EvaluationLifeCycleObserver", "GraphConfiguration", "Graph", "TSB_OUT", "TSD_OUT", "TSS_OUT", "operator",
    "pass_through_node", "pass_through", "no_key", "downcast_ref",
    "reference_service", "subscription_service", "request_reply_service", "service_impl", "register_service",
    "adaptor", "adaptor_impl", "service_adaptor", "service_adaptor_impl", "register_adaptor",
    "from_graph", "to_graph", "impl_input", "impl_output", "get_service_inputs", "set_service_output",
]

from ._wiring import set_delta, compute_set_delta
from ._types import K_1
default_path = ""   # hgraph's default service path sentinel
