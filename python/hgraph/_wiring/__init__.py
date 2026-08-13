"""The wiring layer (the former hgraph._runtime, split 2026-07).

Explicit aggregation: every module-level name the wiring layer defines is
imported and listed in ``__all__`` (underscored names included) so a missed
symbol fails loudly at import time. ``_wiring_stack`` is re-exported by
from-import aliasing — it stays the SAME list object as
``hgraph._wiring._core._wiring_stack``; nothing may rebind it."""
from ._sentinels import (
    REMOVE, REMOVE_IF_EXISTS, Removed, _Removed, _SetDelta, _simplify_delta, compute_set_delta, set_delta
)
from ._state import (
    GlobalContext, GlobalState, RecordReplayContext, RecordReplayEnum, _GLOBAL_MISSING,
    _MemoryRecording, _RecordReplayModes, _friendly_recording_delta, _global_state_local,
    _enter_runtime, _exit_runtime, comparison_summary, evaluate_const,
    record_replay_scope, set_as_of, set_record_replay_config, set_record_replay_model,
    set_pooled_compound_scalar_storage,
    set_table_schema_as_of_key, set_table_schema_date_key, get_recorded_value,
    get_recorder_api, get_recording_label, set_recorder_api, set_recording_label, utc_now
)
from ._markers import (
    CLOCK, EvaluationClock, EvaluationEngineApi, LOGGER, NODE, Node, RECORDABLE_STATE, Traits,
    SCHEDULER, STATE, TSB_OUT, TSD_OUT, TSS_OUT, TSW_OUT, TS_OUT, _INJECTABLE_MARKERS,
    _MISSING, _RecordableStateExpr, _RecordableStateMarker, _TSW_KIND, _TsOutMarker,
    _UNBOUNDED_TUPLE_KIND, _annotation_ts_kind, _is_object_vt, _tsw_kind, _unbounded_tuple_kind
)
from ._core import (
    IncorrectTypeBinding, ParseError, RequirementsNotMetWiringError, WiringError, WiringPort,
    _DUNDERS, _OperatorFunction, _context_name_of, _current_wiring, _port_enter, _port_exit,
    _port_getattr, _port_getitem, _port_iter, _port_keys, _port_len, _port_reduce,
    _published_contexts, _resolve_context, _unwrap, _wiring_stack, operator_function, wire
)
from ._resolution import (
    _BindingsMap, _apply_resolvers, _bind_resolution, _invoke_resolution_callable
)
from ._operator import (
    _DISPATCH_KEYS_NODE, _DISPATCH_KEY_NODE, _Dispatch, _Operator,
    _declared_dispatch_class, _declared_dispatch_classes, _dispatch_branch, _dispatch_key_node,
    _dispatch_keys_node, _dispatch_specificity, _overload_registry_name,
    _overload_wire_trampoline, _register_overload, _requires_bridge, _run_requires, dispatch,
    dispatch_, operator
)
from ._node import (
    _Generator, _PushQueue, _PyNode, _make_py_node, compute_node, generator, lift, push_queue,
    sink_node
)
from ._lower import lower
from ._graph import (
    _Component, _GraphFn, _ResolvedSize, _as_wired, _graph_auto_resolve, _wrap_graph_fn,
    component, graph
)
from ._compose import (
    DebugContext, DelayedBinding, Feedback, _Collect, _Combine, _Convert, _Emit, _TsExprFor,
    _bind_switch_scalar_args, _combine_compound_scalars, _merge_cs, _resolve_requested_target,
    _type_pattern_for_target, cast_, collect, combine, convert, delayed_binding, downcast_, downcast_ref, emit, feedback,
    MeshWiringPort, filter_by, get_mesh, map_, mesh_, no_key, pass_through, passive, reduce, switch_
)
from ._services import (
    _AdaptorStub, _FLAVOUR_TS_ARITY, _ServiceAdaptorStub, _ServiceImpl, _ServiceInputs,
    _ServiceStub, _bind_registered_impl, adaptor, adaptor_impl, context, from_graph, get_context,
    get_service_inputs, impl_input, impl_output, reference_service, register_adaptor,
    register_service, request_reply_service, service_adaptor, service_adaptor_impl,
    service_impl, set_service_output, subscription_service, to_graph, WiringGraphContext
)
from ._callable_shape import callable_shape_key, equal_lambdas
from ._runner import (
    EvaluationLifeCycleObserver, EvaluationMode, GraphConfiguration, _infer_ts_type, _times_for, eval_node, evaluate_graph,
    run_graph
)

__all__ = [
    "CLOCK", "EvaluationClock", "callable_shape_key", "equal_lambdas", "get_context", "get_recorded_value", "get_recorder_api", "get_recording_label", "set_recorder_api", "set_recording_label", "utc_now", "TSW_OUT", "DebugContext", "EvaluationEngineApi", "EvaluationLifeCycleObserver", "EvaluationMode", "Feedback", "GlobalContext", "GlobalState",
    "GraphConfiguration", "IncorrectTypeBinding", "LOGGER", "NODE", "Node", "ParseError", "RECORDABLE_STATE", "Traits",
    "REMOVE", "REMOVE_IF_EXISTS", "RecordReplayContext", "RecordReplayEnum", "Removed",
    "RequirementsNotMetWiringError", "SCHEDULER", "STATE", "TSB_OUT", "TSD_OUT", "TSS_OUT", "TS_OUT", "WiringError",
    "WiringPort", "DelayedBinding", "_AdaptorStub", "_BindingsMap", "_Collect", "_Combine", "_Component",
    "_Convert", "_DISPATCH_KEYS_NODE", "_DISPATCH_KEY_NODE", "_DUNDERS", "_Dispatch", "_Emit",
    "_FLAVOUR_TS_ARITY", "_GLOBAL_MISSING", "_Generator", "_GraphFn", "_INJECTABLE_MARKERS",
    "_MISSING", "_MemoryRecording", "_Operator", "_OperatorFunction", "_PushQueue", "_PyNode",
    "_RecordReplayModes", "_RecordableStateExpr", "_RecordableStateMarker", "_Removed",
    "_ResolvedSize", "_ServiceAdaptorStub", "_ServiceImpl", "_ServiceInputs", "_ServiceStub",
    "_SetDelta", "_TSW_KIND", "_TsExprFor", "_TsOutMarker", "_UNBOUNDED_TUPLE_KIND",
    "_annotation_ts_kind", "_apply_resolvers", "_as_wired", "_bind_registered_impl", "_bind_resolution", "_bind_switch_scalar_args",
    "_combine_compound_scalars", "_context_name_of", "_current_wiring",
    "_declared_dispatch_class", "_declared_dispatch_classes", "_dispatch_branch",
    "_dispatch_key_node", "_dispatch_keys_node", "_dispatch_specificity",
    "_friendly_recording_delta", "_global_state_local", "_graph_auto_resolve", "_infer_ts_type",
    "_is_object_vt", "_make_py_node", "_merge_cs", "_overload_registry_name",
    "_enter_runtime", "_exit_runtime", "_overload_wire_trampoline", "_port_enter", "_port_exit",
    "_port_getattr", "_port_getitem", "_port_iter", "_port_keys", "_port_len", "_port_reduce",
    "_published_contexts", "_register_overload",
    "_invoke_resolution_callable", "_requires_bridge", "_resolve_context", "_resolve_requested_target", "_run_requires",
    "_simplify_delta", "_times_for", "_tsw_kind", "_type_pattern_for_target",
    "_unbounded_tuple_kind", "_unwrap", "_wiring_stack", "_wrap_graph_fn", "adaptor",
    "adaptor_impl", "cast_", "collect", "combine", "comparison_summary", "component",
    "compute_node", "compute_set_delta", "context", "convert", "dispatch", "dispatch_",
    "delayed_binding", "downcast_", "downcast_ref", "emit", "eval_node", "evaluate_const", "evaluate_graph", "feedback",
    "filter_by", "from_graph", "generator", "get_service_inputs", "graph", "impl_input",
    "impl_output", "lift", "lower", "map_", "mesh_", "MeshWiringPort", "get_mesh", "no_key", "operator",
    "operator_function", "pass_through", "passive", "push_queue", "record_replay_scope",
    "reduce", "reference_service", "register_adaptor", "register_service",
    "request_reply_service", "run_graph", "service_adaptor", "service_adaptor_impl",
    "service_impl", "set_as_of", "set_delta", "set_pooled_compound_scalar_storage", "set_record_replay_config",
    "set_record_replay_model", "set_service_output", "set_table_schema_as_of_key",
    "set_table_schema_date_key", "sink_node", "subscription_service", "switch_", "to_graph",
    "wire", "WiringGraphContext"
]
