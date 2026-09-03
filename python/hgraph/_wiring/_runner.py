"""Graph execution entry points: run_graph/evaluate_graph and the
eval_node test harness."""
import functools
import inspect
import logging
import time
from datetime import timedelta

import _hgraph

from .._signature import _is_time_series
from .._types import (_GenericTsExpr, _TsExpr, _TypeVarSentinel,
                      _type_var_is_scalar)
from ._core import (WiringError, WiringPort, _OperatorFunction, _unwrap,
                    _wiring_lock, _wiring_stack, wire)
from ._graph import _GraphFn
from ._node import _PyNode, _ensure_current_signature
from ._sentinels import _simplify_delta
from ._state import (GlobalState, _global_state_scope,
                     _GRAPH_LOGGER_FORMATTER_KEY,
                     _GRAPH_LOGGER_KEY, utc_now)

class EvaluationMode:
    SIMULATION = "simulation"
    REAL_TIME = "real_time"


class EvaluationLifeCycleObserver:
    """Optional callbacks delivered by the native graph executor.

    Graph and node arguments are callback-scoped native views. Retaining and
    using one after its callback returns raises ``RuntimeError``.
    """

    def on_before_start_graph(self, graph): pass
    def on_after_start_graph(self, graph): pass
    def on_start_graph_failed(self, graph): pass
    def on_before_start_node(self, node): pass
    def on_after_start_node(self, node): pass
    def on_start_node_failed(self, node): pass
    def on_before_graph_evaluation(self, graph): pass
    def on_after_graph_evaluation(self, graph): pass
    def on_before_node_evaluation(self, node): pass
    def on_after_node_evaluation(self, node): pass
    def on_after_graph_push_nodes_evaluation(self, graph): pass
    def on_before_stop_node(self, node): pass
    def on_after_stop_node(self, node): pass
    def on_stop_node_failed(self, node): pass
    def on_before_stop_graph(self, graph): pass
    def on_after_stop_graph(self, graph): pass
    def on_stop_graph_failed(self, graph): pass


class GraphConfiguration:
    """Configuration for one native graph execution.

    The public shape matches Python hgraph and adapts each option to the native
    wiring and execution engines.

    :param run_mode: ``EvaluationMode.SIMULATION`` or
        ``EvaluationMode.REAL_TIME``.
    :param start_time: Optional first evaluation time. Defaults to ``MIN_ST``
        in simulation mode and current UTC time in real-time mode.
    :param end_time: Run bound, or a :class:`~datetime.timedelta`
        relative to the effective start time.
    :param trace: ``False``, ``True``, an ``EvaluationTrace``, or keyword
        options used to construct one.
    :param profile: ``False``, ``True``, an ``EvaluationProfiler``, or keyword
        options used to construct one.
    :param life_cycle_observers: Native execution lifecycle observers.
    :param trace_wiring: Enable native wiring trace events.
    :param wiring_observers: Observers notified while the graph is wired.
    :param graph_logger: Logger-like destination for graph diagnostics.
    :param trace_back_depth: Non-negative number of wiring frames retained for
        diagnostics.
    :param capture_values: Capture live values in diagnostic graph views.
    :param default_log_level: Logging level applied to ``graph_logger``.
    :param logger_formatter: Optional callable that formats graph log messages.
    :param cleanup_on_error: Stop and release a partially run graph after an
        evaluation error.
    """

    def __init__(
            self,
            run_mode=EvaluationMode.SIMULATION,
            start_time=None,
            end_time=None,
            trace=False,
            profile=False,
            life_cycle_observers=(),
            trace_wiring=False,
            wiring_observers=(),
            graph_logger=None,
            trace_back_depth=1,
            capture_values=False,
            default_log_level=logging.DEBUG,
            logger_formatter=None,
            cleanup_on_error=True):
        self.run_mode = run_mode
        if start_time is None:
            start_time = (
                utc_now()
                if run_mode == EvaluationMode.REAL_TIME
                else _hgraph.MIN_ST
            )
        self.start_time = start_time
        if isinstance(end_time, timedelta):
            relative_start = (
                utc_now()
                if run_mode == EvaluationMode.REAL_TIME
                else self.start_time
            )
            end_time = relative_start + end_time
        self.end_time = end_time
        self.trace = trace
        self.profile = profile
        self.life_cycle_observers = tuple(life_cycle_observers)
        self.trace_wiring = trace_wiring
        self.wiring_observers = tuple(wiring_observers)
        self.graph_logger = graph_logger or logging.getLogger("hgraph")
        self.trace_back_depth = trace_back_depth
        self.capture_values = capture_values
        self.default_log_level = default_log_level
        self.logger_formatter = logger_formatter
        self.cleanup_on_error = cleanup_on_error

    def _validate(self):
        if self.run_mode not in (EvaluationMode.SIMULATION, EvaluationMode.REAL_TIME):
            raise ValueError(
                "GraphConfiguration.run_mode must be EvaluationMode.SIMULATION or "
                "EvaluationMode.REAL_TIME")
        _make_evaluation_trace(self.trace)
        _make_evaluation_profiler(self.profile)
        if not isinstance(self.trace_back_depth, int) or self.trace_back_depth < 0:
            raise ValueError("GraphConfiguration.trace_back_depth must be a non-negative integer")
        if not all(hasattr(self.graph_logger, name)
                   for name in ("setLevel", "log", "getChild")):
            raise TypeError("GraphConfiguration.graph_logger must be a logging.Logger-like object")
        if self.logger_formatter is not None and not callable(self.logger_formatter):
            raise TypeError("GraphConfiguration.logger_formatter must be callable or None")


def _make_evaluation_trace(trace):
    """Adapt the Python ``bool | dict`` trace option to the native observer."""
    if not trace:
        return None
    if isinstance(trace, _hgraph.EvaluationTrace):
        return trace
    if trace is True:
        return _hgraph.EvaluationTrace()
    if type(trace) is dict:
        return _hgraph.EvaluationTrace(**trace)
    raise TypeError("trace must be a bool, dict, or EvaluationTrace")


def _make_evaluation_profiler(profile):
    """Adapt the Python ``bool | dict`` profile option to the native observer."""
    if not profile:
        return None
    if isinstance(profile, _hgraph.EvaluationProfiler):
        return profile
    if profile is True:
        return _hgraph.EvaluationProfiler()
    if type(profile) is dict:
        return _hgraph.EvaluationProfiler(**profile)
    raise TypeError("profile must be a bool, dict, or EvaluationProfiler")


def _log_evaluation_profile(logger, snapshot):
    logger.info(
        "Graph profile: cycles=%d wall=%.6fs eval=%.6fs load=%.2f%%",
        snapshot.graph_cycles,
        snapshot.wall_time.total_seconds(),
        snapshot.root_evaluation_time.total_seconds(),
        snapshot.runtime_load * 100.0,
    )
    for entry in snapshot.entries:
        phase = entry.evaluation
        if phase.count:
            logger.debug(
                "Profile %s: count=%d failures=%d total=%.6fs max=%.6fs recent=%.6fs",
                entry.path, phase.count, phase.failures,
                phase.total_time.total_seconds(), phase.max_time.total_seconds(),
                phase.recent_time.total_seconds(),
            )


def evaluate_graph(graph, config=None, *args, **kwargs):
    """Wire and run a graph under an explicit :class:`GraphConfiguration`.

    :param graph: Decorated graph, node, or operator to evaluate.
    :param config: Execution configuration. A default configuration is created
        when omitted.
    :param args: Positional wiring-time arguments passed to ``graph``.
    :param kwargs: Keyword wiring-time arguments passed to ``graph``.
    :return: Output ticks as ``(time, value)`` pairs, or ``None`` for a sink.
    """
    fn = graph
    return _evaluate_graph(fn, config or GraphConfiguration(), args, kwargs)

def _times_for(values, start_time):
    return [
        (start_time + i * _hgraph.MIN_TD, v)
        for i, v in enumerate(values)
        if v is not None
    ]


def _times_for_sparse(values):
    return [
        (_hgraph.MIN_ST + offset * _hgraph.MIN_TD, value)
        for offset, value in values
    ]


def run_graph(
        graph,
        *args,
        run_mode=EvaluationMode.SIMULATION,
        start_time=None,
        end_time=None,
        print_progress=True,
        life_cycle_observers=None,
        __trace__=False,
        __profile__=False,
        __trace_wiring__=False,
        __logger__=None,
        __trace_back_depth__=1,
        __capture_values__=False,
        **kwargs):
    """Wire and evaluate a graph with convenient configuration arguments.

    Returns ``[(time, value), ...]`` for graph output ticks, or ``None`` for a
    sink. ``end_time`` is required for self-perpetuating graphs such as bound
    feedback loops. The simulation clock is cycle-aligned from ``MIN_ST`` in
    ``MIN_TD`` steps.

    :param graph: Decorated graph, node, or operator to evaluate.
    :param args: Positional wiring-time arguments passed to ``graph``.
    :param run_mode: Simulation or real-time evaluation mode.
    :param start_time: First evaluation time. Defaults to ``MIN_ST`` in
        simulation mode and current UTC time in real-time mode.
    :param end_time: Run bound, or a duration relative to the start.
    :param print_progress: Compatibility option; progress rendering is not
        performed by the runtime.
    :param life_cycle_observers: Native execution lifecycle observers.
    :param __trace__: Enable evaluation tracing, or supply trace options.
    :param __profile__: Enable evaluation profiling, or supply profile options.
    :param __trace_wiring__: Enable wiring tracing.
    :param __logger__: Logger-like destination for graph diagnostics.
    :param __trace_back_depth__: Wiring traceback depth retained for errors.
    :param __capture_values__: Capture live values in diagnostic graph views.
    :param kwargs: Keyword wiring-time arguments passed to ``graph``.
    :return: Output ticks as ``(time, value)`` pairs, or ``None`` for a sink.
    """
    graph_fn = graph
    del print_progress  # progress rendering is a presentation concern
    config = GraphConfiguration(
        run_mode=run_mode,
        start_time=start_time,
        end_time=end_time,
        trace=__trace__,
        profile=__profile__,
        life_cycle_observers=life_cycle_observers or (),
        trace_wiring=__trace_wiring__,
        graph_logger=__logger__,
        trace_back_depth=__trace_back_depth__,
        capture_values=__capture_values__,
    )
    return _evaluate_graph(graph_fn, config, args, kwargs)


def _evaluate_graph(graph_fn, config, args, kwargs):
    config._validate()
    trace = _make_evaluation_trace(config.trace)
    profiler = _make_evaluation_profiler(config.profile)
    from .._types import _finalize_compound_scalar_types
    _finalize_compound_scalar_types()
    # Opened before wiring, closed in the finally below: the state must not
    # outlive the run that needed it (see _global_state_scope).
    _gs_scope = _global_state_scope()
    state = _gs_scope.__enter__()
    # Setup between here and the try below can raise -- config.graph_logger
    # .setLevel() rejects a bad level, for one -- and the scope must not be
    # left open when it does, or the next run inherits a contaminated state.
    try:
        missing = object()
        previous_logger = state.get(_GRAPH_LOGGER_KEY, missing)
        previous_formatter = state.get(_GRAPH_LOGGER_FORMATTER_KEY, missing)
        previous_start_time = state.get("__start_time__", missing)
        state[_GRAPH_LOGGER_KEY] = config.graph_logger
        if config.logger_formatter is None:
            state.pop(_GRAPH_LOGGER_FORMATTER_KEY, None)
        else:
            state[_GRAPH_LOGGER_FORMATTER_KEY] = config.logger_formatter
        config.graph_logger.setLevel(config.default_log_level)
    except BaseException:
        _gs_scope.__exit__(None, None, None)
        raise
    w = None
    wiring_pushed = False
    wiring_lock_held = False

    def wiring_prepared():
        """End process-wide wiring serialization before native execution."""
        nonlocal wiring_pushed, wiring_lock_held
        if wiring_pushed:
            _wiring_stack.pop()
            wiring_pushed = False
        if wiring_lock_held:
            _wiring_lock.release()
            wiring_lock_held = False

    try:
        _wiring_lock.acquire()
        wiring_lock_held = True
        realtime = config.run_mode == EvaluationMode.REAL_TIME
        w = _hgraph.Wiring(state._impl, realtime)
        w.configure_wiring_observers(
            config.trace_wiring, config.wiring_observers)
        _wiring_stack.append(w)
        wiring_pushed = True
        wiring_started = time.perf_counter()
        # Python-readable mirror of the run start (the C++ run bounds have no
        # wiring-time getter), exactly as eval_node publishes it: start-time-
        # aware wiring — the DATA_FRAME replay overloads' upstream
        # `_api.start_time` filter — reads this. Without it a stored frame
        # carrying rows before start_time replays them into the native
        # generator and the stream aborts empty.
        state["__start_time__"] = (
            config.start_time if config.start_time is not None else _hgraph.MIN_ST)
        config.graph_logger.debug("Wiring graph: %s", getattr(graph_fn, "__name__", graph_fn))
        out = graph_fn(*args, **kwargs)
        # Wiring may materialize a base schema before a downstream extension
        # class is visited. Close every now-visible hierarchy immediately
        # before the native graph captures its immutable type realization.
        _finalize_compound_scalar_types()
        if out is not None:
            w.wire(
                "__harness_record",
                (_unwrap(out).dereferenced, "__run_graph__"),
                {"sparse": True},
            )
        config.graph_logger.debug(
            "Graph wiring completed in %.2f seconds",
            time.perf_counter() - wiring_started,
        )
        run = w.run(start_time=config.start_time, end_time=config.end_time,
                    realtime=realtime,
                    trace=trace, profiler=profiler,
                    logger=config.graph_logger,
                    logger_level=config.default_log_level,
                    logger_formatter=config.logger_formatter,
                    observers=config.life_cycle_observers,
                    trace_back_depth=config.trace_back_depth,
                    capture_values=config.capture_values,
                    cleanup_on_error=config.cleanup_on_error,
                    _on_prepared=wiring_prepared)
    finally:
        if w is not None:
            for line in w.wiring_trace_lines():
                print(line)
            w._release_seed_context()
        if wiring_pushed:
            _wiring_stack.pop()
        if wiring_lock_held:
            _wiring_lock.release()
        if previous_logger is missing:
            state.pop(_GRAPH_LOGGER_KEY, None)
        else:
            state[_GRAPH_LOGGER_KEY] = previous_logger
        if previous_formatter is missing:
            state.pop(_GRAPH_LOGGER_FORMATTER_KEY, None)
        else:
            state[_GRAPH_LOGGER_FORMATTER_KEY] = previous_formatter
        # Restore the enclosing run's start mirror (a nested evaluate_graph
        # during an outer wiring must not leak its bound into replay wired
        # later by the outer graph).
        if previous_start_time is missing:
            state.pop("__start_time__", None)
        else:
            state["__start_time__"] = previous_start_time
        _gs_scope.__exit__(None, None, None)
    if profiler is not None:
        _log_evaluation_profile(config.graph_logger, profiler.snapshot())
    if out is None:
        return None
    return _times_for_sparse(run.recorded("__run_graph__", sparse=True))


def _resolve_generic_annotation(annotation, samples):
    """Resolve a generic collection annotation from eval-node samples.

    ``TSL[X, SIZE]`` (nested children included) becomes the concrete TSL by
    inferring the outer size from the samples (int-keyed dict → max key + 1;
    list/tuple → length). A bare ``TSS[tuple]`` is represented by a
    homogeneous tuple of Python objects: the native set remains the owner of
    delta and lifetime semantics while the eval harness supplies the concrete
    scalar storage that a C++ graph requires. Any annotation that still cannot
    resolve returns None and the caller falls back to sample-scalar inference."""
    pattern = getattr(annotation, "pattern", None)
    if pattern is None:
        return None

    if pattern.ts_kind == _hgraph.TS_KIND_TSS:
        from .._types import TSS
        from ._sentinels import Removed

        elements = (
            item.item if isinstance(item, Removed) else item
            for sample in samples
            if isinstance(sample, (set, frozenset))
            for item in sample
        )
        if any(isinstance(item, tuple) for item in elements):
            concrete = TSS[tuple[object, ...]]
            scope = _hgraph.ResolutionScope()
            if scope.match(pattern, concrete.handle):
                return concrete

    size = 0
    for sample in samples:
        if sample is None:
            continue
        if isinstance(sample, dict):
            keys = [k for k in sample
                    if isinstance(k, int) and not isinstance(k, bool)]
            if len(keys) != len(sample):
                return None
            if keys:
                size = max(size, max(keys) + 1)
        elif isinstance(sample, (list, tuple)):
            size = max(size, len(sample))
        else:
            return None
    if size == 0:
        return None
    scope = _hgraph.ResolutionScope()
    scope.bind_size("SIZE", size)
    resolved = scope.resolve_ts(pattern)
    if resolved is None:
        return None
    return _TsExpr(resolved, f"resolved[{annotation!r}]")


def _infer_ts_type(series):
    """The TS type for an eval_node input vector, from its first non-None
    sample (hgraph's operators are driven without annotations)."""
    from .._types import TS, TSS, TSD

    for sample in series:
        if sample is None:
            continue
        # UN-ANNOTATED inference treats container samples as SCALAR values
        # (hgraph's operator-test convention). TSS/TSD delta semantics apply
        # only through annotated parameters.
        if isinstance(sample, (set, frozenset)):
            for element in sample:
                return TS[frozenset[type(element)]]
            continue
        if isinstance(sample, tuple):
            for element in sample:
                return TS[tuple[type(element), ...]]
            continue
        if isinstance(sample, dict):
            for key_sample, value_sample in sample.items():
                return TS[dict[type(key_sample), type(value_sample)]]
            continue
        return TS[type(sample)]
    return None


def _operator_parameter_annotations(fn):
    """Resolve eval harness annotations from the native operator signature.

    Subscript values such as ``op[TIME_SERIES_TYPE: TS[int]]`` resolve
    generic input positions only. Concrete positions come directly from the
    C++ registry; Python must not assign these hints by call order.
    """
    if not isinstance(fn, _OperatorFunction) and hasattr(fn, "_delegate"):
        fn = fn._delegate
    if not isinstance(fn, _OperatorFunction):
        return (), False
    shape = _hgraph.operator_parameter_shape(fn.__name__)
    if shape is None:
        return (), False

    parameters, variadic = shape
    hints = fn._ts_hint
    if hints is None:
        hints = []
    elif not isinstance(hints, list):
        hints = [hints]

    annotations = []
    hint_index = 0
    for name, is_input, type_variable, fixed in parameters:
        annotation = None
        if is_input:
            if fixed is not None:
                annotation = _TsExpr(fixed, repr(fixed))
            if type_variable and hints:
                annotation = hints[min(hint_index, len(hints) - 1)]
                hint_index += 1
        annotations.append((name, annotation))
    return tuple(annotations), variadic


def eval_node(node, *args, output_type=None, resolution_dict=None,
              __trace__=False, __trace_wiring__=False, __observers__=None,
              __start_time__=None, __end_time__=None, __scalars__=None,
              __elide__=False, __run_mode__=EvaluationMode.SIMULATION,
              **kwargs):
    """Evaluate a graph or node against per-cycle test input vectors.

    ``None`` in a vector means no tick for that cycle. Time-series input types
    come from the callable annotations. The run ends when nothing remains
    scheduled unless ``__end_time__`` supplies an explicit bound.

    :param node: Decorated graph, node, or operator under test.
    :param args: Positional per-cycle input vectors, in signature order.
    :param output_type: Explicit output time-series type when it cannot be
        inferred.
    :param resolution_dict: Mapping from time-series parameter names to
        concrete types for generic inputs.
    :param __trace__: Enable evaluation tracing, or supply trace options.
    :param __trace_wiring__: Enable wiring tracing.
    :param __observers__: Additional native lifecycle observers.
    :param __start_time__: First simulated evaluation time.
    :param __end_time__: Explicit evaluation bound.
    :param __scalars__: Mapping of wiring-time scalar arguments.
    :param __elide__: Return only cycles in which the output ticks.
    :param __run_mode__: ``EvaluationMode.SIMULATION`` or
        ``EvaluationMode.REAL_TIME``.
    :param kwargs: Named per-cycle input vectors or wiring-time scalar values.
    :return: One output value per simulated cycle, using ``None`` for cycles
        without an output tick; sink nodes return ``None``.
    """
    # Upstream-compatible parameter names (node, *args) are the public
    # contract; the implementation keeps its internal vocabulary.
    if __run_mode__ not in (EvaluationMode.SIMULATION, EvaluationMode.REAL_TIME):
        raise ValueError(
            "eval_node __run_mode__ must be EvaluationMode.SIMULATION or "
            "EvaluationMode.REAL_TIME")
    realtime = __run_mode__ == EvaluationMode.REAL_TIME
    fn, inputs = node, args
    if isinstance(fn, (_GraphFn, _PyNode)):
        _ensure_current_signature(fn)
    try:
        fn_sig = inspect.signature(
            fn.fn if isinstance(fn, _GraphFn) or hasattr(fn, "_delegate") else fn,
            eval_str=True)
        declared_params = list(fn_sig.parameters.values())
    except (TypeError, ValueError):
        declared_params = None
    params = [p for p in (declared_params or ())
              if p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)]
    param_names = {p.name for p in params}
    annotations_by_name = {p.name: p.annotation for p in params}

    # Decorated Python graphs and nodes expose an authoritative set of
    # time-series inputs. Reject misspellings before they silently fall
    # through to inference. Native operators deliberately retain their
    # positional/variadic resolution fallback because overload families do
    # not yet expose one complete set of accepted parameter names.
    decorated_target = fn
    while isinstance(decorated_target, functools.partial):
        decorated_target = decorated_target.func
    if (resolution_dict and declared_params is not None
            and isinstance(decorated_target, (_GraphFn, _PyNode))):
        resolution_parameters = {
            param.name for param in declared_params
            if (param.kind is not inspect.Parameter.VAR_KEYWORD
                and _is_time_series(param.annotation))
        }
        if (not params and any(
                param.kind is inspect.Parameter.VAR_KEYWORD
                and _is_time_series(param.annotation)
                for param in declared_params)):
            # A time-series **collector expands its user-supplied keyword
            # names into individual series when there are no fixed inputs.
            # Only those actual series names are useful resolution keys; the
            # formal collector name is not.
            resolution_parameters.update(
                name for name, value in kwargs.items()
                if isinstance(value, (list, tuple)))
        unknown_parameters = set(resolution_dict) - resolution_parameters
        if unknown_parameters:
            unknown = ", ".join(repr(name) for name in sorted(
                unknown_parameters, key=repr))
            valid = ", ".join(repr(name) for name in sorted(
                resolution_parameters, key=repr)) or "<none>"
            raise WiringError(
                f"eval_node resolution_dict contains unknown parameter(s): {unknown}; "
                f"valid time-series parameters: {valid}")

    pinned_scope = None
    if isinstance(fn, _PyNode) and fn._pins:
        from .._types import (
            _GenericTsExpr as _PinnedGenericTsExpr,
            _TsExpr as _PinnedTsExpr,
            _pattern_of,
        )

        pinned_scope = _hgraph.ResolutionScope()
        for name, resolved in fn._pins.items():
            fn._bind_resolved(pinned_scope, name, resolved)

        for name, annotation in tuple(annotations_by_name.items()):
            if not isinstance(annotation, _PinnedGenericTsExpr):
                continue
            resolved = pinned_scope.resolve_ts(_pattern_of(annotation))
            if resolved is not None:
                # eval_node replays ordinary producer values. REF-transparent
                # input binding performs the reference adaptation at wiring.
                resolved = _hgraph.ref_target(resolved)
                annotations_by_name[name] = _PinnedTsExpr(resolved, repr(annotation))

    def _named_series_value(k, v):
        """A list/tuple kwarg is a per-cycle SERIES only on a time-series
        parameter: SCALAR-kind type variables (upstream's HgScalarTypeVar)
        and plain scalar annotations take the value as-is."""
        if k not in param_names or not isinstance(v, (list, tuple)):
            return False
        annotation = annotations_by_name.get(k)
        if isinstance(annotation, _TypeVarSentinel) and _type_var_is_scalar(annotation):
            return False
        if isinstance(v, tuple) and not isinstance(
                annotation, (_TsExpr, _GenericTsExpr, _TypeVarSentinel)):
            return False   # a tuple on a scalar-annotated param is a value
        return True

    named_series = {k: v for k, v in kwargs.items() if _named_series_value(k, v)}
    for k in named_series:
        kwargs.pop(k)
    if not named_series and kwargs and params:
        # A non-list kwarg naming a TS-annotated param is a plain value:
        # promote it to its positional slot so the const-lift rule applies
        # (scalar-annotated params pass through unchanged either way).
        from .._types import _TsExpr as _TsE
        promoted = {k: v for k, v in kwargs.items()
                    if k in param_names and isinstance(
                        dict((p.name, p.annotation) for p in params).get(k), _TsE)}
        if promoted:
            named_series = {k: v for k, v in promoted.items()}
            for k in named_series:
                kwargs.pop(k)
            by_name = {p.name: i for i, p in enumerate(params)}
            extended = list(inputs) + [None] * (max(by_name[k] for k in named_series) + 1 - len(inputs))
            for k, value in named_series.items():
                extended[by_name[k]] = value
            return eval_node(fn, *extended, output_type=output_type, resolution_dict=resolution_dict,
                             __trace__=__trace__, __trace_wiring__=__trace_wiring__,
                             __observers__=__observers__,
                             __start_time__=__start_time__, __end_time__=__end_time__,
                             __scalars__=__scalars__, __elide__=__elide__,
                             __run_mode__=__run_mode__, **kwargs)
        named_series = {}
    if named_series:
        by_name = {p.name: i for i, p in enumerate(params)}
        extended = list(inputs) + [None] * (max(by_name[k] for k in named_series) + 1 - len(inputs))
        for k, series in named_series.items():
            extended[by_name[k]] = series
        # Scalar-supplied kwargs whose position got padded move into the
        # positional slot; otherwise None would be wired over the kwarg.
        for k in list(kwargs):
            index = by_name.get(k)
            if index is not None and index < len(extended) and extended[index] is None:
                extended[index] = kwargs.pop(k)
        return eval_node(fn, *extended, output_type=output_type, resolution_dict=resolution_dict,
                         __trace__=__trace__, __trace_wiring__=__trace_wiring__,
                         __observers__=__observers__,
                         __start_time__=__start_time__, __end_time__=__end_time__,
                         __scalars__=__scalars__, __elide__=__elide__,
                         __run_mode__=__run_mode__, **kwargs)
    operator_annotations, operator_variadic = _operator_parameter_annotations(fn)
    operator_annotations_by_name = dict(operator_annotations)
    trace = _make_evaluation_trace(__trace__)
    from .._types import _finalize_compound_scalar_types
    _finalize_compound_scalar_types()
    # The scope guard opens BEFORE wiring so the state cannot outlive it: it
    # defers to a caller's `with GlobalState():` when one is active, and
    # otherwise opens one for exactly this run and closes it in the finally.
    _gs_scope = _global_state_scope()
    _global_state = _gs_scope.__enter__()
    try:
        w = _hgraph.Wiring(_global_state._impl, realtime)
    except BaseException:
        _gs_scope.__exit__(None, None, None)
        raise
    _wiring_stack.append(w)
    try:
        w.configure_wiring_observers(__trace_wiring__, ())
        def _producer_annotation(annotation):
            """eval_node samples are values of the producer behind REF[T]."""
            if isinstance(annotation, _TsExpr) and annotation.handle.is_ref:
                return _TsExpr(_hgraph.ref_target(annotation.handle), repr(annotation))
            return annotation

        ports = []
        deferred_replay = []
        scalar_positions = set()
        for i, series in enumerate(inputs):
            annotation = annotations_by_name.get(params[i].name) if i < len(params) else None
            if annotation is inspect.Signature.empty:
                # A curated public signature may intentionally omit Python
                # annotations while the native registry still owns the
                # concrete/generic input contract. Fall through to that
                # contract instead of treating ``Signature.empty`` as a
                # scalar annotation.
                annotation = None
            annotation_from_operator = False
            if annotation is None and operator_annotations:
                if i < len(operator_annotations):
                    annotation = operator_annotations[i][1]
                elif operator_variadic:
                    annotation = operator_annotations[-1][1]
                annotation_from_operator = annotation is not None
            if annotation_from_operator:
                from .._types import TS as _TS, _TsExpr as _TsE, _GenericTsExpr as _GTsE
                if not isinstance(annotation, (_TsE, _GTsE)) and isinstance(series, (list, tuple)):
                    # A scalar type-variable resolution describes the element
                    # type of a time-series input to the native operator.
                    annotation = _TS[annotation]
            if resolution_dict and i < len(params) and params[i].name in resolution_dict:
                annotation = resolution_dict[params[i].name]
            elif (resolution_dict and not params
                  and (len(resolution_dict) == len(inputs)
                       or (isinstance(fn, _OperatorFunction)
                           and fn.__name__ == "getitem_"
                           and i < len(resolution_dict)))):
                # OPERATOR functions have no python signature: when the dict
                # has one entry per positional input, its entries type them
                # in order (keys are the operator's own parameter names).
                # getitem_ is the supported partial form: ``ts`` types the
                # first input while the scalar index retains value inference.
                # Other shorter dictionaries may describe a variadic
                # collection (for example merge's single ``tsl`` entry), so
                # they deliberately retain per-input inference.
                annotation = list(resolution_dict.values())[i]
            from .._types import _GenericTsExpr
            if isinstance(annotation, _GenericTsExpr):
                samples = series if isinstance(series, (list, tuple)) else [series]
                # A generic collection annotation (TSL[..., SIZE]) resolves
                # against the samples first — binding the size yields the
                # REAL dynamic time-series shape (issue #81); only a still-
                # generic annotation falls back to sample-scalar inference.
                annotation = (_resolve_generic_annotation(annotation, samples)
                              or _infer_ts_type(samples))
            annotation = _producer_annotation(annotation)
            import types as _pytypes
            import typing as _typing
            scalar_annotation = (
                (isinstance(annotation, type) and annotation in (bool, int, float, str, bytes, tuple))
                or (isinstance(annotation, (_pytypes.GenericAlias, type(_typing.Tuple[int, ...])))
                    and _typing.get_origin(annotation) is tuple))
            if scalar_annotation:
                scalar_positions.add(i)
            if not isinstance(series, (list, tuple)) or scalar_annotation:
                # hgraph parity: a non-list argument is a plain value (lifted
                # to const where a TS input is expected, or a scalar param).
                # A SCALAR-annotated param keeps tuple values verbatim
                # (keys: tuple[str, ...] is not a series).
                if isinstance(annotation, _TsExpr):
                    src = w.wire("const", (series,), {}, output_type=annotation.handle)
                    ports.append(WiringPort(src))
                    continue
                ports.append(series)
                continue
            if not isinstance(annotation, _TsExpr):
                annotation = _infer_ts_type(series)
                if annotation is None:
                    name = params[i].name if i < len(params) else f"arg_{i}"
                    raise TypeError(f"parameter '{name}' needs a TS[...] annotation or a typed sample value")
            key = f"eval_node::{params[i].name if i < len(params) else i}"
            src = w.wire("__harness_replay", (key,), {}, output_type=annotation.handle)
            deferred_replay.append((key, list(series), annotation.handle))
            ports.append(WiringPort(src))
        scalars = dict(__scalars__ or {})
        # hgraph parity: keyword arguments naming the function's parameters
        # are INPUT SERIES (eval_node(g, a=[...], b=[...])); the rest flow to
        # the node as scalars.
        if not params:
            # OPERATOR functions have no python signature: named list kwargs
            # are input series wired as keyword ports. The native registry
            # supplies fixed/generic parameter roles; keyword order does not.
            named_ports = {}
            for k in [k for k, v in kwargs.items() if isinstance(v, (list, tuple))]:
                series = kwargs.pop(k)
                annotation = None
                if resolution_dict and k in resolution_dict and isinstance(resolution_dict[k], _TsExpr):
                    annotation = resolution_dict[k]   # the dict types NAMED series too
                if annotation is None:
                    annotation = operator_annotations_by_name.get(k)
                if annotation is not None:
                    from .._types import TS as _TS, _GenericTsExpr as _GTsE
                    if not isinstance(annotation, (_TsExpr, _GTsE)):
                        annotation = _TS[annotation]
                if annotation is None:
                    annotation = _infer_ts_type(series)
                if annotation is None:
                    raise TypeError(f"named series '{k}' needs typed sample values")
                annotation = _producer_annotation(annotation)
                key = f"eval_node::{k}"
                src = w.wire("__harness_replay", (key,), {}, output_type=annotation.handle)
                deferred_replay.append((key, list(series), annotation.handle))
                named_ports[k] = WiringPort(src)
                inputs = (*inputs, series)   # count toward the run length
            kwargs.update(named_ports)
        scalars.update(kwargs)   # hgraph parity: extra kwargs flow to the node
        # Python-readable mirror of the run start (the C++ run bounds have no
        # wiring-time getter): start-time-aware wiring — the DATA_FRAME
        # replay overloads' upstream `_api.start_time` filter — reads this.
        _global_state["__start_time__"] = (
            __start_time__ if __start_time__ is not None else _hgraph.MIN_ST)
        out = fn(*ports, **scalars)
        # Replay values convert AFTER wiring: hgraph surfaces wiring errors
        # before data-conversion errors, and tests pin that order.
        for key, series, handle in deferred_replay:
            w.set_replay(key, series, ts_type=handle)
        length = max((len(series) for i, series in enumerate(inputs)
                      if isinstance(series, (list, tuple)) and i not in scalar_positions), default=0)
        _finalize_compound_scalar_types()
        if out is None:
            run = w.run(start_time=__start_time__, end_time=__end_time__,
                        realtime=realtime, trace=trace,
                        observers=tuple(__observers__ or ()))
            return None
        # hgraph parity: a REF graph output records its DEREFERENCED values.
        # A TSB with REF fields records a STRUCTURAL bundle of per-field
        # projections, each dereferenced.
        raw = _unwrap(out)
        record_kwargs = {"sparse": True} if __elide__ else {}
        record_port = raw.dereferenced
        if raw.ts_type.is_tsb and _hgraph.tsb_has_ref_fields(raw.ts_type):
            fields = {
                name: _unwrap(wire("getitem_", WiringPort(raw), name)).dereferenced
                for name, _ in _hgraph.ts_field_types(raw.ts_type)
            }
            record_port = _hgraph.tsb_port(record_port.ts_type, fields)
        elif raw.ts_type.is_fixed_tsl and _hgraph.tsl_element(raw, 0).ts_type.is_ref:
            # A TSL of REF elements: record a structural TSL of per-element
            # projections, each dereferenced.
            record_port = _hgraph.tsl_port(
                [_hgraph.tsl_element(raw, i).dereferenced for i in range(raw.ts_type.fixed_size)]
            )
        w.wire("__harness_record", (record_port, "eval_node::out"), record_kwargs)
        run = w.run(start_time=__start_time__, end_time=__end_time__,
                    realtime=realtime, trace=trace,
                    observers=tuple(__observers__ or ()))
    finally:
        for line in w.wiring_trace_lines():
            print(line)
        w._release_seed_context()
        _wiring_stack.pop()
        _gs_scope.__exit__(None, None, None)
    if __elide__:
        # hgraph parity: elide keeps only the ticked cycles, in order (the
        # recording was made SPARSE, so this is just the list).
        return [_simplify_delta(v) for _, v in run.recorded("eval_node::out", sparse=True)]
    recorded = [None if v is None else _simplify_delta(v) for v in run.recorded("eval_node::out")]
    if (not realtime and __start_time__ is not None
            and __start_time__ > _hgraph.MIN_ST):
        # hgraph parity: the result list is aligned to the RUN START (index 0
        # = the first evaluation cycle); run.recorded indexes from MIN_ST.
        recorded = recorded[int((__start_time__ - _hgraph.MIN_ST) / _hgraph.MIN_TD):]
    recorded += [None] * (length - len(recorded))
    if not any(v is not None for v in recorded):
        return None   # hgraph parity: a never-ticking output reports None
    return recorded
