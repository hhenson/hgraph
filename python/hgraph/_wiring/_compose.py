"""Higher-order wiring: map_/reduce/switch_/mesh_, combine, feedback,
convert/collect/emit, DebugContext and casts."""
import inspect

import _hgraph

from ._core import (_OperatorFunction, WiringError, WiringPort, _current_wiring, _unwrap,
                    operator_function, wire)
from ._graph import _as_wired, _prepare_higher_order_call
from ._markers import _unbounded_tuple_kind
from ._sentinels import _REDUCE_ZERO

def map_(func, *args, __label__=None, __keys__=None, __key_arg__=None, **kwargs):
    """Apply a graph independently to each keyed or list element.

    Multiplexed arguments supply one element to each child graph; ordinary
    inputs are broadcast whole. For ``TSD`` inputs, children follow the
    effective key set. Fixed ``TSL`` inputs expand at wiring time and dynamic
    lists allocate stable children by index. An outputless ``func`` creates
    child sink graphs without allocating a result collection.

    :param func: Native operator or Python-authored graph/node instantiated for
        each key or index.
    :param args: Positional multiplexed or broadcast inputs.
    :param __label__: Optional mapped-scope label used by diagnostics.
    :param __keys__: Optional explicit ``TSS`` controlling TSD child lifetime;
        otherwise keys are inferred from multiplexed inputs.
    :param __key_arg__: Child parameter that receives the key/index. ``None``
        uses the conventional name and ``""`` disables key injection.
    :param kwargs: Named multiplexed or broadcast inputs.
    :return: A collection containing one child result per active key/index, or
        ``None`` when ``func`` is a sink.

    Example::

        notionals = map_(multiply, prices, quantities)
    """
    label = __label__
    if __keys__ is not None:
        kwargs["__keys__"] = __keys__
    if __key_arg__ is not None:
        kwargs["__key_arg__"] = __key_arg__
    wired, args, kwargs = _prepare_higher_order_call(
        func, args, kwargs, default_key_arg="key")
    if label:
        with _current_wiring()._graph_wiring_scope(str(label)):
            return wire("map_", wired, *args, **kwargs)
    return wire("map_", wired, *args, **kwargs)


def _mesh_name(fn_or_name):
    if isinstance(fn_or_name, str):
        return fn_or_name
    name = getattr(fn_or_name, "__name__", None)
    if not name:
        raise TypeError("get_mesh expects a mesh name or a named wiring function")
    return name


class MeshWiringPort(WiringPort):
    """A reference to the enclosing mesh, matching release/0.5's Python API.

    Indexing wires a sibling subscription. Treating the object as a normal
    time-series port lazily materializes the mesh key-set forwarding node.
    """

    __slots__ = ("_mesh_name", "_key_set_port")

    def __init__(self, name):
        self._mesh_name = name
        self._key_set_port = None

    @property
    def _port(self):
        if self._key_set_port is None:
            self._key_set_port = _hgraph.mesh_key_set_ref(
                _current_wiring(), self._mesh_name)
        return self._key_set_port

    @property
    def key_set(self):
        return WiringPort(self._port)

    def __bool__(self):
        # get_mesh returns None outside the named scope and a live handle
        # inside it; existing clients use that distinction before indexing.
        return True

    def __getitem__(self, item):
        return WiringPort(_hgraph.mesh_ref(
            _current_wiring(), _unwrap(item), self._mesh_name))


def get_mesh(fn_or_name):
    """Return the named enclosing mesh, or ``None`` outside its wiring scope."""
    name = _mesh_name(fn_or_name)
    return MeshWiringPort(name) if _hgraph.mesh_scope_exists(name) else None


def mesh_(func, *args, __name__=None, __keys__=None, __key_arg__=None, **kwargs):
    """Apply a graph per TSD key while allowing children to reference peers.

    ``mesh_(func, inputs...)`` constructs the mesh. Inside ``func``,
    ``mesh_(func)[key]`` or ``get_mesh(func)[key]`` reads another instance's
    output. Dependencies are ranked within each cycle and missing peer keys may
    create instances on demand.

    :param func: Per-key graph callable.
    :param args: Positional multiplexed or broadcast TSD inputs.
    :param __name__: Stable mesh name used for peer lookup; defaults to the
        callable name.
    :param __keys__: Optional explicit key set controlling child lifetime.
    :param __key_arg__: Child parameter receiving the key, or ``""`` to omit it.
    :param kwargs: Named multiplexed or broadcast inputs.
    :return: A keyed dictionary of child outputs. With no inputs, returns a
        reference to an enclosing mesh of the requested name.

    Example::

        ranked = mesh_(rank_with_peers, scores, __name__="scores")
    """
    if not args and not kwargs and __keys__ is None:
        return get_mesh(__name__ or func)

    kwargs = dict(kwargs)
    kwargs["__name__"] = __name__ or _mesh_name(func)
    if __keys__ is not None:
        kwargs["__keys__"] = __keys__
    if __key_arg__ is not None:
        kwargs["__key_arg__"] = __key_arg__
    wired, args, kwargs = _prepare_higher_order_call(
        func, args, kwargs, default_key_arg="key")
    return wire("mesh_", wired, *args, **kwargs)


def _reduce_nothing(ts):
    """Create the typed invalid identity used by an explicit ``zero=None``."""
    collection = _unwrap(ts).ts_type
    if collection.is_ref:
        collection = _hgraph.ref_target(collection)
    if collection.is_tsd:
        element = _hgraph.tsd_element_ts(collection)
    elif collection.is_tsl:
        element = _hgraph.tsl_element_ts(collection)
    else:
        raise WiringError("zero=None requires a TSD or TSL reduce input")
    return wire("nothing", output_type=element)


def reduce(func, ts, zero=_REDUCE_ZERO, is_associative=True, **kwargs):
    """Reduce live collection values.

    For an associative reduction, an omitted zero leaves an empty result
    invalid and returns a singleton directly. A supplied zero is returned for
    an empty collection and combined with a singleton; it is ignored when two
    or more live values are present. Ordered reduction uses its required zero
    as the initial accumulator.

    :param func: Binary graph combining two elements or accumulator values.
    :param ts: ``TSD`` or ``TSL`` whose live elements are reduced.
    :param zero: Optional empty/singleton identity for associative reduction.
        Ordered reduction requires a live time-series initial accumulator.
    :param is_associative: ``True`` permits tree reduction; ``False`` performs
        a deterministic ordered left fold.
    :param kwargs: Additional wiring options forwarded to native reduction.
    :return: The current reduction of the collection's live values.

    Example::

        total = reduce(add_, values, zero=0)
    """
    if zero is None:
        zero = _reduce_nothing(ts)
    if not is_associative:
        if zero is _REDUCE_ZERO or not isinstance(zero, WiringPort):
            raise WiringError(
                "Non-associative reduce requires a time-series zero/initial accumulator")

        raw_ts = _unwrap(ts)
        if raw_ts.ts_type.is_ts:
            value_type = _hgraph.ts_value_vt(raw_ts.ts_type)
            if _hgraph.vt_kind(value_type) == _unbounded_tuple_kind():
                element_type = _hgraph.vt_element(value_type)
                enumerated_type = _hgraph.tsd(
                    _hgraph.value_type("int"), _hgraph.ts(element_type))
                ts = wire("convert", ts, output_type=enumerated_type)

        kwargs["is_associative"] = False
    if zero is _REDUCE_ZERO:
        return wire("reduce", _as_wired(func), ts, **kwargs)
    return wire("reduce", _as_wired(func), ts, zero, **kwargs)


class Feedback:
    """hgraph's feedback: ``fb = feedback(TS[int])``; ``fb(port)`` binds the
    cycle-closing input; ``fb()`` reads the (next-cycle) source port."""

    __slots__ = ("_wiring", "_fb")

    def __init__(self, wiring, fb):
        self._wiring = wiring
        self._fb = fb

    def __call__(self, port=None):
        if port is None:
            return WiringPort(self._fb.port)
        self._wiring.feedback_bind(self._fb, _unwrap(port))
        return self


class DelayedBinding:
    """A typed wiring placeholder resolved before graph ranking.

    This changes wiring order only. It does not delay values or permit a
    dependency cycle; use ``feedback`` for that runtime behavior.
    """

    __slots__ = ("_wiring", "_binding")

    def __init__(self, wiring, binding):
        self._wiring = wiring
        self._binding = binding

    def __call__(self, port=None):
        if port is None:
            return WiringPort(self._binding.port)
        self._wiring.delayed_binding_bind(self._binding, _unwrap(port))


def passive(ts):
    """hgraph's passive marker: the receiving node's input for THIS usage is
    removed from its active list (ticks no longer schedule the node; values
    still read normally). Returns a marked copy - the original port is
    unaffected."""
    port = ts
    return WiringPort(_hgraph.passive(_unwrap(port)))


def pass_through(tsd):
    """map_/mesh_ pass-through marker: do NOT demultiplex this argument."""
    ts = tsd
    return WiringPort(_hgraph.pass_through_tag(_unwrap(ts)))


def no_key(tsd):
    """map_/mesh_ no-key marker: demultiplex, but exclude from key inference."""
    ts = tsd
    return WiringPort(_hgraph.no_key_tag(_unwrap(ts)))


def feedback(tp_or_wp, default=None):
    w = _current_wiring()
    if isinstance(tp_or_wp, WiringPort):
        port = tp_or_wp
        result = Feedback(w, w.feedback(_unwrap(port).ts_type, default))
        result(port)
        return result
    return Feedback(w, w.feedback(_unwrap(tp_or_wp), default))


def delayed_binding(tp_):
    """Create a typed source placeholder and bind its producer later."""
    tp_or_wp = tp_
    from .._types import _TsExpr

    w = _current_wiring()
    if isinstance(tp_or_wp, WiringPort):
        ts_type = _unwrap(tp_or_wp).ts_type
    elif isinstance(tp_or_wp, _TsExpr):
        ts_type = tp_or_wp.handle
    elif isinstance(tp_or_wp, _hgraph.TsType):
        ts_type = tp_or_wp
    else:
        raise TypeError("delayed_binding expects a time-series type or wiring port")
    return DelayedBinding(w, w.delayed_binding(ts_type))


def combine(*args, __output_type__=None, **kwargs):
    """hgraph's combine: build a composite port. The packing form comes from
    the ARGUMENT SHAPE alone (kwargs -> structural bundle, ports ->
    structural list, a leading keys tuple -> the keyed-TSD kernel); a
    generic target (bare TSD/TSS/TS[Tuple]/...) resolves through the C++
    type-pattern machinery (resolve_combine_target) and the RESOLVED type's
    ``from_ts`` picks the wiring - no type-name inspection anywhere."""
    from .._types import TSB, _TsExpr

    if args and isinstance(args[0], tuple) and len(args) > 1 and all(
            isinstance(a, WiringPort) for a in args[1:]):
        # The keys-literal shape: combine[TSD](("a", "b"), a, b). The C++
        # kernel resolves its own output from the keys + element ports.
        strict = kwargs.pop("__strict__", True)
        return wire("combine_tsd", args[0], *args[1:], __strict__=strict, **kwargs)
    if __output_type__ is None:
        strict = kwargs.pop("__strict__", None)
        if args and all(isinstance(a, WiringPort) for a in args) and not kwargs:
            # UNSUBSCRIPTED positional: a structural TSL of the ports, UNLESS
            # it is the binary CS-merge (two bundle-valued TS -> delta merge).
            if len(args) == 2 and all(_unwrap(a).ts_type.is_ts_bundle for a in args):
                return _combine_compound_scalars(*args)
            return WiringPort(_hgraph.tsl_port([_unwrap(a) for a in args]))
        if kwargs and not args and all(isinstance(v, WiringPort) for v in kwargs.values()):
            # hgraph's un-subscripted kwargs form: a structural un-named TSB;
            # __strict__=True gates it behind all-fields-valid (the erased
            # combine_tsb_strict kernel).
            fields = [(k, _unwrap(v).ts_type) for k, v in kwargs.items()]
            tsb_type = _hgraph.un_named_tsb_type(fields)
            structural = WiringPort(
                _hgraph.tsb_port(tsb_type, {k: _unwrap(v) for k, v in kwargs.items()}))
            if strict:
                return wire("combine", structural, __strict__=True)
            return structural
        raise TypeError("combine requires a subscripted type: combine[TSB[Schema]](...)")
    target = __output_type__
    if not isinstance(target, _TsExpr):
        # A bare generic target (combine[TSL], combine[TSS], ...) needs the
        # operand schemas before C++ can resolve its concrete output. Lift
        # plain values first so resolution sees the same ports the resolved
        # target's from_ts implementation will consume.
        args = tuple(a if isinstance(a, WiringPort) else wire("const", a) for a in args)
        kwargs = {
            name: value if name == "__strict__" or isinstance(value, WiringPort)
            else wire("const", value)
            for name, value in kwargs.items()
        }
    if target is TSB and not args:
        strict = kwargs.pop("__strict__", False)
        if not all(isinstance(value, WiringPort) for value in kwargs.values()):
            raise TypeError("combine[TSB] requires time-series or liftable scalar fields")
        fields = [(name, _unwrap(value).ts_type) for name, value in kwargs.items()]
        tsb_type = _hgraph.un_named_tsb_type(fields)
        structural = WiringPort(
            _hgraph.tsb_port(
                tsb_type, {name: _unwrap(value) for name, value in kwargs.items()}))
        if strict:
            return wire("combine", structural, __strict__=True)
        return structural
    ports = [a for a in args if isinstance(a, WiringPort)] or [
        v for k, v in kwargs.items() if k != "__strict__" and isinstance(v, WiringPort)]
    if ports or not isinstance(target, _TsExpr):
        # Every target - generic or concrete - resolves through the C++
        # pattern machinery (resolve_combine_target); concrete targets pass
        # through except where hgraph's rules rewrite them (tuple rows). The
        # ORIGINAL expr is kept when the handle is unchanged (it carries
        # target annotations like the CS class for dataclass defaults).
        resolved = _resolve_requested_target("combine", target, ports)
        if not (isinstance(target, _TsExpr) and resolved.handle == target.handle):
            target = resolved
    return target.from_ts(*args, **kwargs)


class _Combine:
    """Build one composite time-series value from component ports.

    Use ``combine[OUTPUT_TYPE](...)`` when the desired shape is not implied by
    argument structure. Positional ports build a structural list; named ports
    build a bundle; selected output types add tuple, mapping, TSD, TSS,
    compound-scalar, temporal, and JSON behaviours.

    ``__strict__=True`` suppresses structural output until all required
    components are valid. It is a wiring-time choice and does not add per-tick
    policy dispatch.

    :param args: Positional component ports or liftable values.
    :param kwargs: Named component ports plus supported wiring-time options.
    :return: A composite wiring port of the inferred or selected type.

    Example::

        point = combine[TSB[Point]](x=x, y=y)
        values = combine(a, b, c)
    """

    def __getitem__(self, item):
        def _build(*args, **kwargs):
            return combine(*args, __output_type__=item, **kwargs)

        return _build

    def __call__(self, *args, **kwargs):
        return combine(*args, **kwargs)


class DebugContext:
    """hgraph's wiring-scope debug printer: inside ``with DebugContext(
    prefix=..., debug=True):``, ``DebugContext.print(label, ts)`` wires a
    ``debug_print``; outside a context (or with ``debug=False``) it wires
    nothing."""

    _stack = []

    def __init__(self, prefix="", debug=True):
        self._prefix = prefix
        self._debug = debug

    def __enter__(self):
        DebugContext._stack.append(self)
        return self

    def __exit__(self, *exc):
        DebugContext._stack.pop()
        return False

    @classmethod
    def print(cls, label, ts, **kwargs):
        active = cls._stack[-1] if cls._stack else None
        if active is None or not active._debug:
            return
        full = f"{active._prefix} {label}" if active._prefix else label
        operator_function("debug_print")(full, ts, **kwargs)


def filter_by(ts, expr, **kwargs):
    """Keep keyed entries for which ``expr(value, **kwargs)`` is true.

    One predicate child is mapped over each active key. Additional named inputs
    are passed to every child using normal ``map_`` multiplex/broadcast rules;
    the native grouping then applies the live match dictionary to ``ts``.

    :param ts: Keyed time-series dictionary to filter.
    :param expr: Predicate graph receiving each value (and optionally its key).
    :param kwargs: Additional predicate inputs.
    :return: A TSD containing only keys whose current predicate is true.

    Example::

        expensive = hg.filter_by(prices, above_limit, limit=limit)
    """
    matches = map_(expr, ts, **kwargs)
    return wire("filter_tsd_by_matches", ts, matches)


def _bind_switch_scalar_args(branch, args, kwargs):
    """Capture Python scalar branch arguments before creating a C++ WiredFn.

    WiredFn deliberately models only runtime time-series inputs. Python graph
    callables may additionally accept wiring-time scalar configuration, so an
    adapter closes over those values and exposes only the remaining TS inputs
    to the native switch compiler.
    """
    if isinstance(branch, (str, _hgraph.WiredFn)):
        raise TypeError(
            "switch_ scalar arguments require Python callable branches; "
            "capture scalar configuration in a C++ branch type instead")

    target = getattr(branch, "fn", branch)
    signature = inspect.signature(target)
    parameters = list(signature.parameters.values())
    takes_key = bool(parameters) and parameters[0].name == "key"
    key_marker = object()
    bound = signature.bind(*((key_marker, *args) if takes_key else args), **kwargs)
    bound.apply_defaults()

    dynamic_names = []
    dynamic_parameters = []
    for parameter in parameters:
        value = bound.arguments[parameter.name]
        if value is key_marker or isinstance(value, WiringPort):
            dynamic_names.append(parameter.name)
            dynamic_parameters.append(parameter)
        elif parameter.kind in (inspect.Parameter.VAR_POSITIONAL,
                                inspect.Parameter.VAR_KEYWORD):
            raise TypeError("switch_ scalar binding does not support variadic branch parameters")

    captured = dict(bound.arguments)

    def adapter(*dynamic_args, **dynamic_kwargs):
        dynamic_bound = inspect.Signature(dynamic_parameters).bind(
            *dynamic_args, **dynamic_kwargs)
        call_bound = signature.bind_partial()
        call_bound.arguments.update(captured)
        call_bound.arguments.update(dynamic_bound.arguments)
        return branch(*call_bound.args, **call_bound.kwargs)

    # _as_wired intentionally accepts anonymous convenience callables. The
    # precise signature is what lets C++ bind its key and TS slots normally.
    adapter.__name__ = "<lambda>"
    adapter.__signature__ = signature.replace(parameters=dynamic_parameters)
    return adapter


def switch_(key, cases, *args, reload_on_ticked=False, **kwargs):
    """Run one child graph selected by the current key.

    A key change stops and destroys the old branch, builds the selected branch,
    and repoints the output to it. Use ``DEFAULT`` as a fallback case. A branch
    whose first parameter is named ``key`` receives the selector value.

    :param key: Live branch selector.
    :param cases: Mapping from selector values to graph callables; ``DEFAULT``
        supplies the fallback.
    :param args: Positional inputs forwarded to the active branch.
    :param reload_on_ticked: Rebuild even when the key ticks with the same value.
    :param kwargs: Named inputs forwarded to the active branch.
    :return: The active child graph output, or ``None`` for sink branches.

    Example::

        selected = switch_(mode, {"live": live_graph, DEFAULT: cached_graph}, source)
    """
    from .._types import DEFAULT

    has_scalar_args = any(not isinstance(value, WiringPort) for value in args)
    has_scalar_args = has_scalar_args or any(
        not isinstance(value, WiringPort) for value in kwargs.values())
    ts_args = tuple(value for value in args if isinstance(value, WiringPort))
    ts_kwargs = {name: value for name, value in kwargs.items()
                 if isinstance(value, WiringPort)}

    prepared = {}
    for case_key, branch in cases.items():
        if case_key is DEFAULT:
            case_key = None   # hgraph's DEFAULT marker = the default branch
        if has_scalar_args:
            branch = _bind_switch_scalar_args(branch, args, kwargs)
        prepared[case_key] = branch if isinstance(branch, str) else _as_wired(branch)
    erased = _hgraph.switch_cases(
        prepared, reload=reload_on_ticked, key_type=_unwrap(key).ts_type)
    return wire("switch_", key, erased, *ts_args, **ts_kwargs)


def _type_pattern_for_target(target):
    from .._types import _GenericTsExpr, _TsExpr, TSD, TSB, TSL, TSS, TSW

    if isinstance(target, _TsExpr):
        return _hgraph.type_pattern_concrete(target.handle)
    if isinstance(target, _GenericTsExpr):
        if target.pattern is None:
            raise WiringError(f"cannot resolve generic target {target!r}: no C++ type pattern is attached")
        return target.pattern
    if target is TSS:
        return _hgraph.type_pattern_tss()
    if target is TSD:
        return _hgraph.type_pattern_tsd()
    if target is TSL:
        return _hgraph.type_pattern_tsl()
    if target is TSB:
        return _hgraph.type_pattern_tsb()
    if target is TSW:
        return _hgraph.type_pattern_tsw()
    raise WiringError(f"unsupported generic target {target!r}")


def _resolve_requested_target(op_name, target, inputs, keys=None):
    from .._types import _TsExpr

    pattern = _type_pattern_for_target(target)
    unwrapped = tuple(_unwrap(p) for p in inputs)
    try:
        if op_name == "collect":
            handle = _hgraph.resolve_collect_target(pattern, unwrapped)
        elif op_name == "combine":
            handle = _hgraph.resolve_combine_target(pattern, unwrapped)
        else:
            handle = _hgraph.resolve_convert_target(pattern, unwrapped, keys)
    except (RuntimeError, ValueError) as error:
        raise WiringError(str(error)) from error
    return _TsExpr(handle, "inferred")


# Adaptor-provided convert targets (e.g. Frame - hgraph.adaptors.data_frame):
# each handler is tried with (target, inputs, kwargs) BEFORE registry
# resolution and returns a WiringPort to claim the call or None to decline.
# This mirrors upstream, where the frame converters register only when the
# adaptor module is imported; target kinds the registry cannot pattern-match
# (frame-of-schema has no generic pattern) resolve here at wiring instead.
_convert_target_handlers = []


def register_convert_target_handler(handler):
    if handler not in _convert_target_handlers:
        _convert_target_handlers.append(handler)


class _Convert:
    """hgraph's convert: ``convert[TO](ts)`` or ``convert(ts, to)``. The
    target may be GENERIC (bare TSD/TSS/TSB, unparameterized TS[Tuple] /
    TS[Set] / TS[Mapping]) - the full output type is inferred from the
    INPUT port at wiring, so the C++ overloads always see a bound __out__."""

    __name__ = "convert"

    def __init__(self, to=None):
        self._to = to

    def __getitem__(self, item):
        return _Convert(item)

    def __call__(self, ts=None, *ports, to=None, **kwargs):
        from .._types import _TsExpr, _GenericTsExpr

        # Collect the time-series inputs in call order. hgraph names them
        # ``key``/``ts`` for the multi-input converts; positionally the first
        # is ``ts`` (or the key), extras follow. ``ts`` is a real param so
        # eval_node/resolution_dict can type it.
        inputs = []
        if "key" in kwargs:
            inputs.append(kwargs.pop("key"))
        inputs.append(ts if ts is not None else kwargs.pop("ts", None))
        inputs = [p for p in inputs if p is not None] + list(ports)
        if to is None and inputs and isinstance(
                inputs[-1], (_TsExpr, _GenericTsExpr, type)) and not isinstance(inputs[-1], WiringPort):
            to = inputs.pop()   # hgraph's positional ``to`` type argument
        target = to if to is not None else self._to

        if target is None:
            raise WiringError("convert requires a target type")

        def _registry_convert(resolved_target):
            if not isinstance(resolved_target, _TsExpr):
                resolved_target = _resolve_requested_target(
                    "convert", resolved_target, inputs, keys=kwargs.get("keys"))
            if (len(inputs) == 1 and isinstance(inputs[0], WiringPort)
                    and _unwrap(inputs[0]).ts_type == resolved_target.handle):
                return inputs[0]   # convert to the SAME type is a no-op (hgraph: j is i)
            return wire("convert", *inputs, output_type=resolved_target, **kwargs)

        # Registered target handlers are a FALLBACK for target shapes the C++
        # registry has no pattern for (frame-of-schema targets); registry
        # resolution stays the primary path.
        try:
            return _registry_convert(target)
        except (WiringError, TypeError, ValueError, RuntimeError) as registry_error:
            for handler in _convert_target_handlers:
                port = handler(target, inputs, kwargs)
                if port is not None:
                    return port
            raise registry_error

convert = _Convert()


def _TsExprFor(handle):
    from .._types import _TsExpr

    return _TsExpr(handle, "inferred")


class _Collect:
    """hgraph's collect: accumulate ticks into a growing collection.
    ``collect[TS[Set]](ts, reset=...)`` / ``collect[TSD](k, v, reset=...)``.
    Generic targets infer from the input ports."""

    __name__ = "collect"

    def __init__(self, to=None):
        self._to = to

    def __getitem__(self, item):
        return _Collect(item)

    def _infer(self, ports):
        from .._types import _TsExpr, TS

        target = self._to
        if isinstance(target, _TsExpr):
            return target
        if target is None:
            raise WiringError("collect requires a target type")
        return _resolve_requested_target("collect", target, ports)

    def __call__(self, *ports, reset=None, exclude=None, **kwargs):
        # Absent reset/exclude take the kernels' None defaults (null
        # sources) - no python-side nothing-wiring.
        target = self._infer(ports)
        if reset is not None:
            kwargs["reset"] = reset
        if exclude is not None:
            kwargs["exclude"] = exclude
        return wire("collect", *ports, output_type=target, **kwargs)


collect = _Collect()


class _Emit:
    """hgraph's emit: drain a collection/dict one element per cycle.
    ``emit(x)`` infers the output; ``emit[VALUE_TS](x)`` hints the per-key
    VALUE time-series type for a dict/mapping (the KeyValue value field -
    e.g. emit[TSL[TS[int], Size[2]]](tsd_of_tuples))."""

    __name__ = "emit"

    def __init__(self, value_ts=None):
        self._value_ts = value_ts

    def __getitem__(self, item):
        return _Emit(item)

    def __call__(self, ts, **kwargs):
        from .._types import _TsExpr

        if self._value_ts is None or not isinstance(self._value_ts, _TsExpr):
            return wire("emit", ts, **kwargs)
        # The hinted KeyValue output resolves in C++ (the target-resolution
        # home): {key: TS[K], value: <value_ts>} with K from the dict input.
        out = _TsExprFor(_hgraph.resolve_emit_target(self._value_ts.handle, (_unwrap(ts),)))
        return wire("emit", ts, output_type=out, **kwargs)


emit = _Emit()

from .._types import SCALAR, SCALAR_1, TS
from ._operator import _Operator


def cast_(tp: type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """Convert ``ts`` to ``tp`` through an overloadable operator family."""


class _CastOperator(_Operator):
    def __call__(self, tp, ts):
        # The scalar carrier determines the requested output. Supplying that
        # schema lets the C++ registry resolve output-only SCALAR.
        return self._delegate(tp, ts, output_type=TS[tp])


cast_ = _CastOperator(cast_)


def downcast_(tp: type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """Narrow ``ts`` to the derived CompoundScalar type ``tp``.

    The explicit target argument is the release/0.5-compatible spelling.
    ``downcast_[TS[Derived]](ts)`` is also supported.
    """


def _selected_downcast(ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """Signature carrier for ``downcast_[TS[Derived]](ts)``."""


class _SelectedDowncastOperator(_OperatorFunction):
    def __init__(self, selected):
        super().__init__(
            selected.__name__,
            output_type=selected._output_type,
            sizes=selected._sizes,
            ts_hint=selected._ts_hint,
            resolutions=selected._resolutions,
            signature=_selected_downcast,
        )

    def __call__(self, ts: TS[SCALAR_1]) -> TS[SCALAR]:
        return super().__call__(ts)


class _DowncastOperator(_OperatorFunction):
    def __init__(self):
        super().__init__("downcast_", signature=downcast_)

    def __call__(self, tp: type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
        # ``tp`` is Python authoring syntax. The native operator receives the
        # source port plus the fully resolved output schema and owns the
        # checked Bundle narrowing at runtime.
        return super().__call__(ts, output_type=TS[tp])

    def __getitem__(self, item) -> _SelectedDowncastOperator:
        return _SelectedDowncastOperator(super().__getitem__(item))


downcast_ = _DowncastOperator()


from ._graph import graph


@graph(overloads=cast_)
def _cast_default(tp: type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    # This compatibility operator is only a wiring surface. Its default
    # behavior is the native C++ conversion family.
    return convert(ts, to=TS[tp])


def downcast_ref(tp, ts):
    """Narrow a time-series reference to ``TS[tp]`` without inspecting its
    current value. Runtime storage and forwarding remain owned by C++."""
    from .._types import REF, TS

    source = WiringPort(_hgraph.ref_port(_current_wiring(), _unwrap(ts)))
    return wire("downcast_ref", source, output_type=REF[TS[tp]])

def _merge_cs(orig, delta):
    """Recursive right-over-left CompoundScalar merge (hgraph's
    combine_compound_scalars): delta's None fields keep the original."""
    import dataclasses

    if delta is None:
        return orig
    merged = {}
    for field in dataclasses.fields(orig):
        original = getattr(orig, field.name)
        update = getattr(delta, field.name, None)
        if update is None:
            merged[field.name] = original
        elif dataclasses.is_dataclass(update) and dataclasses.is_dataclass(original):
            merged[field.name] = _merge_cs(original, update)
        else:
            merged[field.name] = update
    return type(orig)(**merged)


def _combine_compound_scalars(orig, delta):
    # C++-first ruling (2026-07-06): CS = C++ Bundle value; the merge is
    # the erased C++ combine over bundle scalars.
    return wire("combine", orig, delta)
