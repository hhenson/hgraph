"""Wiring stack, ports, the erased ``wire`` verb and the wiring errors.

Wiring state is a module-level stack. Top-level ``run_graph`` wiring is
serialized because the stack and native operator registry are process-wide;
the serialization ends once the native executor has been built, so distinct
executors may run concurrently. ``_wiring_stack`` is THE single list object:
tests and C++ re-entry mutate it in place; nothing may rebind it."""
import re
import threading

import _hgraph

from .._operator_signature import PublicTypePatternFormatter
from .._types import _TsExpr
from ._sentinels import _REDUCE_ZERO
from ._state import _active_global_state

_wiring_stack = []
_wiring_lock = threading.RLock()


def _current_wiring():
    if not _wiring_stack:
        raise RuntimeError("no active wiring: wire inside a @graph run by run_graph/eval_node")
    return _wiring_stack[-1]


def is_realtime():
    """Whether the active graph is being wired for real-time execution."""
    return _current_wiring().is_realtime


# Operators whose durable overloads register from hgraph-persistence
# (RFC 0025). replay_const has no in-memory implementation at all.
_DURABLE_OPERATORS = frozenset({"record", "replay", "compare", "replay_const"})

# Wiring-time kwargs adaptation for record/replay, registered by
# hgraph_persistence.compat (the 0.5 override-registry translation). Module
# state, not registry state: it survives native registry resets, exactly like
# the compat storage classes that install it.
_record_replay_wiring_adapter = None


def register_record_replay_wiring_adapter(adapter):
    """Install the record/replay wiring-kwargs adapter (RFC 0025 compat seam)."""
    global _record_replay_wiring_adapter
    _record_replay_wiring_adapter = adapter


# Operators with NO in-memory implementation: core declares the contract and
# only hgraph-persistence registers an overload, so a resolution failure on
# one of these is a candidate for the missing-extension diagnosis whatever
# backend is configured.
_EXTENSION_ONLY_OPERATORS = frozenset({"replay_const"})

# The native resolver reports a resolution failure two ways, and a missing
# extension shows up as EITHER: overloads exist but none matched the call, or
# the operator marker itself is absent because only the extension registers
# one. Anything else -- an argument type error, a bad key -- is not about
# which overloads are registered and must not attract install advice.
_NO_MATCHING_OVERLOAD = "no matching overload for operator"
_NO_SUCH_OPERATOR = re.compile(r"no operator '[^']*' is registered")


def _is_resolution_failure(message):
    return _NO_MATCHING_OVERLOAD in message or _NO_SUCH_OPERATOR.search(message) is not None


def _durable_backend_selected(kwargs):
    """Whether a durable backend is actually in play for this call."""
    model = (kwargs or {}).get("model")
    if model is None:
        try:
            from ._state import GlobalState

            model = _active_global_state().get("__record_replay_model__")
        except Exception:  # no active global state — nothing was selected
            return False
    if model is None:
        return False
    try:
        from ._state import _LEGACY_BACKENDS, _LEGACY_DATA_FRAME_RECORD_REPLAY

        if model == _LEGACY_DATA_FRAME_RECORD_REPLAY:
            model = _hgraph.DATA_FRAME
        model = _LEGACY_BACKENDS.get(model, model)
    except Exception:  # translation is best-effort; the id check still holds
        pass
    # The SAME prefix the load point in _ensure_backend_extension recognises.
    # Backend selection is open, so an independently registered backend may
    # legitimately use another "hgraph.*" id; blaming persistence for it would
    # be the very misattribution this function exists to stop.
    return isinstance(model, str) and model.startswith("hgraph.persistence.")


def _durable_wiring_hint(name, kwargs=None, message=""):
    """The missing-extension diagnosis for a durable operator's wiring failure.

    Wiring diagnoses the missing backend (RFC 0025), but only where the durable
    path is genuinely implicated. Two gates keep it honest:

    * The failure must be a resolution failure. Appending install advice to an
      argument-type error sends the caller after a distribution they do not
      need.
    * Selecting a durable backend IS the extension load point, at graph level
      or per call, and an absent distribution raises a pointed error THERE. So
      for ``record``/``replay``/``compare`` an unloaded extension means no
      durable backend was ever selected and the failure is unrelated. Only
      ``replay_const``, which has no in-memory implementation at all, can fail
      this way without a durable selection.

    Availability is probed without importing (importing would register the
    overloads as an error-path side effect).
    """
    if name not in _DURABLE_OPERATORS:
        return ""
    if not _is_resolution_failure(message):
        return ""
    if name not in _EXTENSION_ONLY_OPERATORS and not _durable_backend_selected(kwargs):
        return ""
    import importlib.util
    import sys

    if "hgraph_persistence" in sys.modules:
        return ""
    try:
        spec = importlib.util.find_spec("hgraph_persistence")
    except ModuleNotFoundError:
        spec = None
    if spec is None:
        return (
            "\n(durable record/replay overloads are provided by the optional "
            "'hgraph-persistence' distribution; install it with "
            "`pip install hgraph-persistence` — the built-in backends are "
            "'memory' and 'testing')"
        )
    return (
        "\n(hgraph-persistence is installed but not loaded: durable overloads "
        "register when a durable backend is selected — for example "
        "hg.set_record_replay_config(hg.DATA_FRAME) or a per-call "
        "model=hg.DATA_FRAME — or on `import hgraph_persistence`)"
    )


class WiringPort:
    """A time-series edge source; supports hgraph's operator sugar."""

    __slots__ = ("_port",)

    def __init__(self, port):
        self._port = port

    @property
    def output_type(self):
        """The resolved time-series type carried by this wiring port."""
        from .._types import _TsExpr

        return _TsExpr(self._port.ts_type, repr(self._port.ts_type))


def _unwrap(value):
    if isinstance(value, WiringPort):
        return value._port
    if isinstance(value, _TsExpr):
        return value.handle
    return value


def wire(name, *args, __output_type__=None, **kwargs):
    """Wire operator ``name`` by registry resolution (the erased contract)."""
    out_type = kwargs.pop("tp", None) or kwargs.pop("output_type", None) or __output_type__
    if out_type is not None:
        if isinstance(out_type, type):
            from .._types import TS as _TS

            out_type = _TS[out_type]   # json_encode[str] names the SCALAR
        out_type = out_type.handle if isinstance(out_type, _TsExpr) else out_type
    w = _current_wiring()
    node_label = kwargs.pop("__node_label__", None)
    sizes = kwargs.pop("__sizes__", None)
    resolutions = kwargs.pop("__resolutions__", None)
    resolution_scope = None
    if resolutions:
        from .._types import _value_type

        resolution_scope = _hgraph.ResolutionScope()
        for variable, resolved in resolutions.items():
            if isinstance(resolved, _TsExpr):
                resolution_scope.bind_ts(variable, resolved.handle)
            elif isinstance(resolved, _hgraph.TsType):
                resolution_scope.bind_ts(variable, resolved)
            elif isinstance(resolved, int) and not isinstance(resolved, bool):
                resolution_scope.bind_size(variable, resolved)
            else:
                resolution_scope.bind_scalar(variable, _value_type(resolved))
    unwrapped = tuple(_unwrap(a) for a in args)
    unwrapped_kw = {k: _unwrap(v) for k, v in kwargs.items()}
    try:
        # Plain-value kwargs falling to a **kwargs collector lift to const
        # sources inside the C++ call normalisation (the scalar-kwargs rule) -
        # no python-side retry.
        wire_options = {"output_type": out_type}
        if node_label:
            wire_options["node_label"] = str(node_label)
        if sizes is not None:
            wire_options["sizes"] = sizes
        if resolution_scope is not None:
            wire_options["initial_resolution"] = resolution_scope
        result = w.wire(name, unwrapped, unwrapped_kw, **wire_options)
    except (RuntimeError, ValueError) as error:
        # std::invalid_argument surfaces as ValueError; both are wiring-time.
        # (RequirementsNotMetWiringError arrives ALREADY typed - the C++
        # resolver throws OperatorRequirementsError, translated directly.)
        message = str(error)
        raise WiringError(message + _durable_wiring_hint(name, kwargs, message)) from error
    return WiringPort(result) if result is not None else None


class _OperatorDefault:
    def __repr__(self):
        return "..."


_OPERATOR_DEFAULT = _OperatorDefault()


def _to_json_public_signature(ts, delta=False):
    """The released public call shape; native overloads select value vs delta."""


def _from_json_public_signature(ts):
    """The released public call shape; the subscript selects the output type."""


_PUBLIC_OPERATOR_SIGNATURES = {
    "to_json": _to_json_public_signature,
    "from_json": _from_json_public_signature,
}


def _operator_carrier_positions(name):
    """Positions at which the native family declares a type argument."""
    try:
        return set(_hgraph.operator_carrier_parameters(name)[1])
    except AttributeError:
        # Allows source imports against an older extension while rebuilding.
        return set()


def _operator_overload_signatures(name):
    try:
        raw_signatures = _hgraph.operator_overload_signatures(name)
    except AttributeError:
        # Allows source imports against an older extension while rebuilding.
        return ()
    signatures = []
    for raw in raw_signatures:
        (parameters, variadic, positional_params, has_kwargs,
         kwargs_pattern, has_output, output_pattern) = raw
        signatures.append({
            "parameters": tuple({
                "name": parameter_name,
                "is_time_series": bool(is_time_series),
                "type_pattern": type_pattern,
                "has_default": bool(has_default),
                # A type argument's carrier form (RFC 0033): None for a value.
                "type_argument": type_argument,
            } for parameter_name, is_time_series, type_pattern, has_default, type_argument
              in parameters),
            "variadic": bool(variadic),
            "positional_params": int(positional_params),
            "has_kwargs": bool(has_kwargs),
            "kwargs_pattern": kwargs_pattern,
            "has_output": bool(has_output),
            "output_pattern": output_pattern,
        })
    return tuple(signatures)


def _operator_signature_key(signature):
    return (
        tuple((parameter["name"], parameter["is_time_series"], parameter["type_pattern"],
               parameter["has_default"], parameter["type_argument"])
              for parameter in signature["parameters"]),
        signature["variadic"],
        signature["positional_params"],
        signature["has_kwargs"],
        signature["kwargs_pattern"],
        signature["has_output"],
        signature["output_pattern"],
    )


def _unique_operator_signatures(signatures):
    return tuple(dict.fromkeys(_operator_signature_key(signature) for signature in signatures))


def _pattern_category(is_time_series, type_argument):
    """The public-vocabulary category of a parameter's pattern: a type
    argument's variables are named by the form it carries (``type[OUT]``,
    not ``type[SCALAR_1]``)."""
    if is_time_series or type_argument in ("time-series", "size"):
        return "time_series"
    return "scalar"


def _format_operator_signature(name, signature_key):
    (raw_parameters, variadic, positional_params, has_kwargs,
     kwargs_pattern, has_output, output_pattern) = signature_key
    parameters = list(raw_parameters)
    formatter = PublicTypePatternFormatter()
    # Name the time-series inputs first, then the output, so a type argument
    # that carries the output type renders as ``type[OUT]`` rather than
    # minting a fresh name; the formatter's naming is idempotent.
    for _, is_time_series, type_pattern, _, _ in parameters:
        if is_time_series:
            formatter.format(type_pattern, category="time_series")
    if has_output:
        formatter.format(output_pattern, category="time_series", output=True)
    variadic_parameter = parameters.pop() if variadic and parameters else None
    positional_count = min(positional_params, len(parameters))
    rendered = []
    for index, (parameter_name, is_time_series, type_pattern, has_default, type_argument) in enumerate(
            parameters[:positional_count]):
        parameter_name = parameter_name or f"arg{index}"
        type_pattern = formatter.format(
            type_pattern, category=_pattern_category(is_time_series, type_argument))
        rendered.append(f"{parameter_name}: {type_pattern}{' = ...' if has_default else ''}")
    if variadic_parameter is not None:
        parameter_name, is_time_series, type_pattern, _, type_argument = variadic_parameter
        parameter_name = parameter_name or "args"
        type_pattern = formatter.format(
            type_pattern, category=_pattern_category(is_time_series, type_argument))
        rendered.append(f"*{parameter_name}: {type_pattern}")
    elif positional_count < len(parameters):
        rendered.append("*")
    for index, (parameter_name, is_time_series, type_pattern, has_default, type_argument) in enumerate(
            parameters[positional_count:], start=positional_count):
        parameter_name = parameter_name or f"arg{index}"
        type_pattern = formatter.format(
            type_pattern, category=_pattern_category(is_time_series, type_argument))
        rendered.append(f"{parameter_name}: {type_pattern}{' = ...' if has_default else ''}")
    if has_kwargs:
        kwargs_pattern = formatter.format(
            kwargs_pattern or "time-series", category="time_series")
        rendered.append(f"**kwargs: {kwargs_pattern}")
    output = (
        formatter.format(output_pattern, category="time_series", output=True)
        if has_output else "None"
    )
    return f"{name}({', '.join(rendered)}) -> {output}"


def _operator_documentation(name, signatures):
    signature_keys = _unique_operator_signatures(signatures)
    try:
        from .._operator_docs import OPERATOR_DOCS
    except ImportError:
        # Bootstrap while regenerating the checked documentation module.
        OPERATOR_DOCS = {}

    lines = [
        OPERATOR_DOCS.get(name, f"Wire ``{name}`` through native overload resolution."),
        "",
        "Accepted native overloads:",
        "",
    ]
    if signature_keys:
        rendered_signatures = dict.fromkeys(
            _format_operator_signature(name, signature) for signature in signature_keys
        )
        lines.extend(f"- {signature}" for signature in rendered_signatures)
    else:
        lines.append(f"- {name}(*args, **kwargs)")
    lines.extend([
        "",
        "Time-series parameters accept wiring ports and compatible plain values",
        "that can be lifted to constant sources.",
        "Generic names use the public Python vocabulary: SCALAR for scalar",
        "payloads, TIME_SERIES_TYPE for complete time-series types, SIZE for a",
        "fixed TSL length, and OUT for an output inferred during wiring.",
    ])
    return "\n".join(lines)


def _operator_runtime_signature(signatures):
    import inspect
    import keyword

    if not signatures:
        return None

    def structural_key(signature):
        parameters = signature["parameters"]
        return (
            tuple((parameter["name"], parameter["has_default"])
                  for parameter in parameters),
            signature["variadic"],
            signature["positional_params"],
            signature["has_kwargs"],
        )

    if len({structural_key(signature) for signature in signatures}) != 1:
        return None

    first = signatures[0]
    declared = list(first["parameters"])
    variadic_parameter = declared.pop() if first["variadic"] and declared else None
    positional_count = min(first["positional_params"], len(declared))
    parameters = []
    names = [parameter["name"] for parameter in declared]
    if variadic_parameter is not None:
        names.append(variadic_parameter["name"])
    if (
        any(not name or not name.isidentifier() or keyword.iskeyword(name) for name in names)
        or len(names) != len(set(names))
    ):
        return None

    def annotation_at(index):
        candidates = [signature["parameters"][index] for signature in signatures]
        is_time_series = candidates[0]["is_time_series"]
        if not all(candidate["is_time_series"] == is_time_series for candidate in candidates):
            return object
        return WiringPort | object if is_time_series else object

    for index, parameter in enumerate(declared):
        kind = (inspect.Parameter.POSITIONAL_OR_KEYWORD
                if index < positional_count else inspect.Parameter.KEYWORD_ONLY)
        parameters.append(inspect.Parameter(
            parameter["name"],
            kind,
            default=_OPERATOR_DEFAULT if parameter["has_default"] else inspect.Parameter.empty,
            annotation=annotation_at(index),
        ))
    if variadic_parameter is not None:
        variadic_index = len(first["parameters"]) - 1
        parameters.insert(positional_count, inspect.Parameter(
            variadic_parameter["name"],
            inspect.Parameter.VAR_POSITIONAL,
            annotation=annotation_at(variadic_index),
        ))
    if first["has_kwargs"]:
        parameters.append(inspect.Parameter(
            "kwargs", inspect.Parameter.VAR_KEYWORD, annotation=WiringPort | object
        ))
    outputs = {signature["has_output"] for signature in signatures}
    return_annotation = (
        WiringPort if outputs == {True}
        else None if outputs == {False}
        else WiringPort | None
    )
    try:
        return inspect.Signature(parameters, return_annotation=return_annotation)
    except ValueError:
        # Native normalisation can express call layouts which one Python
        # Signature cannot (notably a default before a later required input).
        return None


class _OperatorFunction:
    def __rshift__(self, other):
        # arrow-chain sugar: (eval_ | op >> assert_) - uplift into the arrow
        # module's combinator so operators compose in pipelines.
        from ..arrow import arrow

        return arrow(self) >> other

    def __lshift__(self, other):
        # arrow bind sugar: op << const_(x)  ==  i / const_(x) >> op
        from ..arrow import arrow

        return arrow(self) << other

    """A Python callable wiring the named registered operator. Supports
    hgraph's SUBSCRIPT form ``op[TYPE](...)`` - the type becomes the
    requested output type of the call."""

    __slots__ = ("__dict__", "__name__", "__qualname__", "__signature__", "_output_type",
                 "_sizes", "_ts_hint", "_resolutions")

    def __init__(self, name, output_type=None, sizes=None, ts_hint=None,
                 resolutions=None, signature=None, documentation=None):
        import inspect

        self.__name__ = name
        self.__qualname__ = name
        signatures = _operator_overload_signatures(name) if documentation is None else ()
        self.__signature__ = (
            inspect.signature(signature)
            if callable(signature)
            else signature if signature is not None
            else _operator_runtime_signature(signatures)
        )
        self.__doc__ = (
            documentation
            if documentation is not None
            else _operator_documentation(name, signatures)
        )
        self._output_type = output_type
        self._sizes = sizes
        self._ts_hint = ts_hint
        self._resolutions = resolutions

    def __call__(self, *args, **kwargs):
        if self.__name__ in _DURABLE_OPERATORS and kwargs.get("model"):
            # A per-call ``model=`` selects the backend for this node alone,
            # so it is an extension load point exactly like the graph-level
            # configuration setter (RFC 0025) — without the import, the
            # durable overloads are unregistered and resolution fails even
            # with hgraph-persistence installed.
            from ._state import _ensure_backend_extension

            _ensure_backend_extension(kwargs["model"])
        # hgraph parity: a trailing ``None`` argument means "use the
        # parameter's default" (upstream defaults optional scalars to None).
        while args and args[-1] is None:
            args = args[:-1]
        # These three compatibility APIs declare a positional type carrier.
        # Normalize it before the record/replay adapter inspects positional
        # arguments so replay(key, tp, recordable_id) presents the adapter
        # with the native (key, recordable_id) call shape.
        args, kwargs = self._normalise_type_arguments(args, kwargs)
        if self.__name__ in ("record", "replay") and _record_replay_wiring_adapter is not None:
            # release/0.5's data-frame override registry is translated at the
            # Python wiring boundary into native scalar options. The adapter is
            # registered by hgraph_persistence.compat when its storage surface
            # loads (RFC 0025: core wiring must not import adaptor modules) and
            # is a no-op unless a compatibility storage is active.
            kwargs = _record_replay_wiring_adapter(self.__name__, args, kwargs)
        if (self.__name__ == "apply" and args
                and "tp" not in kwargs and "output_type" not in kwargs
                and self._output_type is None and callable(args[0])
                and not isinstance(args[0], WiringPort)):
            import inspect
            from .._types import TS

            result_type = inspect.signature(args[0], eval_str=True).return_annotation
            if result_type is not inspect.Signature.empty:
                kwargs["output_type"] = TS[result_type]
        if (self.__name__ == "const" and args
                and "tp" not in kwargs and "output_type" not in kwargs):
            from .._compat import CompoundScalar
            from .._types import TS, _GenericType, _value_type

            if isinstance(args[0], CompoundScalar):
                # Schema-free C++ value inference intentionally treats an
                # arbitrary Python object as ``object``. A CompoundScalar's
                # Python class is its nominal Bundle schema, so retain that
                # information at the Python boundary before wiring const.
                kwargs["output_type"] = TS[type(args[0])]
            else:
                # Ensure an arbitrary Python class has a nominal registration
                # before native schema-free inference sees the value. Native
                # inference still owns precedence for containers, callables,
                # Arrow values, and every other concrete representation.
                try:
                    _value_type(type(args[0]))
                except _GenericType:
                    pass
        if self._output_type is not None and "tp" not in kwargs and "output_type" not in kwargs:
            kwargs["output_type"] = self._output_type
        if self._sizes is not None:
            kwargs.setdefault("__sizes__", self._sizes)
        if self._resolutions is not None:
            kwargs.setdefault("__resolutions__", self._resolutions)
        # A pinned lookup of a non-storage CompoundScalar attribute can only
        # select the Python descriptor node. Avoid repeating full getattr_
        # overload resolution for this common computed-field form; stored
        # fields and unpinned lookups retain ordinary registry inference.
        if (self.__name__ == "getattr_" and 2 <= len(args) <= 3
                and isinstance(args[0], WiringPort)
                and isinstance(args[1], str)
                and self._resolutions is not None
                and "SCALAR" in self._resolutions
                and set(kwargs).issubset({"output_type", "__resolutions__"})):
            raw = _unwrap(args[0])
            if raw.ts_type.is_ts:
                value_type = _hgraph.ts_value_vt(raw.ts_type)
                fields = getattr(value_type, "fields", ())
                if fields and args[1] not in {name for name, _ in fields}:
                    from ..nodes import _getattr_compound_descriptor

                    return _getattr_compound_descriptor._with_resolution(
                        self._resolutions)(*args)
        # Fixed-TSL integer access is a structural projection, not a runtime
        # node. Keep direct calls to getitem_ consistent with ``port[index]``;
        # other shapes and dynamic keys fall back to the registered operator.
        if (self.__name__ == "getitem_" and len(args) == 2
                and isinstance(args[0], WiringPort)
                and isinstance(args[1], int) and not isinstance(args[1], bool)):
            raw = _unwrap(args[0])
            if raw.ts_type.is_fixed_tsl:
                return WiringPort(_hgraph.tsl_element(raw, args[1]))
        return wire(self.__name__, *args, **kwargs)

    def __getitem__(self, item):
        # hgraph's ``op[TYPEVAR: TYPE]`` pre-resolution subscripts arrive as
        # SLICES. ``op[OUT: X]`` names the requested OUTPUT type; other
        # typevar slices are dropped (resolution happens from the wired
        # inputs). A plain type subscript is the requested output type.
        from .._types import OUT, TS, _type_var_name

        # ``with_columns[RowSchema](...)`` is the released hgraph spelling:
        # its plain subscript selects the Frame row schema, not a bare Bundle
        # output.  Keep this as Python syntax adaptation; native dispatch still
        # receives the complete TS[Frame[RowSchema]] output constraint once.
        if (self.__name__ == "with_columns" and isinstance(item, type)):
            from .._compat import CompoundScalar
            if issubclass(item, CompoundScalar):
                from .._types import Frame, TS
                item = TS[Frame[item]]

        output_type = None
        sizes = []
        ts_hints = []
        resolutions = {}
        for i in (item if isinstance(item, tuple) else (item,)):
            if isinstance(i, slice):
                if i.start is OUT and output_type is None:
                    output_type = i.stop
                elif isinstance(i.stop, int):
                    sizes.append(i.stop)   # op[SIZE: Size[4]] pins size vars
                else:
                    # Keep the named binding for registry dispatch. The
                    # positional hint remains for eval_node input seeding.
                    resolutions[_type_var_name(i.start)] = i.stop
                    ts_hints.append(i.stop)
                    if (self.__name__ == "getattr_"
                            and _type_var_name(i.start) == "SCALAR"
                            and output_type is None):
                        output_type = TS[i.stop]
                continue
            if output_type is None:
                output_type = i
        if output_type is not None and not _hgraph.operator_output_is_selective(self.__name__):
            # The REGISTRY decides what a bare subscript type means: when no
            # candidate's output can be influenced by it (sinks, or every
            # overload shares one fixed output - to_json's TS[str]), the
            # type is an INPUT constraint; otherwise it names the output.
            ts_hints.append(output_type)
            output_type = None
        return _OperatorFunction(
            self.__name__, output_type=output_type, sizes=sizes or None,
            ts_hint=ts_hints or None, resolutions=resolutions or None,
            signature=self.__signature__, documentation=self.__doc__)

    def _normalise_type_arguments(self, args, kwargs):
        """Move documented positional type carriers into output selection.

        Type expressions are ordinary scalar values for some operators, so
        this compatibility adaptation is deliberately name- and position-
        specific rather than scanning every operator call. A family that
        declares the type argument natively (RFC 0033: ``const``,
        ``nothing``) keeps it in place -- the dispatcher binds the output
        from it and a positional ``delay`` after it lands on ``delay`` -- and
        the output constraint is set as well so the remaining Python-side
        rules see the same call; only a family that does not declare it yet
        (``replay``) has the type removed from the positional list.
        """
        if "tp" in kwargs or "output_type" in kwargs:
            return args, kwargs
        type_index = {"const": 1, "nothing": 0, "replay": 1}.get(
            self.__name__)
        if (type_index is None or type_index >= len(args)
                or not isinstance(args[type_index], _TsExpr)):
            return args, kwargs
        kwargs = dict(kwargs)
        kwargs["output_type"] = args[type_index]
        if type_index in _operator_carrier_positions(self.__name__):
            return args, kwargs
        return (*args[:type_index], *args[type_index + 1:]), kwargs

    def __repr__(self):
        return f"<operator {self.__name__}>"


def operator_function(name, signature=None):
    """Return a documented native operator callable with a public signature.

    ``signature`` may be an :class:`inspect.Signature` or a callable whose
    signature describes the user-facing contract. Otherwise a signature is
    derived when every native overload has the same structural call shape.
    The callable's docstring always lists the complete native overload set.
    Native dispatch and subscripted type selection remain unchanged.
    """
    if signature is None:
        signature = _PUBLIC_OPERATOR_SIGNATURES.get(name)
    return _OperatorFunction(name, signature=signature)


# --- hgraph's WiringPort operator sugar (release/0.5 _operators pattern) ---
_DUNDERS = {
    "__add__": "add_", "__sub__": "sub_", "__mul__": "mul_", "__truediv__": "div_",
    "__floordiv__": "floordiv_", "__mod__": "mod_", "__divmod__": "divmod_",
    "__pow__": "pow_", "__lshift__": "lshift_", "__rshift__": "rshift_",
    "__and__": "bit_and", "__or__": "bit_or", "__xor__": "bit_xor",
    "__eq__": "eq_", "__ne__": "ne_", "__lt__": "lt_", "__le__": "le_",
    "__gt__": "gt_", "__ge__": "ge_",
}
for dunder, op_name in _DUNDERS.items():
    setattr(WiringPort, dunder, (lambda op: lambda x, y: wire(op, x, y))(op_name))
for dunder, op_name in {
    "__radd__": "add_", "__rsub__": "sub_", "__rmul__": "mul_", "__rtruediv__": "div_",
    "__rfloordiv__": "floordiv_", "__rmod__": "mod_", "__rpow__": "pow_",
}.items():
    setattr(WiringPort, dunder, (lambda op: lambda x, y: wire(op, y, x))(op_name))
for dunder, op_name in {
    "__neg__": "neg_", "__pos__": "pos_", "__abs__": "abs_", "__invert__": "invert_",
}.items():
    setattr(WiringPort, dunder, (lambda op: lambda x: wire(op, x))(op_name))
def _port_getitem(self, item):
    # Fixed-TSL integer indexing is the STRUCTURAL element projection
    # (zero-copy, no node); a REF[TSL] port dereferences first (the input
    # binding inserts the from-REF adaptation); everything else dispatches
    # getitem_.
    if isinstance(item, int) and not isinstance(item, bool):
        raw = _unwrap(self)
        if raw.ts_type.is_fixed_tsl:
            return WiringPort(_hgraph.tsl_element(raw, item))
    return wire("getitem_", self, item)


WiringPort.__getitem__ = _port_getitem
WiringPort.__hash__ = object.__hash__  # __eq__ wires a node; identity hashing stands


def _port_bundle_field_names(self):
    """The TSB field names of a port (looking through REF), else None."""
    ts_type = self._port.ts_type
    if ts_type.is_tsb:
        return _hgraph.tsb_field_names(ts_type)
    try:
        deref = self._port.dereferenced
        if deref is not None and deref.ts_type.is_tsb:
            return _hgraph.tsb_field_names(deref.ts_type)
    except Exception:
        pass
    return None


def _port_len(self):
    ts_type = self._port.ts_type
    if ts_type.is_fixed_tsl:
        return ts_type.fixed_size
    if (names := _port_bundle_field_names(self)) is not None:
        return len(names)
    raise TypeError("len() is only defined for fixed-size TSL and TSB ports")


def _port_iter(self):
    # The sequence protocol for fixed TSLs and TSBs (`*tsl` / `a, b = tsb`
    # unpacking, hgraph parity; REF[TSB] iterates the referenced fields).
    # Without __len__/__iter__, python's fallback iteration via __getitem__
    # would wire getitem_ nodes FOREVER.
    if (names := _port_bundle_field_names(self)) is not None:
        return (getattr(self, name) for name in names)
    return (self[i] for i in range(len(self)))


def _port_copy_with(self, **overrides):
    """Rebuild a TSB from its projected fields with named replacements."""
    names = _port_bundle_field_names(self)
    if names is None:
        raise TypeError("copy_with is only defined for TSB ports")
    unknown = tuple(name for name in overrides if name not in names)
    if unknown:
        raise TypeError(f"unknown TSB field(s): {', '.join(unknown)}")

    target = self._port.ts_type
    if not target.is_tsb:
        target = self._port.dereferenced.ts_type
    from .._types import _TsExpr

    fields = {name: getattr(self, name) for name in names}
    fields.update(overrides)
    return _TsExpr(target, repr(target)).from_ts(**fields)


def _port_as_dict(self):
    """Project a TSB (including REF[TSB]) to its named field ports."""
    names = _port_bundle_field_names(self)
    if names is None:
        raise TypeError("as_dict is only defined for TSB ports")
    raw = _unwrap(self)
    source = self if raw.ts_type.is_tsb else WiringPort(raw.dereferenced)
    return {name: getattr(source, name) for name in names}


def _port_as_scalar_ts(self):
    """Convert a lifted CompoundScalar TSB (or REF[TSB]) to its scalar TS."""
    raw = _unwrap(self)
    source = raw if raw.ts_type.is_tsb else raw.dereferenced
    if source is None or not source.ts_type.is_tsb:
        raise TypeError("as_scalar_ts is only defined for TSB ports")

    from .._types import TS

    value_type = _hgraph.ts_value_vt(source.ts_type)
    scalar_type = _hgraph.python_type_for_value(value_type)
    return wire("convert", WiringPort(source), output_type=TS[scalar_type])


WiringPort.__len__ = _port_len
WiringPort.__iter__ = _port_iter
WiringPort.copy_with = _port_copy_with
WiringPort.as_dict = _port_as_dict
WiringPort.as_scalar_ts = _port_as_scalar_ts


class _CallableWiringPort(WiringPort):
    """A wiring port that tolerates a no-arg call: upstream's method-style
    accessors (``td.total_seconds()``, ``dt.weekday()``) spell an attribute
    THEN a call, while hgraph attribute sugar also allows the bare form —
    this port serves both (issue #82)."""

    __slots__ = ()

    def __call__(self):
        return self


# Upstream getattr_ *method* tables (date/datetime/time/timedelta): names the
# DSL may spell with trailing parentheses.
_METHOD_STYLE_ACCESSORS = frozenset({
    "weekday", "isoweekday", "isoformat", "total_seconds", "timestamp",
})


def _port_getattr(self, name):
    if name.startswith("_"):
        raise AttributeError(name)
    if name == "as_schema":
        return self   # hgraph's TSB.as_schema: typed field access (same port)
    if name == "key_set":
        return wire("keys_", self)   # hgraph's TSD.key_set property
    # JSON leaf coercions: j["a"].int / .float / .str / .bool.
    if name in ("int", "float", "str", "bool") and _unwrap(self).ts_type.is_ts_json:
        return wire("json_as_" + name, self)
    raw = _unwrap(self)
    if raw.ts_type.is_tsb:
        fields = _hgraph.tsb_field_names(raw.ts_type)
        try:
            index = fields.index(name)
        except ValueError:
            pass
        else:
            return WiringPort(_hgraph.tsb_element(raw, index))
    try:
        return wire("getattr_", self, name)
    except WiringError:
        # hgraph attribute sugar: port.year / .month / .weekday ... resolve
        # as unary operators when no bundle field matches.
        if name in _hgraph.operator_names():
            try:
                port = wire(name, self)
            except WiringError as error:
                # No overload for this port's type either: surface the
                # Python attribute protocol (upstream raises AttributeError).
                raise AttributeError(
                    f"{_unwrap(self).ts_type} has no attribute '{name}'") from error
            if name in _METHOD_STYLE_ACCESSORS:
                return _CallableWiringPort(_unwrap(port))
            return port
        raise


def _port_reduce(self, fn, zero=_REDUCE_ZERO, is_associative=True):
    from ._compose import reduce

    return reduce(fn, self, zero, is_associative=is_associative)


def _port_keys(self):
    """hgraph's TSB mapping protocol: field names (dict(**tsb) works)."""
    tp = _unwrap(self).ts_type
    return tuple(_hgraph.tsb_field_names(tp))


WiringPort.reduce = _port_reduce
WiringPort.keys = _port_keys
WiringPort.__getattr__ = _port_getattr

class WiringError(RuntimeError):
    """A wiring-time error (hgraph parity)."""

class ParseError(WiringError):
    """A wiring function's declaration is malformed (hgraph parity)."""


class IncorrectTypeBinding(WiringError):
    """A port's type does not match the parameter it is wired to."""


class RequirementsNotMetWiringError(WiringError):
    """An overload's requires= predicate rejected the call."""

_published_contexts = []   # [(port, ts_type_handle, frame, owning_wiring)] newest last


def _port_enter(self):
    import sys

    frame = sys._getframe(1)
    _published_contexts.append(
        (self, self._port.ts_type, frame, _current_wiring()))
    return self


def _port_exit(self, *exc):
    _published_contexts.pop()
    return False


WiringPort.__enter__ = _port_enter
WiringPort.__exit__ = _port_exit


def _context_name_of(port, frame):
    """The `as` variable name: the frame local bound to this port."""
    # The source parameter and the later ``as`` alias commonly point at the
    # same WiringPort. Locals retain insertion order, so the alias is the last
    # matching binding and is the public context name.
    for var_name, value in reversed(tuple(frame.f_locals.items())):
        if value is port:
            return var_name
    return None


def _resolve_context(ctx_expr, name=None, resolution_scope=None):
    """The most recent published context matching type (and name).

    Generic annotations bind through the caller's native ResolutionScope so
    repeated uses of one type variable remain consistent.
    """
    import typing

    from .._types import (
        _GenericTsExpr,
        _TSB_SCHEMA_CLASSES,
        _TypeVarSentinel,
        _pattern_of,
    )

    wanted = getattr(ctx_expr.ts, "handle", None)
    pattern = _pattern_of(ctx_expr.ts) if isinstance(
        ctx_expr.ts, (_GenericTsExpr, _TypeVarSentinel)) else None
    for port, ts_type, frame, owning_wiring in reversed(_published_contexts):
        if name is not None and _context_name_of(port, frame) != name:
            continue
        matches = wanted is not None and ts_type == wanted
        if not matches and pattern is not None:
            scope = resolution_scope or _hgraph.ResolutionScope()
            matches = scope.match(pattern, ts_type)
        if not matches:
            requested_class = getattr(ctx_expr.ts, "_py_class", None)
            published_class = None
            candidate = _hgraph.ref_target(ts_type) if ts_type.is_ref else ts_type
            if candidate.is_ts:
                published_class = _hgraph.python_type_for_value(
                    _hgraph.ts_value_vt(candidate))
            elif candidate.is_tsb:
                published_class = _hgraph.python_type_for_value(
                    _hgraph.tsb_value_vt(candidate))
                if published_class is None:
                    published_class = _TSB_SCHEMA_CLASSES.get(candidate)
            requested_class = typing.get_origin(requested_class) or requested_class
            published_class = typing.get_origin(published_class) or published_class
            matches = (
                isinstance(requested_class, type)
                and isinstance(published_class, type)
                and issubclass(published_class, requested_class)
            )
        if not matches:
            continue
        current_wiring = _current_wiring()
        # Nested graph/service callbacks receive borrowed Python wrappers for
        # their native Wiring.  Wrapper identity therefore cannot establish a
        # graph boundary: compare the underlying Wiring objects instead.
        if _hgraph.same_wiring(current_wiring, owning_wiring):
            return port
        # A context can cross multiple nested compilation boundaries. Make
        # each boundary explicit through the same C++ capture protocol used
        # by native Context inputs; carrying the parent's boundary placeholder
        # directly would leave the inner nested node with no material input.
        return WiringPort(
            _hgraph.capture_outer_source(current_wiring, _unwrap(port)))
    return None
