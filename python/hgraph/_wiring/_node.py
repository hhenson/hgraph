"""_PyNode: Python user-node wiring (signature binding + call
normalisation), @compute_node/@sink_node, lift, @generator, push_queue."""
import inspect
import typing
import warnings
from copy import copy

import _hgraph

from .._types import (default_type_var_of as _default_type_var_of,
                      wiring_signature_of as _wiring_signature_of,
                      _ContextExpr, _GenericTsExpr, _Required, _TsExpr,
                      _TypeVarSentinel, _evaluated_annotations,
                      _type_var_is_scalar, _type_var_name)
from ._core import (IncorrectTypeBinding, RequirementsNotMetWiringError,
                    WiringError, WiringPort, _current_wiring,
                    _resolve_context, _unwrap, wire)
from ._markers import (STATE, _INJECTABLE_MARKERS, _MISSING,
                       _RecordableStateExpr, _StateExpr, _annotation_ts_kind,
                       _is_object_vt, _tsw_kind, _unbounded_tuple_kind)
from ._operator import _register_overload, _run_requires
from ._resolution import (_apply_resolvers as _apply_wiring_resolvers,
                          _bind_resolution, _invoke_resolution_callable)


def _warn_deprecated(name, deprecated):
    if not deprecated:
        return
    message = deprecated if isinstance(deprecated, str) else f"'{name}' is deprecated"
    warnings.warn(message, DeprecationWarning, stacklevel=3)


def _annotation_type_vars(annotation):
    """The type-variable names in one parameter or return annotation, in order.

    Read off the lowered C++ pattern, so the answer comes from the same
    structure the matcher unifies against. Annotations that lower to no pattern
    at all - a plain ``int`` scalar, an injectable marker - contribute nothing.
    """
    from .._types import _pattern_of, _scalar_pattern, _type_var_name

    if typing.get_origin(annotation) is type:
        # `to: Type[SCALAR_1]` - a scalar parameter naming a type variable.
        args = typing.get_args(annotation)
        if args and isinstance(args[0], (_TypeVarSentinel, typing.TypeVar)):
            return [_type_var_name(args[0])]
        return []
    for lower in (_pattern_of, _scalar_pattern):
        try:
            pattern = lower(annotation)
        except Exception:
            continue
        return list(getattr(pattern, "variables", ()) or ())
    return []


def _is_time_series_annotation(annotation):
    return (
        isinstance(annotation, (_TsExpr, _ContextExpr, _GenericTsExpr))
        or (
            isinstance(annotation, (_TypeVarSentinel, typing.TypeVar))
            and not _type_var_is_scalar(annotation)
        )
    )


def _lift_time_series_argument(value, annotation):
    """Lift a plain Python value to the time-series shape declared by a
    graph or node parameter, retaining nominal CompoundScalar schemas when
    the annotation itself is generic."""
    if isinstance(annotation, _TsExpr):
        return wire("const", value, output_type=annotation)

    from .._compat import CompoundScalar
    if isinstance(value, CompoundScalar):
        from .._types import TS
        return wire("const", value, output_type=TS[type(value)])
    return wire("const", value)

class _PyNode:
    """@compute_node / @sink_node: a Python function as a runtime node. The
    function runs on the graph thread (both modes) under the GIL, receives
    time-series inputs as plain Python VALUES and wiring-time scalars as
    supplied; STATE/CLOCK/SCHEDULER/EvaluationEngineApi-annotated parameters
    are injected. A
    compute node's return value ticks its output (None = no tick). It must
    have no side effects beyond its output."""

    def __init__(self, fn, has_output, active=None, valid=None, all_valid=None,
                 resolvers=None, node_type=None, label=None, deprecated=False):
        self._wiring_signature, self._default_type_var = _default_type_var_of(
            inspect.signature(fn, eval_str=True))
        var_params = [p for p in self._wiring_signature.parameters.values()
                      if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)]
        if var_params:
            # hgraph parity (upstream WiringNodeClass): star params
            # receive ONE packed time-series (a TSL/TSB view), so the code
            # object is rewritten to make every parameter keyword-only.
            import types

            co = fn.__code__
            kw_only_code = co.replace(
                co_flags=co.co_flags & ~(inspect.CO_VARARGS | inspect.CO_VARKEYWORDS),
                co_argcount=0,
                co_posonlyargcount=0,
                co_kwonlyargcount=len(self._wiring_signature.parameters),
            )
            rewritten = types.FunctionType(kw_only_code, fn.__globals__, name=fn.__name__,
                                           argdefs=fn.__defaults__, closure=fn.__closure__)
            # an EMPTY group must still bind: () / {} defaults.
            kw_defaults = dict(fn.__kwdefaults__ or {})
            for p in var_params:
                kw_defaults[p.name] = () if p.kind is inspect.Parameter.VAR_POSITIONAL else {}
            rewritten.__kwdefaults__ = kw_defaults
            fn = rewritten
        self.fn = fn
        self.has_output = has_output
        # active=/valid= accept name iterables OR wiring-time callables
        # (m, **scalars) evaluated once the call's scalars are known.
        self._active_fn = active if callable(active) else None
        self._valid_fn = valid if callable(valid) else None
        self._all_valid_fn = all_valid if callable(all_valid) else None
        self._active = None if callable(active) else self._policy_names("active", active)
        self._valid = None if callable(valid) else self._policy_names("valid", valid)
        self._all_valid = (
            None if callable(all_valid) else self._policy_names("all_valid", all_valid))
        self._resolvers = dict(resolvers) if resolvers else None
        self._requires = None
        self._pins = {}
        self._node_type = node_type
        self._label = label
        self._deprecated = deprecated
        self._wired_fn_cache = {}
        self._start_fn = None
        self._stop_fn = None
        self.__name__ = fn.__name__
        sig = self._wiring_signature
        self._signature = sig
        # Callers introspecting the NODE (eval_node's input typing) must see
        # the user function's signature, not _PyNode.__call__'s.
        self.__signature__ = sig
        self._out_tp = sig.return_annotation if has_output else None
        self._params = list(sig.parameters.values())
        self._recordable_state = next(
            (param.annotation for param in self._params
             if isinstance(param.annotation, _RecordableStateExpr)), None)
        ts_names = {
            param.name
            for param in self._params
            # Generic annotations (TIME_SERIES_TYPE, TSS[SCALAR], ...) are
            # time-series parameters too - they resolve from the wired ports.
            if _is_time_series_annotation(param.annotation)
        }
        self._ts_names = ts_names
        for policy, names in (("active", self._active), ("valid", self._valid),
                              ("all_valid", self._all_valid)):
            if names is None:
                continue
            unknown = names - ts_names
            if unknown:
                rendered = ", ".join(sorted(unknown))
                raise TypeError(f"{self.__name__}: {policy}= contains non-time-series input(s): {rendered}")
        if self._valid is not None and self._all_valid is not None:
            overlap = self._valid & self._all_valid
            if overlap:
                rendered = ", ".join(sorted(overlap))
                raise TypeError(
                    f"{self.__name__}: valid= and all_valid= overlap: {rendered}")
        # Injectable parameters MUST default to None (hgraph convention):
        # the default guarantees user code in a graph never supplies them.
        for param in self._params:
            is_injectable = (param.annotation in _INJECTABLE_MARKERS or
                             isinstance(param.annotation, _StateExpr) or
                             isinstance(param.annotation, _RecordableStateExpr))
            if is_injectable and param.default is not None:
                raise TypeError(
                    f"injectable parameter '{param.name}' of '{self.__name__}' must default to None"
                )
        if sum(isinstance(param.annotation, _RecordableStateExpr)
               for param in self._params) > 1:
            raise TypeError(f"'{self.__name__}' supports at most one RECORDABLE_STATE parameter")
        if self._recordable_state is not None and any(
                (param.annotation is STATE or
                 isinstance(param.annotation, _StateExpr))
                for param in self._params):
            raise TypeError(
                f"'{self.__name__}' cannot combine STATE with RECORDABLE_STATE")

    @staticmethod
    def _policy_names(policy, names):
        if names is None:
            return None
        if isinstance(names, str):
            raise TypeError(f"{policy}= must be an iterable of input names, not a string")
        try:
            result = frozenset(names)
        except TypeError as error:
            raise TypeError(f"{policy}= must be an iterable of input names") from error
        if any(not isinstance(name, str) for name in result):
            raise TypeError(f"{policy}= must contain only input names")
        return result

    def _lifecycle_annotation(self, param):
        """The governing annotation for a lifecycle parameter (issue #79).

        Start/stop parameters match the EVAL signature by name and nothing
        else — eval is the signature bearer, so a name it declares takes
        eval's definition and the lifecycle function's own annotation and
        default are documentation. Names eval does not declare (e.g. an
        extra injectable such as a clock) keep their own annotation.
        """
        for eval_param in self._params:
            if eval_param.name == param.name:
                return eval_param.annotation
        return param.annotation

    def _set_lifecycle(self, phase, fn):
        eval_names = {p.name for p in self._params}
        for param in _wiring_signature_of(fn)[0].parameters.values():
            if param.name == "_output":
                raise TypeError(
                    f"@{self.__name__}.{phase} supports wiring-time scalars and injectables only"
                )
            annotation = self._lifecycle_annotation(param)
            if (param.annotation in _INJECTABLE_MARKERS or
                    isinstance(param.annotation, (_StateExpr, _RecordableStateExpr)) or
                    param.name in eval_names):
                if (isinstance(annotation, _RecordableStateExpr)
                        and self._recordable_state is None):
                    raise TypeError(
                        f"@{self.__name__}.{phase} cannot inject RECORDABLE_STATE "
                        "when the node does not declare one"
                    )
                if ((isinstance(annotation, (_TsExpr, _ContextExpr))
                     or _is_time_series_annotation(annotation)) and phase != "stop"):
                    raise TypeError(
                        f"@{self.__name__}.{phase} supports wiring-time scalars and injectables only"
                    )
                continue
            if (isinstance(param.annotation, (_TsExpr, _ContextExpr))
                    and phase != "stop"):
                raise TypeError(
                    f"@{self.__name__}.{phase} supports wiring-time scalars and injectables only"
                )
            if _is_time_series_annotation(param.annotation):
                raise TypeError(
                    f"@{self.__name__}.stop has no node input named '{param.name}'")
        setattr(self, f"_{phase}_fn", fn)
        return fn

    def start(self, fn):
        return self._set_lifecycle("start", fn)

    def stop(self, fn):
        return self._set_lifecycle("stop", fn)

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        node_type = self._node_type or (
            WiringNodeType.COMPUTE_NODE if self.has_output else WiringNodeType.SINK_NODE)
        return extract_signature(self.fn, node_type)

    def __getitem__(self, item):
        # node[TYPEVAR: TYPE] pre-resolution: the pins seed the call's
        # ResolutionScope (the C++ type-variable map) - a copied node so the
        # shared decorator object is never mutated.
        from .._types import _TypeVarSentinel, _TsExpr

        import copy

        pinned = copy.copy(self)
        pinned._pins = dict(self._pins)
        pinned._wired_fn_cache = {}
        items = item if isinstance(item, tuple) else (item,)
        unnamed = []
        named = set()
        for entry in items:
            if isinstance(entry, slice) and isinstance(entry.start, _TypeVarSentinel):
                name = _type_var_name(entry.start)
                pinned._pins[name] = entry.stop
                named.add(name)
            else:
                unnamed.append(entry)
        # Unnamed items fill the type variables the caller did NOT name, so the
        # two forms compose: my_node[str, V: int] names V and positions str.
        for name, entry in zip(self._unnamed_targets(unnamed, named), unnamed):
            pinned._pins[name] = entry
        return pinned

    def _ordered_type_vars(self):
        """This signature's type-variable names, in order of first appearance.

        Derived from the lowered patterns rather than from a second python-side
        notion of what a type variable is, so it cannot drift from what the
        matcher actually unifies against. Cached: signatures are immutable once
        the node is decorated.
        """
        cached = getattr(self, "_type_var_cache", None)
        if cached is not None:
            return cached
        ordered = []
        annotations = [param.annotation for param in self._params]
        if self.has_output:
            annotations.append(self._out_tp)
        for annotation in annotations:
            for name in _annotation_type_vars(annotation):
                if name not in ordered:
                    ordered.append(name)
        ordered = tuple(ordered)
        self._type_var_cache = ordered
        return ordered

    def _unnamed_targets(self, entries, named):
        """Type-variable names for the unnamed pre-resolution ``entries``.

        A single item binds the signature's sole remaining type variable, or
        the one marked ``DEFAULT[...]``. Several items bind remaining type
        variables in order of first appearance across the parameters and then
        the return annotation. ``named`` are the variables the caller already
        bound explicitly, and are never filled by position.
        """
        if not entries:
            return []
        remaining = [name for name in self._ordered_type_vars()
                     if name not in named]
        if len(entries) == 1:
            if len(remaining) == 1:
                return remaining
            if self._default_type_var is not None and self._default_type_var not in named:
                return [self._default_type_var]
        elif len(entries) <= len(remaining):
            return remaining[:len(entries)]
        rendered = ", ".join(remaining) if remaining else "none"
        raise WiringError(
            f"{self.__name__}: can not figure out which type parameter to assign "
            f"{entries[0]!r} to (unbound type variables in this signature: {rendered}). "
            f"Name it explicitly, as in {self.__name__}[TYPE_VAR: {entries[0]!r}], "
            f"or mark one with DEFAULT[...].")

    def _with_resolution(self, bindings):
        """Return a call-local node seeded from operator dispatch.

        The C++ overload registry has already unified the candidate's input,
        output, scalar, and size patterns. Reusing those bindings prevents a
        Python implementation from starting a second, incomplete type
        resolution pass when its generic is fixed only by the requested
        output.
        """
        import copy

        resolved = copy.copy(self)
        resolved._pins = dict(self._pins)
        resolved._pins.update(bindings)
        resolved._wired_fn_cache = {}
        return resolved

    @staticmethod
    def _bind_resolved(scope, name, resolved):
        """Seed a pinned/resolved type variable into the C++ scope: a ts
        expression binds the ts var, a fixed-size value binds the size var,
        and a python scalar type binds the scalar var of the same name."""
        _bind_resolution(scope, name, resolved)

    @staticmethod
    def _eval_policy(policy, fn, scope, scalar_values):
        """Evaluate a wiring-time active=/valid= callable: (m, **scalars) ->
        None (all inputs) / iterable of input names."""
        result = _invoke_resolution_callable(fn, scope.bindings, scalar_values)
        if result is None:
            return None
        return frozenset(result)

    @staticmethod
    def _resolved_auto_value(scope, param):
        """Project a C++ resolution-scope binding into a Python node scalar."""
        import typing

        from .._types import _TsExpr, _type_var_name

        arguments = typing.get_args(param.annotation)
        if typing.get_origin(param.annotation) is not type or not arguments:
            raise WiringError(
                f"AUTO_RESOLVE parameter '{param.name}' needs a type[TYPEVAR] annotation")
        name = _type_var_name(arguments[0])
        if (scalar := scope.find_scalar(name)) is not None:
            return _hgraph.python_type_for_value(scalar)
        if (ts := scope.find_ts(name)) is not None:
            return _TsExpr(ts, f"resolved[{name}]")
        if (size := scope.find_size(name)) is not None:
            from ._graph import _ResolvedSize
            return _ResolvedSize(size)
        raise WiringError(
            f"AUTO_RESOLVE could not resolve '{param.name}' ({arguments[0]!r}) from the wired arguments")

    def _diagnostic_label(self, scalar_values=None):
        """User-facing node identity for diagnostics (issue #247): trace,
        wiring-trace, and profiler render implementation:label. Explicit
        labels resolve ``{scalar}`` placeholders with the call's wiring-time
        scalar values (upstream parity: label="custom_label {i}")."""
        if self._label:
            if scalar_values and "{" in self._label:
                class _Keep(dict):
                    def __missing__(self, key):
                        return "{" + key + "}"

                try:
                    return self._label.format_map(_Keep(scalar_values))
                except (ValueError, IndexError):
                    return self._label
            return self._label
        return getattr(self.fn, "__name__", None) or self.__name__

    def _check_binding(self, scope, param, value):
        """Match the wired port against the parameter's TYPE PATTERN,
        binding type variables into the scope. A mismatch on a concrete
        CompoundScalar annotation accepts SUBCLASS ports (python's
        inheritance is a py-frontend concept); anything else raises."""
        from .._types import TS, _pattern_of

        annotation = param.annotation
        if isinstance(annotation, _ContextExpr):
            return
        import typing

        if typing.get_origin(annotation) is typing.Union:
            members = typing.get_args(annotation)
            port_tp = _unwrap(value).ts_type
            for member in members:
                if isinstance(member, _TsExpr) and member.handle == port_tp:
                    return
            raise IncorrectTypeBinding(
                f"{self.__name__}: '{param.name}' expects one of {members!r}")
        if isinstance(annotation, _TsExpr):
            handle = annotation.handle
            # TS[object] widens over any payload (the py-any input rule).
            if handle.is_ts and _is_object_vt(_hgraph.ts_value_vt(handle)):
                return
            # TSW strictness is deferred until the duration/tick marker
            # normalisation lands (min-period defaults are applied at
            # wiring, so handle equality over-rejects).
            if handle.kind == _tsw_kind():
                return
        try:
            pattern = _pattern_of(annotation)
        except TypeError:
            return   # non-ts annotation reached with a port: leave to the runtime
        port_tp = _unwrap(value).ts_type
        if scope.match(pattern, port_tp):
            return
        if isinstance(annotation, _TsExpr) and annotation.handle.is_ts:
            # tuple[E, ...] widens over fixed tuples of E: re-match with the
            # C++ homogeneous-tuple pattern (the matcher owns that rule).
            vt = _hgraph.ts_value_vt(annotation.handle)
            if _hgraph.vt_kind(vt) == _unbounded_tuple_kind():
                homogeneous = _hgraph.type_pattern_ts(
                    _hgraph.scalar_pattern_homogeneous_tuple(
                        _hgraph.scalar_pattern_value(_hgraph.vt_element(vt))))
                if scope.match(homogeneous, port_tp):
                    return
        cs_class = getattr(annotation, "_cs_class", None)
        if cs_class is not None:
            stack = list(cs_class.__subclasses__())
            while stack:
                candidate = stack.pop()
                stack.extend(candidate.__subclasses__())
                if TS[candidate].handle == port_tp:
                    return   # subtype binding (auto-cast)
        raise IncorrectTypeBinding(
            f"{self.__name__}: '{param.name}' expects {annotation!r}, got {port_tp!r}")

    @staticmethod
    def _requested_input_shape(scope, annotation, value):
        """Resolve the exact input shape used when packing a Python node.

        Union inputs are validated member-by-member, so they do not have one
        TypePattern to resolve. Preserve the member that matched the port;
        ordinary inputs continue through the shared C++ resolution scope.
        """
        import types
        import typing
        from .._types import _pattern_of

        if typing.get_origin(annotation) in (typing.Union, types.UnionType):
            port_type = _unwrap(value).ts_type
            for member in typing.get_args(annotation):
                if isinstance(member, _TsExpr) and member.handle == port_type:
                    return member.handle
            return port_type
        return scope.resolve_ts(_pattern_of(annotation))

    def _apply_resolvers(self, scope, scalar_values):
        """resolvers={TYPEVAR: lambda mapping, <scalars...>: type}: bind the
        computed types into the scope (mapping = the scope's bindings)."""
        _apply_wiring_resolvers(scope, self._resolvers, scalar_values)

    @staticmethod
    def _resolve_recordable_state(annotation, scope):
        from .._types import _TsExpr, _pattern_of, _value_type
        import typing

        schema = annotation.schema
        origin = typing.get_origin(schema) or schema
        args = typing.get_args(schema)
        for parameter, resolved in zip(getattr(origin, "__parameters__", ()), args):
            if isinstance(resolved, _TypeVarSentinel):
                continue
            name = _type_var_name(parameter)
            if isinstance(resolved, _TsExpr):
                scope.bind_ts(name, resolved.handle)
            else:
                scope.bind_scalar(name, _value_type(resolved))

        annotations = {}
        for klass in reversed(origin.__mro__):
            annotations.update(_evaluated_annotations(klass))
        fields = []
        for name, field_type in annotations.items():
            if isinstance(field_type, _TsExpr):
                resolved = field_type.handle
            else:
                resolved = scope.resolve_ts(_pattern_of(field_type))
            if resolved is None:
                raise TypeError(
                    f"cannot resolve recordable-state field '{name}' of {origin.__name__}")
            fields.append((name, resolved))

        state_name = origin.__name__
        qualname = getattr(origin, "__qualname__", state_name)
        if "<locals>" in qualname:
            state_name = f"{origin.__module__}.{qualname}"
        return _TsExpr(_hgraph.tsb(state_name, fields), f"TSB[{origin.__name__}]")

    def __call__(self, *args, **kwargs):
        from .._types import _pattern_of

        _warn_deprecated(self.__name__, self._deprecated)
        kwargs.pop("__recordable_id__", None)
        lifecycle_scalar_values = {}
        for phase in ("start", "stop"):
            lifecycle_fn = getattr(self, f"_{phase}_fn")
            if lifecycle_fn is None:
                continue
            for param in inspect.signature(lifecycle_fn, eval_str=True).parameters.values():
                injectable = (
                    param.annotation in _INJECTABLE_MARKERS
                    or isinstance(param.annotation, (_StateExpr, _RecordableStateExpr))
                )
                if (not injectable and param.name not in self._signature.parameters
                        and param.name in kwargs):
                    lifecycle_scalar_values[param.name] = kwargs.pop(param.name)
        ref = _hgraph.node_ref(self.fn)
        layout, ports, scalars, reference_shapes = [], [], [], []
        # The wiring-time RESOLUTION SCOPE: the C++ type-variable map. Every
        # generic input pattern matches into it; outputs/resolvers/pins read
        # and write the same map - no python-side type classification.
        scope = _hgraph.ResolutionScope()
        for name, resolved in self._pins.items():
            self._bind_resolved(scope, name, resolved)
        bound = self._signature.bind_partial(*args, **kwargs)
        scalar_values = dict(lifecycle_scalar_values)
        # Pre-collect scalar values so callable active=/valid= policies can
        # evaluate before layout letters are chosen.
        for param in self._params:
            if (_is_time_series_annotation(param.annotation)
                    or isinstance(param.annotation, (_RecordableStateExpr,
                                                     _StateExpr))):
                continue
            if param.annotation in _INJECTABLE_MARKERS:
                continue
            value = bound.arguments.get(param.name, _MISSING)
            if value is _MISSING and param.default is not inspect.Parameter.empty:
                value = param.default
            if value is not _MISSING and not isinstance(value, WiringPort):
                scalar_values[param.name] = value
        # Resolve concrete input patterns before evaluating resolver-backed
        # active/valid policies. Upstream policy callables receive the final
        # resolution map, not an empty pre-binding scope.
        for param in self._params:
            value = bound.arguments.get(param.name, _MISSING)
            if isinstance(value, WiringPort) and _is_time_series_annotation(
                    param.annotation):
                self._check_binding(scope, param, value)
        if self._resolvers:
            self._apply_resolvers(scope, scalar_values)
        from .._types import AUTO_RESOLVE
        for param in self._params:
            if scalar_values.get(param.name, _MISSING) is AUTO_RESOLVE:
                scalar_values[param.name] = self._resolved_auto_value(scope, param)
        active_policy = self._active
        valid_policy = self._valid
        all_valid_policy = self._all_valid
        if self._active_fn is not None:
            active_policy = self._eval_policy("active", self._active_fn, scope, scalar_values)
        if self._valid_fn is not None:
            valid_policy = self._eval_policy("valid", self._valid_fn, scope, scalar_values)
        if self._all_valid_fn is not None:
            all_valid_policy = self._eval_policy(
                "all_valid", self._all_valid_fn, scope, scalar_values)
        for policy, names in (("active", active_policy), ("valid", valid_policy),
                              ("all_valid", all_valid_policy)):
            if names is None:
                continue
            unknown = names - self._ts_names
            if unknown:
                rendered = ", ".join(sorted(unknown))
                raise TypeError(
                    f"{self.__name__}: {policy}= contains non-time-series input(s): {rendered}")
        if valid_policy is not None and all_valid_policy is not None:
            overlap = valid_policy & all_valid_policy
            if overlap:
                rendered = ", ".join(sorted(overlap))
                raise TypeError(
                    f"{self.__name__}: valid= and all_valid= overlap: {rendered}")
        # Var-args parity (upstream model): the *args group packs into ONE
        # TSL (or structural TSB when so annotated), **kwargs into ONE named
        # TSB (or TSD); the rewritten fn receives every parameter BY NAME.
        has_var_group = any(p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
                            for p in self._params)
        by_name = has_var_group
        layout_names, layout_by_name = [], []   # parallel to ``layout``

        def _note(name):
            layout_names.append(name)
            layout_by_name.append(by_name)

        def _lift(entry):
            """A plain value in a variadic group lifts to an inferred const."""
            if isinstance(entry, WiringPort):
                return entry
            return wire("const", entry)

        def _group_layout(param):
            if all_valid_policy is not None and param.name in all_valid_policy:
                return "a" if active_policy is None or param.name in active_policy else "A"
            required = valid_policy is None or param.name in valid_policy
            is_active = active_policy is None or param.name in active_policy
            return {(True, True): "t", (True, False): "u",
                    (False, True): "T", (False, False): "U"}[(is_active, required)]

        for param in self._params:
            if param.kind is inspect.Parameter.VAR_POSITIONAL:
                entries = [_unwrap(_lift(entry)) for entry in bound.arguments.get(param.name, ())]
                if entries:
                    if _annotation_ts_kind(param.annotation) == _hgraph.TS_KIND_TSB:
                        packed_group = _hgraph.bundle_port(entries, [False] * len(entries))
                    else:
                        packed_group = _hgraph.tsl_port(entries)
                    # Match the PACK against the declared annotation (issue
                    # #224): binds pack-level schema/type vars so a generic
                    # return (e.g. TSL[E, SIZE]) can resolve.
                    self._check_binding(scope, param, packed_group)
                    layout.append(_group_layout(param))
                    _note(param.name)
                    ports.append(packed_group)
                    reference_shapes.append(False)
                continue
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                extras = {k: _unwrap(_lift(v)) for k, v in bound.arguments.get(param.name, {}).items()}
                if extras:
                    if _annotation_ts_kind(param.annotation) == _hgraph.TS_KIND_TSD:
                        packed_group = _unwrap(wire(
                            "combine_tsd", tuple(extras.keys()),
                            *(WiringPort(v) for v in extras.values()), __strict__=False))
                    else:
                        tsb_type = _hgraph.un_named_tsb_type(
                            [(k, v.ts_type) for k, v in extras.items()])
                        packed_group = _hgraph.tsb_port(tsb_type, extras)
                    # Match the PACK against the declared annotation (issue
                    # #224): binds TSB[TS_SCHEMA] so the generic return
                    # resolves from the supplied keywords.
                    self._check_binding(scope, param, packed_group)
                    layout.append(_group_layout(param))
                    _note(param.name)
                    ports.append(packed_group)
                    reference_shapes.append(False)
                continue
            if param.kind is inspect.Parameter.KEYWORD_ONLY:
                by_name = True
            if isinstance(param.annotation, _RecordableStateExpr):
                if param.name in bound.arguments:
                    raise TypeError(
                        f"{self.__name__}: injectable '{param.name}' cannot be supplied")
                layout.append("R")
                _note(param.name)
                continue
            if isinstance(param.annotation, _StateExpr):
                if param.name in bound.arguments:
                    raise TypeError(
                        f"{self.__name__}: injectable '{param.name}' cannot be supplied")
                layout.append("Q")
                _note(param.name)
                scalars.append(param.annotation.factory)
                continue
            marker = _INJECTABLE_MARKERS.get(param.annotation)
            if marker is not None:
                if param.name in bound.arguments:
                    raise TypeError(f"{self.__name__}: injectable '{param.name}' cannot be supplied")
                layout.append(marker)
                _note(param.name)
                continue
            if isinstance(param.annotation, _ContextExpr):
                # A caller-supplied port overrides ambient context, matching
                # the native Context input contract. Requirement/name marker
                # values still select from the published context stack.
                supplied = bound.arguments.get(param.name, _MISSING)
                if isinstance(supplied, WiringPort):
                    context_param = param.replace(annotation=param.annotation.ts)
                    self._check_binding(scope, context_param, supplied)
                    resolved = supplied
                else:
                    requirement = (
                        supplied if supplied is not _MISSING else param.default)
                    name = None
                    required = False
                    if isinstance(requirement, _Required):
                        required, name = True, requirement.name
                    elif isinstance(requirement, str):
                        name = requirement
                    resolved = _resolve_context(param.annotation, name, scope)
                    if resolved is None:
                        where = f" with name {name}" if name else ""
                        if required:
                            raise WiringError(
                                f"no context published for '{param.name}'{where} of '{self.__name__}'")
                        continue   # optional and absent: the fn sees its None default
                is_active = active_policy is None or param.name in active_policy
                layout.append("C" if is_active else "P")
                _note(param.name)
                ports.append(_unwrap(resolved))
                reference_shapes.append(False)
                continue
            if param.name == "_output":
                # hgraph's _output injection: the node's own output view.
                if param.name in bound.arguments:
                    raise TypeError(f"{self.__name__}: _output cannot be supplied")
                layout.append("o")
                _note(param.name)
                continue
            value = bound.arguments.get(param.name, _MISSING)
            if value is _MISSING:
                if param.default is inspect.Parameter.empty:
                    raise TypeError(f"{self.__name__}: missing argument '{param.name}'")
                value = param.default
                if value is None and isinstance(param.annotation, (_TsExpr,)):
                    # unwired optional ts input: a never-ticking source
                    value = wire("nothing", output_type=param.annotation)
            if value is AUTO_RESOLVE:
                value = scalar_values[param.name]
            if not isinstance(value, WiringPort) and _is_time_series_annotation(
                    param.annotation) and value is not None \
                    and not (value is _MISSING):
                # A plain VALUE on a time-series parameter lifts to const at
                # the declared type (hgraph's auto-const rule); conversion
                # errors surface as wiring errors.
                value = _lift_time_series_argument(value, param.annotation)
            if isinstance(value, WiringPort):
                self._check_binding(scope, param, value)
                if all_valid_policy is not None and param.name in all_valid_policy:
                    layout.append(
                        "a" if active_policy is None or param.name in active_policy else "A")
                    _note(param.name)
                    ports.append(_unwrap(value))
                    requested = self._requested_input_shape(
                        scope, param.annotation, value)
                    reference_shapes.append(
                        requested
                        if requested is not None and _hgraph.ref_target(requested) != requested
                        else False)
                    continue
                required = valid_policy is None or param.name in valid_policy
                is_active = active_policy is None or param.name in active_policy
                layout.append({(True, True): "t", (True, False): "u",
                               (False, True): "T", (False, False): "U"}[(is_active, required)])
                _note(param.name)
                ports.append(_unwrap(value))
                requested = self._requested_input_shape(scope, param.annotation, value)
                reference_shapes.append(
                    requested
                    if requested is not None and _hgraph.ref_target(requested) != requested
                    else False)
            else:
                layout.append("s")
                _note(param.name)
                scalars.append(value)
                scalar_values[param.name] = value
        if self._requires is not None:
            try:
                verdict = _run_requires(self._requires, scope.bindings, scalar_values)
            except KeyError as error:
                raise RequirementsNotMetWiringError(
                    f"{self.__name__}: requires= references an unresolved type variable {error}") from error
            if verdict is not True:
                reason = verdict if isinstance(verdict, str) else "requirements not met"
                raise RequirementsNotMetWiringError(f"{self.__name__}: {reason}")
        packed = WiringPort(_hgraph.bundle_port(ports, reference_shapes))
        config = "".join(layout)
        kw_names = [n for n, named in zip(layout_names, layout_by_name) if named]
        if kw_names:
            # ``layout|name,...``: the trailing entries fill BY NAME (all of
            # them when a star group rewrote the fn to keyword-only params,
            # else the keyword-only tail).
            config += "|" + ",".join(kw_names)
        # The suffix is cold-path bridge metadata for native diagnostics. It
        # lets the inspector project the packed ``args._N`` storage back to
        # the user-authored Python parameter names without retaining Python
        # signature objects in the runtime.
        input_names = [
            name
            for marker, name in zip(layout, layout_names)
            if marker in "tuaCTUAP"
        ]
        if input_names:
            config += ";" + ",".join(input_names)
        node_kwargs = {"fn": ref, "config": config, "scalars": _hgraph.any_list(scalars)}
        input_index_by_name = {}
        input_index = 0
        for marker, name in zip(layout, layout_names):
            if marker in "tuaCTUAP":
                input_index_by_name[name] = input_index
                input_index += 1
        recordable_state_type = None
        if self._recordable_state is not None:
            recordable_state_type = self._resolve_recordable_state(
                self._recordable_state, scope)
            node_kwargs["recordable_state_schema"] = recordable_state_type.handle
        for phase in ("start", "stop"):
            lifecycle_fn = getattr(self, f"_{phase}_fn")
            lifecycle_layout, lifecycle_scalars = [], []
            if lifecycle_fn is not None:
                for param in inspect.signature(lifecycle_fn, eval_str=True).parameters.values():
                    # Issue #79: eval is the signature bearer — an eval-named
                    # parameter classifies by eval's annotation, not its own.
                    annotation = self._lifecycle_annotation(param)
                    if (_is_time_series_annotation(annotation)
                            and phase == "stop"):
                        lifecycle_layout.append("i")
                        lifecycle_scalars.append(input_index_by_name[param.name])
                        continue
                    if isinstance(annotation, _RecordableStateExpr):
                        lifecycle_layout.append("R")
                        continue
                    if isinstance(annotation, _StateExpr):
                        lifecycle_layout.append("Q")
                        lifecycle_scalars.append(annotation.factory)
                        continue
                    marker = _INJECTABLE_MARKERS.get(annotation)
                    if marker is not None:
                        lifecycle_layout.append(marker)
                        continue
                    value = scalar_values.get(param.name, _MISSING)
                    if value is _MISSING:
                        if param.default is inspect.Parameter.empty:
                            raise TypeError(
                                f"{self.__name__}.{phase}: scalar '{param.name}' is not supplied by the node call"
                            )
                        value = param.default
                    lifecycle_layout.append("s")
                    lifecycle_scalars.append(value)
            node_kwargs[f"{phase}_fn"] = _hgraph.node_ref(lifecycle_fn or self.fn)
            node_kwargs[f"{phase}_enabled"] = lifecycle_fn is not None
            node_kwargs[f"{phase}_config"] = "".join(lifecycle_layout)
            node_kwargs[f"{phase}_scalars"] = _hgraph.any_list(lifecycle_scalars)
        if self.has_output:
            out_tp = self._out_tp
            if isinstance(out_tp, (_GenericTsExpr, _TypeVarSentinel)):
                resolved = scope.resolve_ts(_pattern_of(out_tp))
                if resolved is not None:
                    out_tp = _TsExpr(resolved, f"resolved[{out_tp!r}]")
            if not isinstance(out_tp, _TsExpr):
                raise TypeError(f"@compute_node '{self.__name__}' needs a TS[...] return annotation")
            op_name = ("__py_compute_recordable" if recordable_state_type is not None
                       else "__py_compute")
            return wire(op_name, packed, output_type=out_tp,
                        __node_label__=self._diagnostic_label(scalar_values),
                        **node_kwargs)
        return wire("__py_sink", packed,
                    __node_label__=self._diagnostic_label(scalar_values),
                    **node_kwargs)


def _make_py_node(fn, *, has_output, active, valid, all_valid, resolvers,
                  overloads, requires, label, deprecated):
    node = _PyNode(
        fn,
        has_output=has_output,
        active=active,
        valid=valid,
        all_valid=all_valid,
        resolvers=resolvers,
        label=label,
        deprecated=deprecated,
    )
    if overloads is not None:
        _register_overload(overloads, node, requires)
    elif requires is not None:
        # a plain node checks its own requires= at wiring (upstream raises
        # RequirementsNotMetWiringError on rejection).
        node._requires = requires
    return node


def compute_node(fn=None, /, node_impl=None, active=None, valid=None, all_valid=None,
                 overloads=None, resolvers=None, requires=None, label=None,
                 deprecated=False):
    """Decorate a Python callable as a native-runtime compute node.

    ``active`` names the inputs that drive invocation; ``valid`` names the
    inputs required to be valid; and ``all_valid`` requires every child of a
    selected structured input to be valid. Each policy accepts a wiring-time
    callable ``(m, **scalars)``. ``resolvers`` maps type variables to
    wiring-time resolver callables. ``overloads=`` registers this node as a
    candidate of an @operator family; ``requires=`` guards its selection.

    ``node_impl`` selects an implementation class from the retired Python
    runtime and is intentionally unavailable in the C++-first runtime.

    :param fn: Callable to decorate. Omit it when configuring the decorator.
    :param node_impl: Retired Python-runtime extension point; only ``None`` is
        supported.
    :param active: Input names, or a wiring-time callable returning them, that
        schedule evaluation when modified.
    :param valid: Input names, or a wiring-time callable returning them, that
        must be valid before evaluation.
    :param all_valid: Structured input names whose children must all be valid.
    :param overloads: Operator contract whose overload set should include this
        node.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :param requires: Wiring-time predicate selecting this overload. Return
        ``True`` to accept it, or ``False``/a message string to reject it.
    :param label: Diagnostic label used in the wired graph.
    :param deprecated: ``True`` or a message string to emit a deprecation
        warning when the node is wired.
    :return: A compute-node decorator or the decorated node.
    """
    if node_impl is not None:
        raise NotImplementedError(
            "compute_node(node_impl=...) is a legacy Python-runtime extension; "
            "implement the callable directly or provide a native C++ node")
    if fn is None:
        return lambda f: _make_py_node(
            f, has_output=True, active=active, valid=valid, all_valid=all_valid,
            resolvers=resolvers, overloads=overloads, requires=requires,
            label=label, deprecated=deprecated)
    return _make_py_node(
        fn, has_output=True, active=active, valid=valid, all_valid=all_valid,
        resolvers=resolvers, overloads=overloads, requires=requires,
        label=label, deprecated=deprecated)


def sink_node(fn=None, /, node_impl=None, active=None, valid=None, all_valid=None,
              overloads=None, resolvers=None, requires=None, label=None,
              deprecated=False):
    """Decorate a side-effecting Python callable as a native-runtime sink node.

    Sink nodes use the same activation, validity, overload and type-resolution
    rules as :func:`hgraph.compute_node`, but have no output.

    :param fn: Callable to decorate. Omit it when configuring the decorator.
    :param node_impl: Retired Python-runtime extension point; only ``None`` is
        supported.
    :param active: Input names, or a wiring-time callable returning them, that
        schedule evaluation when modified.
    :param valid: Input names, or a wiring-time callable returning them, that
        must be valid before evaluation.
    :param all_valid: Structured input names whose children must all be valid.
    :param overloads: Operator contract whose overload set should include this
        node.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :param requires: Wiring-time predicate selecting this overload. Return
        ``True`` to accept it, or ``False``/a message string to reject it.
    :param label: Diagnostic label used in the wired graph.
    :param deprecated: ``True`` or a message string to emit a deprecation
        warning when the node is wired.
    :return: A sink-node decorator or the decorated node.
    """
    if node_impl is not None:
        raise NotImplementedError(
            "sink_node(node_impl=...) is a legacy Python-runtime extension; "
            "implement the callable directly or provide a native C++ node")
    if fn is None:
        return lambda f: _make_py_node(
            f, has_output=False, active=active, valid=valid, all_valid=all_valid,
            resolvers=resolvers, overloads=overloads, requires=requires,
            label=label, deprecated=deprecated)
    return _make_py_node(
        fn, has_output=False, active=active, valid=valid, all_valid=all_valid,
        resolvers=resolvers, overloads=overloads, requires=requires,
        label=label, deprecated=deprecated)

def lift(fn, inputs=None, output=None, active=None, valid=None, all_valid=None,
         dedup_output=False, defaults=None):
    """hgraph's lift: wrap a plain scalar function as a compute node with a
    TS-lifted signature (each scalar annotation becomes TS[...]; ``inputs``/
    ``output`` override per name). Inputs cross into python nodes as plain
    values in this bridge, so the wrapped callable is the function itself."""
    from .._types import TS

    sig, _ = _wiring_signature_of(fn)

    def _scalar(arg):
        # python nodes receive live TimeSeries VIEWS; the lifted fn is a
        # plain scalar function (upstream passes a.value if a.valid).
        if isinstance(arg, _hgraph.TimeSeries):
            return arg.value if arg.valid else None
        return arg

    def _lifted(*args, **kwargs):
        return fn(*(_scalar(a) for a in args), **{k: _scalar(v) for k, v in kwargs.items()})

    def _ts(tp):
        return tp if isinstance(tp, (_TsExpr, _GenericTsExpr)) else TS[tp]

    parameters, annotations = [], {}
    for name, parameter in sig.parameters.items():
        tp = (inputs or {}).get(name, parameter.annotation)
        if tp is inspect.Parameter.empty:
            raise WiringError(f"lift: parameter '{name}' needs an annotation or an inputs= entry")
        ts_tp = _ts(tp)
        annotations[name] = ts_tp
        default = (defaults or {}).get(name, parameter.default)
        parameters.append(parameter.replace(annotation=ts_tp, default=default))
    return_annotation = output if output is not None else sig.return_annotation
    if return_annotation in (inspect.Signature.empty, None):
        raise WiringError(f"lift: '{getattr(fn, '__name__', fn)!r}' needs a return annotation or output=")
    annotations["return"] = _ts(return_annotation)
    _lifted.__name__ = getattr(fn, "__name__", "lifted")
    _lifted.__qualname__ = _lifted.__name__
    _lifted.__annotations__ = annotations
    _lifted.__signature__ = sig.replace(parameters=parameters,
                                        return_annotation=annotations["return"])
    node = _PyNode(
        _lifted, has_output=True, active=active, valid=valid,
        all_valid=all_valid)
    if not dedup_output:
        return node

    def _deduplicated(*args, **kwargs):
        return wire("dedup", node(*args, **kwargs))

    _deduplicated.__name__ = _lifted.__name__
    _deduplicated.__qualname__ = _lifted.__qualname__
    _deduplicated.__annotations__ = annotations
    _deduplicated.__signature__ = _lifted.__signature__

    from ._graph import graph

    return graph(_deduplicated)




class _Generator:
    """@generator: a Python generator function yielding (datetime, value)
    pairs; each pair is emitted at its ABSOLUTE time. Wiring-time arguments
    are captured per call (each call is a distinct source node)."""

    def __init__(self, fn, *, resolvers=None, requires=None, label=None,
                 deprecated=False):
        self.fn = fn
        self.__name__ = fn.__name__
        signature, self._default_type_var = _wiring_signature_of(fn)
        self._out_tp = signature.return_annotation
        self._resolvers = dict(resolvers) if resolvers else None
        self._requires = requires
        self._label = label
        self._deprecated = deprecated
        self._stop_fn = None

    def _stop_annotation(self, parameter):
        """Issue #79: stop parameters match the generator signature by name
        and nothing else — a name it declares takes the generator's
        definition; other names keep their own annotation."""
        for fn_param in inspect.signature(self.fn, eval_str=True).parameters.values():
            if fn_param.name == parameter.name:
                return fn_param.annotation
        return parameter.annotation

    def stop(self, fn):
        for parameter in _wiring_signature_of(fn)[0].parameters.values():
            annotation = self._stop_annotation(parameter)
            injectable = (
                annotation in _INJECTABLE_MARKERS
                or isinstance(annotation, _StateExpr)
            )
            if injectable:
                continue
            if _is_time_series_annotation(annotation):
                raise TypeError(
                    f"@{self.__name__}.stop supports wiring-time scalars "
                    "and injectables only")
        self._stop_fn = fn
        return fn

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        return extract_signature(self.fn, WiringNodeType.PULL_SOURCE_NODE)

    def __call__(self, *args, **kwargs):
        _warn_deprecated(self.__name__, self._deprecated)
        signature = inspect.signature(self.fn, eval_str=True)
        user_parameters = [
            parameter
            for parameter in signature.parameters.values()
            if (parameter.annotation not in _INJECTABLE_MARKERS and
                not isinstance(parameter.annotation, _StateExpr))
        ]
        user_signature = signature.replace(parameters=user_parameters)
        bound_call = user_signature.bind(*args, **kwargs)
        bound_call.apply_defaults()
        scalar_values = dict(bound_call.arguments)
        scope = _hgraph.ResolutionScope()
        _apply_wiring_resolvers(scope, self._resolvers, scalar_values)
        if self._requires is not None:
            verdict = _run_requires(self._requires, scope.bindings, scalar_values)
            if verdict is not True:
                reason = verdict if isinstance(verdict, str) else "requirements not met"
                raise RequirementsNotMetWiringError(f"{self.__name__}: {reason}")
        out_tp = self._out_tp
        if isinstance(out_tp, (_GenericTsExpr, _TypeVarSentinel)):
            from .._types import _pattern_of

            resolved = scope.resolve_ts(_pattern_of(out_tp))
            if resolved is not None:
                out_tp = _TsExpr(resolved, f"resolved[{out_tp!r}]")
        if not isinstance(out_tp, _TsExpr):
            raise TypeError(f"@generator '{self.__name__}' needs a TS[...] return annotation")
        layout = []
        scalars = []
        keyword_names = []
        for parameter in signature.parameters.values():
            if isinstance(parameter.annotation, _StateExpr):
                layout.append("Q")
                scalars.append(parameter.annotation.factory)
                continue
            marker = _INJECTABLE_MARKERS.get(parameter.annotation)
            if marker is not None:
                layout.append(marker)
            else:
                layout.append("s")
                scalars.append(bound_call.arguments[parameter.name])
            if parameter.kind is inspect.Parameter.KEYWORD_ONLY:
                keyword_names.append(parameter.name)
        config = "".join(layout)
        if keyword_names:
            config += "|" + ",".join(keyword_names)
        stop_layout = []
        stop_scalars = []
        if self._stop_fn is not None:
            for parameter in inspect.signature(self._stop_fn, eval_str=True).parameters.values():
                annotation = self._stop_annotation(parameter)
                if isinstance(annotation, _StateExpr):
                    stop_layout.append("Q")
                    stop_scalars.append(annotation.factory)
                    continue
                marker = _INJECTABLE_MARKERS.get(annotation)
                if marker is not None:
                    stop_layout.append(marker)
                    continue
                if parameter.name in bound_call.arguments:
                    value = bound_call.arguments[parameter.name]
                elif parameter.default is not inspect.Parameter.empty:
                    value = parameter.default
                else:
                    raise TypeError(
                        f"{self.__name__}.stop: scalar '{parameter.name}' "
                        "is not supplied by the generator call")
                stop_layout.append("s")
                stop_scalars.append(value)
        return wire(
            "__py_generator",
            __node_label__=self._label or getattr(self.fn, "__name__", None),
            fn=_hgraph.node_ref(self.fn),
            config=config,
            scalars=_hgraph.any_list(scalars),
            stop_fn=_hgraph.node_ref(self._stop_fn or self.fn),
            stop_enabled=self._stop_fn is not None,
            stop_config="".join(stop_layout),
            stop_scalars=_hgraph.any_list(stop_scalars),
            output_type=out_tp,
        )


def generator(fn=None, overloads=None, resolvers=None, requires=None, label=None,
              deprecated=False):
    """Decorate a Python generator that emits absolute-time output ticks.

    The decorated callable yields ``(datetime, value)`` pairs and must declare
    a ``TS[...]`` return annotation. Wiring-time arguments are captured per
    call. A stop hook may be registered with ``@source.stop``.

    :param fn: Generator callable to decorate. Omit it when configuring the
        decorator.
    :param overloads: Operator contract whose overload set should include this
        source.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :param requires: Wiring-time predicate selecting this overload. Return
        ``True`` to accept it, or ``False``/a message string to reject it.
    :param label: Diagnostic label used in the wired graph.
    :param deprecated: ``True`` or a message string to emit a deprecation
        warning when the source is wired.
    :return: A generator decorator or the decorated source.
    """
    def _make(f):
        wrapped = _Generator(
            f, resolvers=resolvers, requires=requires, label=label,
            deprecated=deprecated)
        if overloads is not None:
            _register_overload(overloads, wrapped, requires)
        return wrapped

    if fn is None:
        return _make
    return _make(fn)

class _PushQueue:
    """hgraph's @push_queue: the wrapped function is the node's START
    lifecycle hook - called with the sender callable as its first argument
    (plus any wiring-time scalars as kwargs) once the real-time graph is
    running. sender(value) is thread-safe from any Python thread."""

    def __init__(self, fn, tp, conflate, *, resolvers=None, requires=None,
                 label=None, deprecated=False):
        self.fn = fn
        self.tp = tp
        self.conflate = conflate
        self.__name__ = fn.__name__
        signature, self._default_type_var = _default_type_var_of(
            inspect.signature(fn, eval_str=True))
        parameters = list(signature.parameters.values())
        if not parameters:
            raise TypeError(f"@push_queue '{self.__name__}' requires a sender parameter")
        self._signature = signature.replace(
            parameters=parameters[1:], return_annotation=tp)
        self._wiring_signature = self._signature
        self.__signature__ = self._signature
        self._resolvers = dict(resolvers) if resolvers else None
        self._requires = requires
        self._label = label
        self._deprecated = deprecated

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        return extract_signature(self, WiringNodeType.PUSH_SOURCE_NODE)

    def __call__(self, *args, **kwargs):
        _warn_deprecated(self.__name__, self._deprecated)
        bound_call = self._signature.bind(*args, **kwargs)
        bound_call.apply_defaults()
        scalar_values = dict(bound_call.arguments)
        scope = _hgraph.ResolutionScope()
        _apply_wiring_resolvers(scope, self._resolvers, scalar_values)
        if self._requires is not None:
            verdict = _run_requires(self._requires, scope.bindings, scalar_values)
            if verdict is not True:
                reason = verdict if isinstance(verdict, str) else "requirements not met"
                raise RequirementsNotMetWiringError(f"{self.__name__}: {reason}")
        out_tp = self.tp
        if isinstance(out_tp, (_GenericTsExpr, _TypeVarSentinel)):
            from .._types import _pattern_of

            resolved = scope.resolve_ts(_pattern_of(out_tp))
            if resolved is not None:
                out_tp = _TsExpr(resolved, f"resolved[{out_tp!r}]")
        if not isinstance(out_tp, _TsExpr):
            raise TypeError(f"@push_queue '{self.__name__}' needs a resolved TS[...] output type")
        w = _current_wiring()
        fn = self.fn

        def on_start(sender):
            fn(sender.send, *bound_call.args, **bound_call.kwargs)

        port, _sender = w.push_source(
            _unwrap(out_tp), self.conflate, on_start,
            self._label or self.__name__)
        return WiringPort(port)


def push_queue(tp, overloads=None, resolvers=None, requires=None, label=None,
               deprecated=False, *, conflate=False):
    """Decorate an external callback source for real-time graph execution.

    The decorated start hook receives a thread-safe ``sender(value)`` callable
    as its first argument. It may retain that sender and invoke it from any
    Python thread while the graph is running.

    :param tp: Output time-series type, such as ``TS[int]``.
    :param overloads: Operator contract whose overload set should include this
        source.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :param requires: Wiring-time predicate selecting this overload. Return
        ``True`` to accept it, or ``False``/a message string to reject it.
    :param label: Diagnostic label used in the wired graph.
    :param deprecated: ``True`` or a message string to emit a deprecation
        warning when the source is wired.
    :param conflate: Retain only the latest pending value when producers run
        ahead of graph evaluation.
    :return: A push-queue decorator.
    """
    def decorator(fn):
        wrapped = _PushQueue(
            fn, tp, conflate, resolvers=resolvers, requires=requires,
            label=label, deprecated=deprecated)
        if overloads is not None:
            _register_overload(overloads, wrapped, requires)
        return wrapped

    return decorator
