"""_GraphFn/@graph, graph-fn wrapping (the borrowed-wiring re-entry),
signature auto-resolution and @component."""
import inspect

import _hgraph

from .._types import (_ContextExpr, _GenericTsExpr, _TsExpr,
                      _TypeVarSentinel, _pattern_of, _type_var_name)
from ._core import (ParseError, WiringError, WiringPort, _current_wiring,
                    _resolve_context, _unwrap, _wiring_stack, wire)
from ._markers import _INJECTABLE_MARKERS
from ._node import (_PyNode, _is_time_series_annotation,
                    _lift_time_series_argument, _warn_deprecated)
from ._operator import _register_overload, _run_requires
from ._resolution import _apply_resolvers
from ._state import GlobalState

def _wrap_graph_fn(gfn, *, input_names=None, scalar_bindings=None):
    """Erase a Python @graph function into a WiredFn: the wrapper runs the
    function against whatever Wiring the C++ side supplies (inline OR a
    fresh child during sub-graph compilation), pushing it as the active
    wiring context. Identity is the user function object."""
    user_fn = gfn.fn if isinstance(gfn, _GraphFn) else gfn
    # Introspect the REAL signature (unwrap @compute_node/@graph wrappers);
    # injectable/context parameters are not wired inputs.
    sig = inspect.signature(getattr(user_fn, "fn", user_fn), eval_str=True)
    names = list(input_names) if input_names is not None else [
        p.name for p in sig.parameters.values()
        if p.annotation not in _INJECTABLE_MARKERS and not isinstance(p.annotation, _ContextExpr)
    ]
    scalar_bindings = dict(scalar_bindings or {})
    # Node decorators are authoritative even when the wrapped user function is
    # unannotated. For graphs/lambdas, only explicit ``-> None`` marks a sink;
    # an unannotated callable remains provisionally output-producing.
    has_output = gfn.has_output if isinstance(gfn, _PyNode) else sig.return_annotation is not None

    def wrapper(borrowed_wiring, ports):
        _wiring_stack.append(borrowed_wiring)
        try:
            supplied = dict(scalar_bindings)
            supplied.update(zip(names, (WiringPort(port) for port in ports)))
            call_args = []
            call_kwargs = {}
            positional_gap = False
            for parameter in sig.parameters.values():
                if parameter.name not in supplied:
                    if parameter.kind in (
                            inspect.Parameter.POSITIONAL_ONLY,
                            inspect.Parameter.POSITIONAL_OR_KEYWORD):
                        positional_gap = True
                    continue
                value = supplied[parameter.name]
                if (parameter.kind is inspect.Parameter.POSITIONAL_ONLY or
                        (parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
                         and not positional_gap)):
                    call_args.append(value)
                else:
                    call_kwargs[parameter.name] = value
            label = getattr(gfn, "_label", None) or getattr(
                gfn, "__name__", "<python-graph>")
            with borrowed_wiring._graph_wiring_scope(label):
                if isinstance(gfn, _GraphFn):
                    # The wrapper owns this nested scope. Calling __call__
                    # would emit the same graph scope a second time.
                    out = gfn._call(*call_args, **call_kwargs)
                elif isinstance(gfn, _PyNode):
                    out = gfn(*call_args, **call_kwargs)
                else:
                    out = user_fn(*call_args, **call_kwargs)
            if out is None:
                return None
            if not isinstance(out, WiringPort):
                # a plain value returned from a @graph lifts to const
                # (hgraph parity - dispatch branches `return "woof"`).
                out = wire("const", out)
            raw = _unwrap(out)
            # Common C++ subgraph finalization converts a structural result
            # into its zero-copy REF terminal. Leaving the structural port
            # intact here keeps Python and native graph functions on the same
            # wiring path and preserves partially-bound field references.
            if not raw.is_structural and raw.has_path and not (
                    isinstance(out_tp, _TsExpr) and out_tp.is_ref):
                # Python graph outputs expose referenced values unless the
                # author explicitly declares a REF return. Preserve the
                # projected endpoint path while giving map_/mesh_ the plain
                # child schema used by equivalent C++ wiring.
                raw = raw.dereferenced
            return raw
        finally:
            _wiring_stack.pop()

    out_tp = sig.return_annotation
    out_handle = out_tp.handle if isinstance(out_tp, _TsExpr) else None
    input_handles = []
    input_patterns = []
    for name in names:
        annotation = sig.parameters[name].annotation
        input_handles.append(
            annotation.handle if isinstance(annotation, _TsExpr) else None)
        try:
            input_patterns.append(_pattern_of(annotation))
        except TypeError:
            input_patterns.append(None)
    identity = wrapper if input_names is not None or scalar_bindings else gfn
    return _hgraph.graph_fn(
        wrapper, identity, names, has_output, output_type=out_handle,
        input_types=input_handles, input_patterns=input_patterns,
        user_callable=gfn)


def _prepare_higher_order_call(func, args, kwargs, *, default_key_arg):
    """Bind wiring-time scalar parameters into a Python mapped callable.

    Native nested graphs expose only time-series boundaries. Python scalar
    parameters are therefore configuration captured while wiring the child,
    rather than const time-series inputs owned by every child instance.
    """
    if isinstance(func, (_hgraph.WiredFn, str)) or not isinstance(
            func, (_GraphFn, _PyNode)) and not callable(func):
        return _as_wired(func), args, kwargs
    if (not isinstance(func, (_GraphFn, _PyNode))
            and getattr(func, "__name__", None) != "<lambda>"):
        return _as_wired(func), args, kwargs

    user_fn = func.fn if isinstance(func, (_GraphFn, _PyNode)) else func
    signature = inspect.signature(getattr(user_fn, "fn", user_fn), eval_str=True)
    parameters = [
        parameter for parameter in signature.parameters.values()
        if parameter.annotation not in _INJECTABLE_MARKERS
        and not isinstance(parameter.annotation, _ContextExpr)
    ]
    if any(parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD) for parameter in parameters):
        return _as_wired(func), args, kwargs

    key_arg = kwargs.get("__key_arg__") or default_key_arg
    takes_key = bool(parameters and parameters[0].name == key_arg)
    call_parameters = parameters[1:] if takes_key else parameters
    callable_signature = signature.replace(parameters=call_parameters)
    ordinary_kwargs = {
        name: value for name, value in kwargs.items()
        if not name.startswith("__")
    }
    bound = callable_signature.bind_partial(*args, **ordinary_kwargs)

    input_names = [parameters[0].name] if takes_key else []
    scalar_bindings = {}
    prepared_args = []
    for parameter in call_parameters:
        value = bound.arguments.get(parameter.name, inspect.Parameter.empty)
        if _is_time_series_annotation(parameter.annotation):
            input_names.append(parameter.name)
            if value is not inspect.Parameter.empty:
                prepared_args.append(value)
            continue
        if parameter.annotation is inspect.Parameter.empty:
            if value is inspect.Parameter.empty:
                input_names.append(parameter.name)
                continue
            if isinstance(value, WiringPort):
                input_names.append(parameter.name)
                prepared_args.append(value)
                continue
        if value is not inspect.Parameter.empty:
            scalar_bindings[parameter.name] = value

    special_kwargs = {
        name: value for name, value in kwargs.items()
        if name.startswith("__")
    }
    wrapped = None
    cache = getattr(func, "_wired_fn_cache", None)
    if cache is not None:
        candidate = (
            tuple(input_names),
            tuple(sorted(scalar_bindings.items())),
        )
        try:
            hash(candidate)
        except TypeError:
            pass
        else:
            wrapped = cache.get(candidate)
            if wrapped is None:
                wrapped = _wrap_graph_fn(
                    func,
                    input_names=input_names,
                    scalar_bindings=scalar_bindings,
                )
                cache[candidate] = wrapped
    if wrapped is None:
        wrapped = _wrap_graph_fn(
            func, input_names=input_names, scalar_bindings=scalar_bindings)
    return wrapped, tuple(prepared_args), special_kwargs


def _as_wired(func):
    """Accept an operator name, an operator_function, a @graph-decorated
    function, a @compute_node, a WiredFn handle - or a bare LAMBDA (the
    anonymous convenience). Plain named functions must be tagged @graph."""
    if isinstance(func, _hgraph.WiredFn):
        return func
    if isinstance(func, (_GraphFn, _PyNode)):
        return _wrap_graph_fn(func)
    from ._operator import _Dispatch, _Operator

    if isinstance(func, _Dispatch):
        return _wrap_graph_fn(func)
    if isinstance(func, _Operator):
        output = inspect.signature(func.fn, eval_str=True).return_annotation
        output_handle = output.handle if isinstance(output, _TsExpr) else None
        return _hgraph.wired_op(func._registry_name, output_handle)
    if callable(func) and not isinstance(func, str):
        name = getattr(func, "__name__", None)
        if name is not None and name in _hgraph.operator_names():
            expected = getattr(func, "_output_type", None)
            return _hgraph.wired_op(name, getattr(expected, "handle", None))
        if name == "<lambda>":
            return _wrap_graph_fn(func)
        raise TypeError(
            f"'{name}' must be decorated with @graph (or @compute_node) to be used as a wired "
            "function; bare lambdas are also accepted")
    return _hgraph.wired_op(func)

class _ResolvedSize:
    """The object an AUTO_RESOLVE'd type[SIZE] parameter receives: hgraph's
    Size-like carrier with the concrete ``.SIZE``."""

    __slots__ = ("SIZE",)

    def __init__(self, size):
        self.SIZE = size

    def __repr__(self):
        return f"Size[{self.SIZE}]"


def _graph_auto_resolve(signature, arguments, resolvers=None, requires=None,
                        seed_bindings=None):
    """Fill ``x: type[SENTINEL] = AUTO_RESOLVE`` graph parameters: match
    every time-series parameter's TYPE PATTERN against its wired port in a
    C++ resolution scope, then read each sentinel's binding from it."""
    import typing

    from .._types import _TsExpr, _GenericTsExpr, _TypeVarSentinel, _pattern_of

    scope = _hgraph.ResolutionScope()
    for name, resolved in (seed_bindings or {}).items():
        try:
            _PyNode._bind_resolved(scope, name, resolved)
        except (RuntimeError, ValueError, TypeError):
            pass   # a binding kind the scope cannot seed is simply unavailable
    for name, param in signature.parameters.items():
        value = arguments.get(name)
        if isinstance(value, WiringPort) and isinstance(
                param.annotation, (_GenericTsExpr, _TypeVarSentinel)):
            try:
                scope.match(_pattern_of(param.annotation), _unwrap(value).ts_type)
            except (RuntimeError, ValueError, TypeError):
                pass   # inconsistent bindings surface at the consuming node

    scalar_values = {
        name: value for name, value in arguments.items()
        if not isinstance(value, WiringPort)
    }
    _apply_resolvers(scope, resolvers, scalar_values)
    if requires is not None:
        verdict = _run_requires(requires, scope.bindings, scalar_values)
        if verdict is not True:
            reason = verdict if isinstance(verdict, str) else "requirements not met"
            raise WiringError(f"graph requirements not met: {reason}")

    resolved = {}
    for name, param in signature.parameters.items():
        from .._types import AUTO_RESOLVE

        if param.default is not AUTO_RESOLVE or name in arguments:
            continue
        args = typing.get_args(param.annotation)
        sentinel = args[0] if args else None
        if not isinstance(sentinel, _TypeVarSentinel):
            raise WiringError(
                f"AUTO_RESOLVE parameter '{name}' needs a type[TYPEVAR] annotation")
        size = scope.find_size(_type_var_name(sentinel))
        if size is not None:
            resolved[name] = _ResolvedSize(size)
            continue
        ts = scope.find_ts(_type_var_name(sentinel))
        if ts is not None:
            resolved[name] = _TsExpr(ts, f"resolved[{sentinel!r}]")
            continue
        scalar = scope.find_scalar(_type_var_name(sentinel))
        if scalar is not None:
            resolved[name] = _hgraph.python_type_for_value(scalar)
            continue
        raise WiringError(
            f"AUTO_RESOLVE could not resolve '{name}' ({sentinel!r}) from the wired arguments")
    return resolved

class _Component:
    """Python signature adapter for the C++ component wiring primitive."""

    def __init__(self, fn, recordable_id=None, *, resolvers=None, label=None,
                 deprecated=False):
        self.fn = getattr(fn, "fn", fn)
        self.__name__ = fn.__name__
        self.recordable_id = recordable_id or fn.__name__
        self._graph = _GraphFn(
            self.fn, resolvers=resolvers, label=label, deprecated=deprecated)
        self._signature = inspect.signature(self.fn, eval_str=True)
        self._params = list(self._signature.parameters.values())
        # eval_node/introspection must see the USER signature, not __call__'s.
        self.__signature__ = self._signature

    @staticmethod
    def _is_ts(param, value):
        return isinstance(value, WiringPort) or isinstance(
            param.annotation, (_TsExpr, _GenericTsExpr, _TypeVarSentinel))

    def __call__(self, *args, **kwargs):
        bound = self._signature.bind(*args, **kwargs)
        call_args = dict(bound.arguments)
        input_names = []
        input_ports = []
        for param in self._params:
            key = param.name
            if key not in call_args or not self._is_ts(param, call_args[key]):
                continue
            input_names.append(key)
            input_ports.append(_unwrap(call_args[key]))

        def compose(wrapped_ports):
            for key, port in zip(input_names, wrapped_ports):
                call_args[key] = WiringPort(port)
            out = self._graph(**call_args)
            return None if out is None else _unwrap(out)

        out = _hgraph.component(
            _current_wiring(), self.recordable_id, input_names, input_ports, compose)
        return None if out is None else WiringPort(out)


def component(fn=None, *, recordable_id=None, resolvers=None, label=None,
              deprecated=False):
    if fn is None:
        return lambda f: _Component(
            f, recordable_id, resolvers=resolvers, label=label,
            deprecated=deprecated)
    return _Component(
        fn, recordable_id, resolvers=resolvers, label=label,
        deprecated=deprecated)

class _GraphFn:
    """The @graph decorator: a plain composition function. Inside an active
    wiring it inlines (a call is just a call); run_graph/eval_node make it a
    top level."""

    def __init__(self, fn, *, resolvers=None, requires=None, label=None,
                 deprecated=False):
        self.fn = fn
        self._signature = inspect.signature(fn, eval_str=True)
        self._wiring_signature = self._signature
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__
        self.__signature__ = self._signature
        self._resolvers = dict(resolvers) if resolvers else None
        self._requires = requires
        self._label = label
        self._deprecated = deprecated
        self._wired_fn_cache = {}
        out = self._signature.return_annotation
        if isinstance(out, type):
            from .._types import TimeSeriesSchema

            if issubclass(out, TimeSeriesSchema):
                # hgraph parity: a bare schema class is not a time-series
                # type - the return must be TSB[Schema].
                raise ParseError(
                    f"@graph '{self.__name__}': return type {out.__name__} is a schema class - "
                    f"use TSB[{out.__name__}]")

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        return extract_signature(self.fn, WiringNodeType.GRAPH)

    def __getitem__(self, item):
        # g[str] pins the graph's typevar-defaulted params (hgraph's
        # DEFAULT[SCALAR_1] pattern): params whose default is a typevar
        # sentinel receive the subscript items in declaration order. Slice
        # syntax (g[TIME_SERIES_TYPE: TS[int]]) resolves annotated typevars.
        from .._types import AUTO_RESOLVE, _TypeVarSentinel
        import typing

        items = item if isinstance(item, tuple) else (item,)
        resolved_types = {
            value.start: value.stop
            for value in items
            if isinstance(value, slice) and isinstance(value.start, _TypeVarSentinel)
        }
        scalar_items = [value for value in items if not isinstance(value, slice)]
        pinned, index = {}, 0
        for name, param in self._signature.parameters.items():
            is_type_default = (
                param.default is AUTO_RESOLVE and
                typing.get_origin(param.annotation) is type
            )
            if is_type_default:
                type_args = typing.get_args(param.annotation)
                sentinel = type_args[0] if type_args else None
                if sentinel in resolved_types:
                    pinned[name] = resolved_types[sentinel]
                    continue
            if (isinstance(param.default, _TypeVarSentinel) or is_type_default) \
                    and index < len(scalar_items):
                pinned[name] = scalar_items[index]
                index += 1
        import functools

        wrapper = functools.partial(self, **pinned)
        wrapper.__name__ = self.__name__
        parameters = [
            param.replace(annotation=resolved_types.get(param.annotation, param.annotation))
            for param in self._signature.parameters.values()
        ]
        return_annotation = resolved_types.get(self._signature.return_annotation,
                                               self._signature.return_annotation)
        wrapper.__signature__ = self._signature.replace(parameters=parameters,
                                                       return_annotation=return_annotation)
        return wrapper

    def _with_resolution(self, bindings):
        """Return a call-local graph seeded with the overload registry's
        resolved type variables (the _PyNode contract): AUTO_RESOLVE
        parameters whose sentinel is fixed only by the requested output —
        e.g. ``replay[TS[int]]`` — read the dispatch bindings instead of
        starting a second, incomplete resolution pass."""
        import copy

        resolved = copy.copy(self)
        resolved._seed_bindings = dict(bindings)
        resolved._wired_fn_cache = {}
        return resolved

    def __call__(self, *args, **kwargs):
        from contextlib import nullcontext

        wiring = _current_wiring()
        scope_factory = getattr(wiring, "_graph_wiring_scope", None)
        scope = (
            scope_factory(self._label or self.__name__)
            if scope_factory is not None else nullcontext()
        )
        with scope:
            return self._call(*args, **kwargs)

    def _call(self, *args, **kwargs):
        _warn_deprecated(self.__name__, self._deprecated)
        bound = self._signature.bind_partial(*args, **kwargs)
        context_scope = _hgraph.ResolutionScope()
        for param in self._signature.parameters.values():
            if param.annotation is GlobalState and param.name not in bound.arguments:
                bound.arguments[param.name] = GlobalState.instance()
            value = bound.arguments.get(param.name)
            if isinstance(param.annotation, _ContextExpr):
                requirement = value if param.name in bound.arguments else param.default
                if isinstance(requirement, WiringPort):
                    continue
                from .._types import _Required, _pattern_of

                name = None
                required = False
                if isinstance(requirement, _Required):
                    required, name = True, requirement.name
                elif isinstance(requirement, str):
                    name = requirement
                resolved = _resolve_context(
                    param.annotation, name, context_scope)
                if resolved is not None:
                    bound.arguments[param.name] = resolved
                    continue
                if required:
                    where = f" with name {name}" if name else ""
                    raise WiringError(
                        f"no context published for '{param.name}'{where} of '{self.__name__}'")
                resolved_type = getattr(param.annotation.ts, "handle", None)
                if resolved_type is None:
                    resolved_type = context_scope.resolve_ts(
                        _pattern_of(param.annotation.ts))
                bound.arguments[param.name] = (
                    wire(
                        "nothing",
                        output_type=_TsExpr(
                            resolved_type, f"resolved[{param.annotation.ts!r}]"),
                    )
                    if resolved_type is not None else None
                )
                continue
            if (param.name in bound.arguments and
                    param.kind in (inspect.Parameter.VAR_POSITIONAL,
                                   inspect.Parameter.VAR_KEYWORD) and
                    _is_time_series_annotation(param.annotation)):
                def lift_variadic(item):
                    return item if item is None or isinstance(item, WiringPort) \
                        else wire("const", item)

                if param.kind is inspect.Parameter.VAR_POSITIONAL:
                    bound.arguments[param.name] = tuple(
                        lift_variadic(item) for item in value)
                else:
                    bound.arguments[param.name] = {
                        key: lift_variadic(item) for key, item in value.items()
                    }
                continue
            if (param.name in bound.arguments and value is not None
                    and not isinstance(value, WiringPort)
                    and _is_time_series_annotation(param.annotation)):
                bound.arguments[param.name] = _lift_time_series_argument(
                    value, param.annotation)
        bound.arguments.update(_graph_auto_resolve(
            self._signature, bound.arguments, self._resolvers, self._requires,
            getattr(self, "_seed_bindings", None)))
        result = self.fn(*bound.args, **bound.kwargs)
        if isinstance(result, dict) and result and all(isinstance(v, WiringPort) for v in result.values()):
            # hgraph parity: a dict literal of ports returned from a @graph
            # coerces to its annotated TSB output (a structural bundle when
            # the annotation is generic/absent).
            annotation = self._signature.return_annotation
            if isinstance(annotation, _TsExpr) and annotation.handle.is_tsb:
                fields = {k: _unwrap(v) for k, v in result.items()}
                return WiringPort(_hgraph.tsb_port(annotation.handle, fields))
            fields = [(k, _unwrap(v).ts_type) for k, v in result.items()]
            tsb_type = _hgraph.un_named_tsb_type(fields)
            return WiringPort(_hgraph.tsb_port(tsb_type, {k: _unwrap(v) for k, v in result.items()}))
        if (result is not None and not isinstance(result, WiringPort)
                and _is_time_series_annotation(self._signature.return_annotation)):
            # hgraph parity: a plain value returned from a @graph with a
            # time-series return annotation lifts to const of that type.
            return _lift_time_series_argument(
                result, self._signature.return_annotation)
        return result


def graph(fn=None, overloads=None, resolvers=None, requires=None, label=None,
          deprecated=False):
    def _make(f):
        wrapped = _GraphFn(
            f, resolvers=resolvers, requires=requires, label=label,
            deprecated=deprecated)
        if overloads is not None:
            _register_overload(overloads, wrapped, requires)
        return wrapped

    if fn is None:
        return _make
    return _make(fn)
