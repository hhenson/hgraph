"""@operator/@dispatch: overload registration and registry dispatch.

The standing ruling: the C++ registry's pattern matching owns ALL dispatch —
no label/name matching on the Python side. Back-edges to ``._node``/
``._graph``/``._compose`` are lazy in-function imports (call-time only)."""
import inspect

import _hgraph

from .._types import (_ContextExpr, _TsExpr, _TypeVarSentinel,
                      _type_var_name, _type_variables_of,
                      wiring_signature_of as _wiring_signature_of)
from ._core import (WiringError, WiringPort, _OperatorFunction, _unwrap,
                    _wiring_stack, wire)
from ._markers import (_INJECTABLE_MARKERS, _RecordableStateExpr,
                       _StateExpr, _is_object_vt)
from ._resolution import (_BindingsMap, _apply_resolvers,
                          _bind_native_resolution, _binding_for_type_value,
                          _invoke_resolution_callable, _match_type_argument,
                          _python_value_for_binding, _resolution_binding)


def _is_hidden_node_parameter(parameter):
    """Whether a node parameter is supplied by the runtime, not its caller."""
    annotation = parameter.annotation
    return (
        parameter.name == "_output"
        or annotation in _INJECTABLE_MARKERS
        or isinstance(
            annotation,
            (_ContextExpr, _StateExpr, _RecordableStateExpr),
        )
    )

class _Operator:
    """hgraph's @operator: an overloadable signature root. Implementations
    attach via ``@compute_node(overloads=op)`` / ``@graph(overloads=op)``;
    a call dispatches through the C++ registry (the standing ruling: the
    registry's pattern matching owns ALL dispatch). The registry name is
    unique per operator OBJECT - tests re-declare same-named operators
    freely against the process-global registry."""

    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.__name__
        self.__qualname__ = getattr(fn, "__qualname__", fn.__name__)
        self.__doc__ = fn.__doc__
        self._wiring_signature, self._default_type_var = _wiring_signature_of(fn)
        variables = {}
        for parameter in self._wiring_signature.parameters.values():
            for variable in _type_variables_of(
                    (parameter.annotation, parameter.default)):
                variables.setdefault(_type_var_name(variable), variable)
        for variable in _type_variables_of(
                self._wiring_signature.return_annotation):
            variables.setdefault(_type_var_name(variable), variable)
        self._type_variables = tuple(variables.values())
        # Present the DECLARED signature to introspection (upstream parity;
        # dispatch still accepts any call shape and resolves overloads).
        import inspect
        try:
            self.__signature__ = inspect.signature(fn)
        except (ValueError, TypeError):
            pass
        self._registry_name = f"__pyop__{self.__qualname__}_{id(self):x}"
        self._delegate = _OperatorFunction(self._registry_name)
        self._overloads = []   # (impl, wiring signature) - dispatch_ reads these

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        return extract_signature(self.fn, WiringNodeType.OPERATOR)

    @property
    def overloads(self):
        """Decorated implementations registered on this operator."""
        return tuple(implementation for implementation, _ in self._overloads)

    def __call__(self, *args, **kwargs):
        deprecated = getattr(self, "_deprecated", False)
        if deprecated:
            import warnings
            message = deprecated if isinstance(deprecated, str) else \
                f"operator '{self.__name__}' is deprecated"
            warnings.warn(message, DeprecationWarning, stacklevel=2)
        return self._delegate(*args, **kwargs)

    def __getitem__(self, item):
        if not isinstance(item, (slice, tuple)):
            target = None
            if len(self._type_variables) == 1:
                target = self._type_variables[0]
            elif self._default_type_var is not None:
                target = next(
                    (
                        variable for variable in self._type_variables
                        if _type_var_name(variable) == self._default_type_var
                    ),
                    None,
                )
            elif len(self._type_variables) > 1:
                raise WiringError(
                    f"operator '{self.__name__}' cannot infer which type "
                    "variable to bind; use TYPEVAR: concrete"
                )
            if target is not None:
                item = slice(target, item)
        return self._delegate[item]

    def overload(self, implementation):
        """Add an existing decorated graph/node as an overload of this operator."""
        _register_overload(self, implementation)
        return self


def operator(fn=None, deprecated=False):
    """Declare an overloadable Python authoring contract.

    The decorated callable supplies the public signature; implementations are
    registered with ``overloads=contract`` or ``@contract.overload`` and are
    selected during wiring.

    :param fn: Signature callable to decorate. Omit it when configuring the
        decorator.
    :param deprecated: ``True`` or a message string to emit a
        :class:`DeprecationWarning` when the operator is wired.
    :return: An operator decorator or the declared operator.
    """
    if fn is None:
        if deprecated:
            def _decorate(inner):
                op = _Operator(inner)
                op._deprecated = deprecated
                return op
            return _decorate
        return _Operator
    op = _Operator(fn)
    if deprecated:
        op._deprecated = deprecated
    return op


def _overload_registry_name(target):
    """The C++ registry name an ``overloads=`` target dispatches under."""
    if isinstance(target, _Operator):
        return target._registry_name
    if isinstance(target, str):
        return target
    name = getattr(target, "__name__", None)
    if name is not None and name in _hgraph.operator_names():
        return name   # overloading a BUILT-IN operator family
    raise TypeError(f"overloads= target {target!r} is not an @operator or a registered operator")


def _run_requires(user_requires, bindings, scalar_values):
    """Evaluate a ``requires=lambda m[, <scalar names...>]`` predicate.
    Returns True to accept; False or an explanation string rejects."""
    scalar_values = {
        name: (value._python_callable
               if isinstance(value, _hgraph.WiredFn) and value._python_callable is not None
               else value)
        for name, value in scalar_values.items()
    }
    return _invoke_resolution_callable(user_requires, bindings, scalar_values)


def _requires_bridge(user_requires):
    """Bridge hgraph's ``requires=`` onto the C++ requires_predicate.
    Exceptions and non-True results reject the candidate."""
    if user_requires is None:
        return None

    def _check(scope, scalars):
        try:
            return _run_requires(user_requires, scope.bindings, dict(scalars)) is True
        except Exception:
            return False

    return _check


def _resolvers_bridge(user_resolvers, type_carriers=(), output_pattern=None):
    """Run Python decorator resolvers during C++ overload selection.

    Output-only type variables must be resolved before the registry can select
    a candidate, so waiting for the wire trampoline is too late.
    """
    if not user_resolvers and not type_carriers:
        return None

    def _resolve(scope, scalars):
        scalar_values = dict(scalars)

        def apply_type_carriers(required):
            for parameter_name, placeholder, type_argument in type_carriers:
                supplied = _binding_for_type_value(
                    scalar_values.get(parameter_name))
                if supplied is not None:
                    _bind_native_resolution(scope, placeholder, supplied)
                binding = supplied or _resolution_binding(scope, placeholder)
                if binding is None and _type_var_name(placeholder) == "OUT" \
                        and output_pattern is not None:
                    resolved_output = scope.resolve_ts(output_pattern)
                    if resolved_output is not None:
                        scope.bind_ts("OUT", resolved_output)
                        binding = ("ts", resolved_output)
                if binding is None:
                    if required:
                        raise WiringError(
                            f"type carrier '{parameter_name}' could not resolve "
                            f"{placeholder!r}")
                    continue
                if not _match_type_argument(scope, type_argument, binding):
                    raise WiringError(
                        f"type carrier '{parameter_name}' resolved "
                        f"{placeholder!r} to a type that does not match "
                        f"{type_argument!r}")

        apply_type_carriers(required=False)
        _apply_resolvers(scope, user_resolvers, scalar_values)
        apply_type_carriers(required=True)
        return scope

    return _resolve


def _overload_wire_trampoline(impl):
    """The C++ wire closure calls this with the borrowed Wiring and the
    NORMALISED call (ports/scalars in declared order, defaults
    materialised); it re-enters the python wiring function."""

    signature = (
        getattr(impl, "_wiring_signature", None)
        or inspect.signature(impl.fn, eval_str=True)
    )
    call_parameters = [
        parameter
        for parameter in signature.parameters.values()
        if not _is_hidden_node_parameter(parameter)
        and parameter.kind is not inspect.Parameter.VAR_KEYWORD
    ]
    has_variadic = any(
        parameter.kind is inspect.Parameter.VAR_POSITIONAL
        for parameter in call_parameters
    )
    has_keyword_collector = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    accepted_keywords = set(signature.parameters)

    def _wire(borrowed_wiring, args, kwargs, resolution_scope):
        _wiring_stack.append(borrowed_wiring)
        try:
            wrap = lambda a: WiringPort(a) if isinstance(a, _hgraph.Port) else a
            values = [wrap(value) for value in args]
            from .._types import AUTO_RESOLVE, _TypeVarSentinel, _type_var_name
            import typing

            for index, (parameter, value) in enumerate(zip(call_parameters, values)):
                if typing.get_origin(parameter.annotation) is not type:
                    continue
                concrete = _binding_for_type_value(value)
                if (concrete is not None
                        and isinstance(parameter.default, (_TypeVarSentinel, typing.TypeVar))):
                    values[index] = _python_value_for_binding(
                        parameter.default, concrete)
                    continue
                if isinstance(value, (_TypeVarSentinel, typing.TypeVar)):
                    binding = _resolution_binding(resolution_scope, value)
                    if binding is not None:
                        values[index] = _python_value_for_binding(value, binding)
                    continue
                if value is not AUTO_RESOLVE:
                    continue
                type_arguments = typing.get_args(parameter.annotation)
                if not type_arguments:
                    continue
                variable = type_arguments[0]
                binding = _resolution_binding(resolution_scope, variable)
                if binding is not None:
                    values[index] = _python_value_for_binding(variable, binding)
            call_kwargs = {
                key: wrap(value) for key, value in kwargs.items()
                if has_keyword_collector or key in accepted_keywords
            }
            if has_variadic:
                call_args = values
            else:
                call_args = []
                for parameter, value in zip(call_parameters, values):
                    if parameter.kind is inspect.Parameter.KEYWORD_ONLY:
                        call_kwargs[parameter.name] = value
                    else:
                        call_args.append(value)
            callable_impl = impl
            if hasattr(impl, "_with_resolution"):
                callable_impl = impl._with_resolution(resolution_scope.bindings)
            out = callable_impl(*call_args, **call_kwargs)
            if out is None:
                return None
            if not isinstance(out, WiringPort):
                out = wire("const", out)
            raw = _unwrap(out)
            if raw.is_structural:
                if _hgraph.structural_has_ref_children(raw):
                    raw = _hgraph.ref_port(borrowed_wiring, raw)
                else:
                    raw = _unwrap(wire("__materialize", out))
            elif raw.has_path:
                raw = _unwrap(wire("__materialize", out))
            return raw
        finally:
            _wiring_stack.pop()

    return _wire


def _register_overload(target, impl, requires=None):
    """Register a python node/graph as an operator overload candidate: the
    parameter/output PATTERNS come from its annotations through the same
    bridged constructors the rest of the DSL uses."""
    from .._types import _pattern_of, _scalar_pattern

    name = _overload_registry_name(target)
    if isinstance(target, _Operator):
        target._overloads.append(
            (impl, getattr(impl, "_wiring_signature", None) or inspect.signature(impl.fn, eval_str=True)))
    fn = impl.fn
    # the ORIGINAL wiring signature: star-group nodes rewrite fn's code
    # object to keyword-only params (upstream parity), so fn's live
    # signature no longer shows *args/**kwargs.
    sig = (getattr(impl, "_wiring_signature", None)
           or _wiring_signature_of(fn)[0])
    param_options, variadic, has_kwargs = [], False, False
    type_carriers = []
    kwargs_pattern = None
    positional = None
    for parameter in sig.parameters.values():
        annotation = parameter.annotation
        if _is_hidden_node_parameter(parameter):
            continue
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            has_kwargs = True
            # Keep the pack annotation (issue #224): a ts-typed **kwargs
            # (e.g. TSB[TS_SCHEMA]) becomes the candidate's kwargs pattern,
            # matched at dispatch against the synthesized un-named TSB of
            # the supplied keywords — which is what binds the schema var.
            if annotation is not inspect.Parameter.empty:
                try:
                    kwargs_pattern = _pattern_of(annotation)
                except Exception:
                    kwargs_pattern = None
            continue
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY and positional is None:
            positional = len(param_options)
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            variadic = True
            if positional is None:
                positional = len(param_options)
            # The C++ variadic convention matches the declared pattern
            # PER TAIL ARGUMENT (in a throwaway scope), while the python
            # annotation describes the PACK (TSL[E, SIZE] / TSB[SCHEMA]).
            # Match each tail arg as an unconstrained ts for now (element
            # strictness deferred with the pack-shape work).
            param_options.append(((parameter.name, _hgraph.type_pattern_var(f"__{parameter.name}__")),))
            continue
        import types
        import typing

        union_members = (
            typing.get_args(annotation)
            if typing.get_origin(annotation) in (typing.Union, types.UnionType)
            else ()
        )
        if union_members:
            try:
                patterns = tuple(_pattern_of(member) for member in union_members)
            except TypeError as error:
                raise TypeError(
                    f"operator overload union for '{parameter.name}' must contain only "
                    "time-series annotations"
                ) from error
        else:
            try:
                patterns = (_pattern_of(annotation),)
            except TypeError:
                # ``type[T]`` is a wiring-time type carrier, not a value of
                # T. Its referenced type is resolved from the surrounding
                # input/output patterns and materialised in the Python wire
                # trampoline; do not bind T to the carrier implementation.
                if typing.get_origin(annotation) is type:
                    patterns = (_hgraph.scalar_pattern_var(
                        f"__type_arg__{id(impl):x}__{parameter.name}"
                    ),)
                elif annotation in (inspect.Parameter.empty, object):
                    patterns = (_hgraph.scalar_pattern_var(
                        f"__any_scalar__{id(impl):x}__{parameter.name}"
                    ),)
                else:
                    patterns = (_scalar_pattern(annotation),)
        annotation_args = typing.get_args(annotation)
        if (typing.get_origin(annotation) is type and annotation_args
                and isinstance(parameter.default, (_TypeVarSentinel, typing.TypeVar))):
            type_carriers.append(
                (parameter.name, parameter.default, annotation_args[0]))
        if parameter.default is inspect.Parameter.empty:
            param_options.append(tuple((parameter.name, pattern) for pattern in patterns))
        else:
            param_options.append(
                tuple((parameter.name, pattern, parameter.default) for pattern in patterns)
            )
    if name == "mesh_" and "__name__" not in sig.parameters:
        # ``mesh_`` supplies its scope name as a private native control. It is
        # not part of a user overload's callable signature, but it must not
        # prevent that overload from participating in registry selection.
        if positional is None:
            positional = len(param_options)
        param_options.append((("__name__", _scalar_pattern(str), ""),))
    output = None
    out_tp = sig.return_annotation
    if out_tp not in (inspect.Signature.empty, None):
        output = _pattern_of(out_tp)
    from itertools import product

    wire_fn = _overload_wire_trampoline(impl)
    resolver_fn = _resolvers_bridge(
        getattr(impl, "_resolvers", None), type_carriers, output)
    requires_fn = _requires_bridge(requires)
    for params in product(*param_options):
        _hgraph.register_python_overload(
            name, list(params), output, wire_fn, resolver_fn, requires_fn,
            variadic, has_kwargs, positional, kwargs_pattern)


# ---------------------------------------------------------------------------
# dispatch: runtime type dispatch = a small KEY UTILITY feeding switch_
# (Howard's ruling). ``type_(arg)`` reads each dispatch argument's dynamic
# python type per tick; the key node maps it (isinstance/MRO specificity)
# onto the enumerated overload keys; switch_ instantiates the winner.
# Python-class scalars keep their dynamic object type. CompoundScalar values
# expose the active leaf of their graph-scoped closed Bundle union, so both
# representations provide the same concrete class key to ``type_``.
# ---------------------------------------------------------------------------

def _dispatch_specificity(cls):
    import typing

    return len((typing.get_origin(cls) or cls).__mro__)


def _dispatch_key_node():
    global _DISPATCH_KEY_NODE
    if _DISPATCH_KEY_NODE is None:
        from .._types import TS
        from ._node import compute_node

        @compute_node
        def _adjust_dispatch_key(key: TS[object], available_keys: tuple) -> TS[object]:
            value = key.value
            if value in available_keys:
                return value
            candidates = [(a_key, _dispatch_specificity(a_key))
                          for a_key in available_keys if issubclass(value, a_key)]
            if not candidates:
                raise RuntimeError(f"No suitable overload found for {value}")
            candidates.sort(key=lambda entry: entry[1], reverse=True)
            if len(candidates) > 1 and candidates[0][1] == candidates[1][1]:
                raise RuntimeError(f"Ambiguous dispatch for {value}")
            return candidates[0][0]

        _DISPATCH_KEY_NODE = _adjust_dispatch_key
    return _DISPATCH_KEY_NODE


def _dispatch_keys_node():
    global _DISPATCH_KEYS_NODE
    if _DISPATCH_KEYS_NODE is None:
        from .._types import TS
        from ._node import compute_node

        import typing

        @compute_node
        def _adjust_dispatch_keys(key: TS[typing.Tuple[object, ...]],
                                  available_keys: tuple) -> TS[typing.Tuple[object, ...]]:
            value = tuple(key.value)
            if value in available_keys:
                return value
            candidates = []
            for a_keys in available_keys:
                if len(a_keys) == len(value) and all(
                        issubclass(k, a_key) for a_key, k in zip(a_keys, value)):
                    candidates.append((a_keys, sum(_dispatch_specificity(a) for a in a_keys)))
            if not candidates:
                raise RuntimeError(f"No suitable overload found for {value}")
            candidates.sort(key=lambda entry: entry[1], reverse=True)
            if len(candidates) > 1 and candidates[0][1] == candidates[1][1]:
                raise RuntimeError(f"Ambiguous dispatch for {value}")
            return candidates[0][0]

        _DISPATCH_KEYS_NODE = _adjust_dispatch_keys
    return _DISPATCH_KEYS_NODE


_DISPATCH_KEY_NODE = None
_DISPATCH_KEYS_NODE = None


def _declared_dispatch_class(annotation):
    """The DECLARED python class of a dispatchABLE ``TS[cls]`` annotation:
    a CompoundScalar or an object-kind class scalar (structural - the
    expression carries the class AND the value schema decides the kind;
    atomic scalars like TS[int] are not dispatch subjects)."""
    cls = getattr(annotation, "_py_class", None)
    if getattr(annotation, "_cs_class", None) is not None:
        return getattr(annotation, "_structured_schema", None) or cls
    if cls is None:
        return None
    handle = getattr(annotation, "handle", None)
    if handle is not None and handle.is_ts and _is_object_vt(_hgraph.ts_value_vt(handle)):
        return cls
    return None


def _declared_dispatch_classes(annotation):
    """All runtime-dispatch classes represented by an annotation.

    A normal ``TS[Class]`` contributes one class. A time-series union on an
    overload contributes one switch key per member; it is expanded here and
    during C++ overload registration so the two paths cannot disagree.
    """
    import types
    import typing

    members = (
        typing.get_args(annotation)
        if typing.get_origin(annotation) in (typing.Union, types.UnionType)
        else (annotation,)
    )
    classes = tuple(_declared_dispatch_class(member) for member in members)
    return classes if classes and all(cls is not None for cls in classes) else ()


def _dispatch_branch(op, impl, root_signature, branch_signature, scalar_arguments,
                     dispatch_types, expected_output=None):
    """Adapt base-typed switch inputs and re-enter registry dispatch.

    The runtime type key only chooses the closed switch branch. Once its
    inputs have their selected concrete schemas, the ordinary C++ operator
    registry owns overload ranking, requirements, and output typing for
    CompoundScalar. Object-kind class annotations share one ``TS[object]``
    schema, so those branches invoke the already-selected Python overload.
    """
    from .._types import TS
    from ._graph import _GraphFn

    registry_dispatch = all(
        getattr(root_signature.parameters[name].annotation, "_cs_class", None) is not None
        for name in dispatch_types
    )

    def invoke(*args, **kwargs):
        parameter_names = tuple(branch_signature.parameters)
        if len(args) > len(parameter_names):
            raise TypeError(f"{invoke.__name__}: too many dispatch branch inputs")
        arguments = dict(scalar_arguments)
        arguments.update(zip(parameter_names, args))
        arguments.update(kwargs)
        bound = inspect.BoundArguments(root_signature, arguments)
        for name, cls in dispatch_types.items():
            target_type = TS[cls]
            value = bound.arguments[name]
            source = _unwrap(value).ts_type
            if source != target_type.handle:
                bound.arguments[name] = wire("downcast_", value, output_type=target_type)
        callable_ = (
            op._delegate[expected_output]
            if registry_dispatch and expected_output is not None
            else op._delegate if registry_dispatch
            else impl
        )
        return callable_(*bound.args, **bound.kwargs)

    suffix = "_".join(
        getattr(cls, "__name__", repr(cls)).replace(".", "_")
        for cls in dispatch_types.values()
    )
    invoke.__name__ = f"__dispatch_{op.__name__}_{suffix}"
    invoke.__signature__ = branch_signature
    return _GraphFn(invoke)


def dispatch_(overloaded, *args, __on__=None, __output_type=None, **kwargs):
    """Dispatch to an operator implementation by current runtime value types.

    This is dynamic dispatch for ``TS[BaseClass]`` values: wiring-time overload
    resolution first defines the closed set of implementations, then a native
    selector routes each concrete runtime leaf type through ``switch_``. Branch
    inputs are checked-downcast to their declared implementation types.

    :param overloaded: ``@operator`` or ``@dispatch`` target whose overloads
        form the runtime cases.
    :param args: Positional arguments supplied to the selected implementation.
    :param __on__: Optional iterable of parameter names that participate in
        runtime dispatch. By default every class-typed TS parameter participates.
    :param __output_type: Explicit output type when the operator annotation
        cannot determine it.
    :param kwargs: Named arguments supplied to the selected implementation.
    :return: Output from the implementation matching the current concrete types.

    Example::

        result = dispatch_(price_operator, instrument, market)
    """
    from ._compose import switch_
    from ._graph import _as_wired

    op = overloaded
    if not isinstance(op, _Operator):
        raise WiringError(f"dispatch_ needs an @operator/@dispatch target, got {op!r}")
    if not op._overloads:
        raise WiringError(f"{op.__name__} has no overloads to dispatch to")
    sig = inspect.signature(op.fn, eval_str=True)
    if __output_type is None and isinstance(sig.return_annotation, _TsExpr):
        __output_type = sig.return_annotation
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    call_kwargs = dict(bound.arguments)
    for name, value in tuple(call_kwargs.items()):
        annotation = sig.parameters[name].annotation
        if isinstance(annotation, _TsExpr) and not isinstance(value, WiringPort):
            call_kwargs[name] = (
                wire("nothing", output_type=annotation)
                if value is None
                else wire("const", value, output_type=annotation)
            )
    port_kwargs = {
        name: value for name, value in call_kwargs.items()
        if isinstance(value, WiringPort)
    }
    scalar_arguments = {
        name: value for name, value in call_kwargs.items()
        if not isinstance(value, WiringPort)
    }
    branch_signature = sig.replace(
        parameters=[
            parameter for name, parameter in sig.parameters.items()
            if name in port_kwargs
        ]
    )

    dispatch_params = {}
    for name, param in sig.parameters.items():
        if __on__ is not None and name not in __on__:
            continue
        cls = _declared_dispatch_class(param.annotation)
        if cls is not None:
            dispatch_params[name] = cls
    if __on__ is not None and set(__on__) != set(dispatch_params):
        missing = set(__on__) - set(dispatch_params)
        raise WiringError(f"cannot dispatch on non-class parameter(s): {sorted(missing)}")
    if not dispatch_params:
        raise WiringError(f"{op.__name__} has no dispatchable (TS[class]) parameters")

    dispatch_map = {}
    for impl, impl_sig in op._overloads:
        class_options = []
        for name in dispatch_params:
            impl_param = impl_sig.parameters.get(name)
            classes = (
                _declared_dispatch_classes(impl_param.annotation)
                if impl_param is not None
                else ()
            )
            if not classes:
                raise WiringError(
                    f"{impl.__name__}: dispatch parameter '{name}' must be a "
                    "TS[class] or a union of TS[class] annotations"
                )
            import typing
            base = typing.get_origin(dispatch_params[name]) or dispatch_params[name]
            if not all(issubclass(typing.get_origin(cls) or cls, base)
                       for cls in classes):
                raise WiringError(
                    f"{impl.__name__}: dispatch parameter '{name}' is outside "
                    f"{getattr(dispatch_params[name], '__name__', dispatch_params[name])}"
                )
            class_options.append(classes)
        from itertools import product

        for classes in product(*class_options):
            key = tuple(classes) if len(classes) > 1 else classes[0]
            dispatch_map[key] = _dispatch_branch(
                op, impl, sig, branch_signature, scalar_arguments,
                dict(zip(dispatch_params, classes)), __output_type,
            )
    if not dispatch_map:
        raise WiringError(f"no dispatchable overloads found for {op.__name__}")

    names = list(dispatch_params)
    compound_dispatch = all(
        getattr(sig.parameters[name].annotation, "_cs_class", None) is not None
        for name in names
    )
    if compound_dispatch:
        from .._types import _value_type

        port_names = list(port_kwargs)
        entries = []
        for key, branch in dispatch_map.items():
            classes = key if isinstance(key, tuple) else (key,)
            entries.append((tuple(_value_type(cls) for cls in classes), _as_wired(branch)))
        erased = _hgraph.dispatch_cases(
            entries,
            [port_names.index(name) for name in names],
            declared_types=[_value_type(dispatch_params[name]) for name in names],
        )
        return wire("dispatch_", erased, output_type=__output_type, **port_kwargs)

    # Python-object class dispatch has no native Bundle schema. Keep its
    # Python type key utility while CompoundScalar dispatch uses the native
    # closed-union selector above.
    if len(names) == 1:
        key = _dispatch_key_node()(wire("type_", call_kwargs[names[0]]),
                                   tuple(dispatch_map.keys()))
    else:
        from .._types import TSL
        from ..nodes import flatten_tsl_values

        types_tsl = TSL.from_ts(*(wire("type_", call_kwargs[name]) for name in names))
        key = _dispatch_keys_node()(flatten_tsl_values(types_tsl, all_valid=True),
                                    tuple(dispatch_map.keys()))
    return switch_(key, dispatch_map, **port_kwargs)


class _Dispatch(_Operator):
    """An operator whose call dispatches on runtime types.

    ``@dispatch`` on a function registers that body as the generic fallback.
    ``dispatch(operator(signature))`` creates an empty dispatch set to which
    existing implementations can be added with :meth:`overload`.
    """

    def __init__(self, fn, on=None):
        from ._graph import _GraphFn

        declaration_only = isinstance(fn, _Operator)
        super().__init__(fn.fn if declaration_only else fn)
        self._dispatch_on = ((on,) if isinstance(on, str) else tuple(on)) if on else None
        self._dispatch_fallback = None if declaration_only else _GraphFn(fn)
        if self._dispatch_fallback is not None:
            _register_overload(self, self._dispatch_fallback)

    def __call__(self, *args, **kwargs):
        return dispatch_(self, *args, __on__=self._dispatch_on, **kwargs)

    def __getitem__(self, item):
        from ._graph import _GraphFn
        from ._node import _is_time_series_annotation
        from .._types import AUTO_RESOLVE, _TsExpr

        if not isinstance(item, _TsExpr):
            return super().__getitem__(item)

        signature = inspect.signature(self.fn, eval_str=True)
        ts_parameters = [
            parameter for parameter in signature.parameters.values()
            if _is_time_series_annotation(parameter.annotation)
        ]
        scalar_values = {}
        import typing

        for parameter in signature.parameters.values():
            if parameter in ts_parameters:
                continue
            if (parameter.default is AUTO_RESOLVE
                    and typing.get_origin(parameter.annotation) is type):
                scalar_values[parameter.name] = item
            elif parameter.default is not inspect.Parameter.empty:
                scalar_values[parameter.name] = parameter.default
            else:
                raise TypeError(
                    f"specialized dispatch '{self.__name__}' cannot erase required "
                    f"scalar parameter '{parameter.name}'")

        exposed = signature.replace(
            parameters=ts_parameters, return_annotation=item)

        def invoke(*args, **kwargs):
            bound = exposed.bind(*args, **kwargs)
            arguments = dict(scalar_values)
            arguments.update(bound.arguments)
            return dispatch_(
                self, __on__=self._dispatch_on, __output_type=item, **arguments)

        invoke.__name__ = f"__specialized_dispatch_{self.__name__}"
        invoke.__signature__ = exposed
        return _GraphFn(invoke)


def dispatch(fn=None, *, on=None):
    """Declare a graph that selects an implementation by a time-series value.

    Dispatch creates and manages the keyed nested graphs in the native runtime.
    The selected value may be one time series or a tuple of time-series inputs.

    :param fn: Dispatched callable to decorate. Omit it when configuring the
        decorator.
    :param on: Name of the dispatch input, or names for multiple dispatch.
        When omitted, the first time-series parameter is used.
    :return: A dispatch decorator or the decorated dispatch graph.
    """
    if fn is None:
        return lambda f: dispatch(f, on=on)
    return _Dispatch(fn, on=on)
