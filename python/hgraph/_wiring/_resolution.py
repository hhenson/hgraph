"""Shared wiring-time type-resolution behavior.

All Python authoring surfaces delegate resolver scheduling and result binding
to this module. The C++ ``ResolutionMap`` remains the source of truth for
whether a variable is already resolved and for consistent re-binding checks.
"""
import inspect
import typing
from collections.abc import Mapping

import _hgraph

from .._types import (_TsExpr, _TypeVarSentinel, _pattern_of,
                      _scalar_pattern, _type_var_name, _value_type)


class _BindingsMap(Mapping):
    """Resolver/requires view keyed by either a type variable or its name."""

    __slots__ = ("_bindings",)

    def __init__(self, bindings):
        self._bindings = bindings

    def __getitem__(self, key):
        return self._bindings[_type_var_name(key)]

    def __contains__(self, key):
        return _type_var_name(key) in self._bindings

    def get(self, key, default=None):
        return self._bindings.get(_type_var_name(key), default)

    def __iter__(self):
        return iter(self._bindings)

    def __len__(self):
        return len(self._bindings)


def _invoke_resolution_callable(fn, bindings, scalar_values):
    """Invoke a wiring callable as ``fn(mapping, **declared_scalars)``."""
    # Resolver annotations do not participate in argument selection. Avoid
    # evaluating postponed annotations here: a valid resolver may annotate
    # its parameters with function-local types that are absent from globals.
    parameters = list(inspect.signature(fn).parameters)
    scalars = dict(scalar_values or {})
    return fn(
        _BindingsMap(bindings),
        **{name: scalars.get(name) for name in parameters[1:]},
    )


def _bind_resolution(scope, name, resolved):
    """Bind one dynamically authored result into the native resolution map."""
    if isinstance(resolved, _TsExpr):
        scope.bind_ts(name, resolved.handle)
    elif isinstance(resolved, _hgraph.TsType):
        scope.bind_ts(name, resolved)
    elif isinstance(resolved, int) and not isinstance(resolved, bool):
        scope.bind_size(name, resolved)
    else:
        scope.bind_scalar(name, _value_type(resolved))


def _resolution_binding(scope, variable):
    """Return the native kind/value currently bound to ``variable``."""
    name = _type_var_name(variable)
    if (value := scope.find_scalar(name)) is not None:
        return "scalar", value
    if (value := scope.find_ts(name)) is not None:
        return "ts", value
    if (value := scope.find_size(name)) is not None:
        return "size", value
    return None


def _python_value_for_binding(variable, binding):
    """Project a native resolution binding into a Python type carrier."""
    kind, value = binding
    name = _type_var_name(variable)
    if kind == "scalar":
        return _hgraph.python_type_for_value(value)
    if kind == "ts":
        return _TsExpr(value, f"resolved[{name}]")
    if kind == "size":
        from ._graph import _ResolvedSize

        return _ResolvedSize(value)
    raise TypeError(f"unknown resolution binding kind {kind!r}")


def _binding_for_type_value(value):
    """Return a native binding for a concrete Python ``type[...]`` value."""
    if isinstance(value, _TsExpr):
        return "ts", value.handle
    if isinstance(value, _hgraph.TsType):
        return "ts", value
    if isinstance(value, type):
        return "scalar", _value_type(value)
    size = getattr(value, "SIZE", None)
    if isinstance(size, int) and not isinstance(size, bool):
        return "size", size
    return None


def _bind_native_resolution(scope, variable, binding):
    """Bind an already-lowered native carrier value to ``variable``."""
    kind, value = binding
    name = _type_var_name(variable)
    if kind == "ts":
        scope.bind_ts(name, value)
    elif kind == "scalar":
        scope.bind_scalar(name, value)
    elif kind == "size":
        scope.bind_size(name, value)
    else:
        raise TypeError(f"unknown resolution binding kind {kind!r}")


def _match_type_argument(scope, type_argument, binding):
    """Match the inside of ``type[...]`` against a resolved carrier value.

    The match is structural and updates ``scope``. For example, matching
    ``TS[SCALAR]`` against ``TS[HttpResponse]`` binds ``SCALAR``; matching it
    against a TSD fails at the outer time-series kind.
    """
    kind, value = binding
    if kind == "ts":
        try:
            return scope.match(_pattern_of(type_argument), value)
        except (RuntimeError, ValueError, TypeError):
            return False
    if kind == "scalar":
        try:
            pattern = _hgraph.type_pattern_ts(_scalar_pattern(type_argument))
            return scope.match(pattern, _hgraph.ts(value))
        except (RuntimeError, ValueError, TypeError):
            return False
    if kind == "size":
        if isinstance(type_argument, (_TypeVarSentinel, typing.TypeVar)):
            try:
                scope.bind_size(_type_var_name(type_argument), value)
                return True
            except (RuntimeError, ValueError, TypeError):
                return False
        return getattr(type_argument, "SIZE", type_argument) == value
    return False


def _apply_resolvers(scope, resolvers, scalar_values=None):
    """Apply only resolvers whose target is not already resolved.

    This is the common hgraph compatibility rule for nodes, graphs, operator
    overloads, sources, services, and adaptors. A concrete Python type in a
    resolver table is a literal resolution (not a callable resolver), matching
    service specialization behavior.
    """
    for sentinel, resolver in (resolvers or {}).items():
        name = _type_var_name(sentinel)
        if scope.is_resolved(name):
            continue
        resolved = resolver if isinstance(resolver, type) else _invoke_resolution_callable(
            resolver, scope.bindings, scalar_values
        )
        _bind_resolution(scope, name, resolved)
    return scope
