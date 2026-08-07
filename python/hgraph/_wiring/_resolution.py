"""Shared wiring-time type-resolution behavior.

All Python authoring surfaces delegate resolver scheduling and result binding
to this module. The C++ ``ResolutionMap`` remains the source of truth for
whether a variable is already resolved and for consistent re-binding checks.
"""
import inspect
from collections.abc import Mapping

import _hgraph

from .._types import _TsExpr, _type_var_name, _value_type


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
    parameters = list(inspect.signature(fn, eval_str=True).parameters)
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
