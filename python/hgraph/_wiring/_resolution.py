"""Shared wiring-time type-resolution behavior.

All Python authoring surfaces delegate resolver scheduling and result binding
to this module. The C++ ``ResolutionMap`` remains the source of truth for
whether a variable is already resolved and for consistent re-binding checks.
"""
import inspect
import typing
from collections.abc import Mapping
from functools import lru_cache

import _hgraph

from .._types import (_TsExpr, _TypeVarSentinel, _pattern_of,
                      _scalar_pattern,
                      _type_var_name, _value_type)


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


@lru_cache(maxsize=None)
def _cached_resolution_parameter_names(fn):
    return tuple(inspect.signature(fn).parameters)


def _resolution_parameter_names(fn):
    try:
        return _cached_resolution_parameter_names(fn)
    except TypeError:
        # Callable instances may deliberately be unhashable.
        return tuple(inspect.signature(fn).parameters)


def _invoke_resolution_callable(fn, bindings, scalar_values):
    """Invoke a wiring callable as ``fn(mapping, **declared_scalars)``."""
    # Resolver annotations do not participate in argument selection. Avoid
    # evaluating postponed annotations here: a valid resolver may annotate
    # its parameters with function-local types that are absent from globals.
    parameters = _resolution_parameter_names(fn)
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


def _carried_pattern(type_argument):
    """The carried pattern of ``type[X]`` as the bridge pattern the one
    matcher takes (RFC 0033): a ``TypePattern`` for a time-series ``X``, a
    ``SizePattern`` for a size, a ``ScalarPattern`` otherwise. A bare
    variable lowers to a scalar variable; the bridge follows the map's
    binding kind when that variable is bound as a time series or a size
    (``type[OUT]``, ``type[SIZE]``)."""
    from .._types import (TSB, TimeSeriesSchema, _GenericTsExpr, _size_pattern,
                          _type_var_is_scalar)

    x = type_argument
    if isinstance(x, (_TsExpr, _GenericTsExpr)):
        return _pattern_of(x)
    if isinstance(x, (_TypeVarSentinel, typing.TypeVar)):
        return _scalar_pattern(x) if _type_var_is_scalar(x) else _pattern_of(x)
    if isinstance(x, bool):
        raise TypeError(f"{x!r} is not a type argument")
    if isinstance(x, int):
        return _size_pattern(x)
    size = getattr(x, "SIZE", None)
    if isinstance(size, int) and not isinstance(size, bool):
        return _size_pattern(size)
    if isinstance(x, type) and issubclass(x, TimeSeriesSchema):
        return _pattern_of(TSB[x])
    return _scalar_pattern(x)


def _carrier_value(value):
    """A Python type argument as the value the scope matches: a ``TsType``,
    a ``ValueType`` or a size; ``None`` when ``value`` is not a type."""
    from .._types import TSB, TimeSeriesSchema

    if isinstance(value, _TsExpr):
        return value.handle
    if isinstance(value, (_hgraph.TsType, _hgraph.ValueType)):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    size = getattr(value, "SIZE", None)
    if isinstance(size, int) and not isinstance(size, bool):
        return size
    if isinstance(value, type) and issubclass(value, TimeSeriesSchema):
        return TSB[value].handle
    try:
        return _value_type(value)
    except Exception:
        return None


def _carrier_to_python(value):
    """What a Python body receives for a materialised type argument: a
    ``TS[...]`` expression, the annotation of a scalar schema, or a
    ``Size``-like object with ``.SIZE``."""
    if isinstance(value, _hgraph.TsType):
        return _TsExpr(value, repr(value))
    if isinstance(value, int) and not isinstance(value, bool):
        from ._graph import _ResolvedSize

        return _ResolvedSize(value)
    return value


def _match_type_carrier(scope, type_argument, value):
    """Match a supplied type argument against ``type[type_argument]``,
    binding into ``scope`` (the registry's ``type_carrier_match``)."""
    carrier = _carrier_value(value)
    if carrier is None:
        return False
    try:
        return scope.match_carrier(_carried_pattern(type_argument), carrier)
    except (RuntimeError, ValueError, TypeError):
        return False


def _materialise_type_carrier(scope, type_argument):
    """Resolve a deferred type argument's pattern in ``scope`` and hand back
    the Python value a body receives; ``None`` while a variable it needs is
    unbound."""
    try:
        resolved = scope.materialise(_carried_pattern(type_argument))
    except (RuntimeError, ValueError, TypeError):
        return None
    return None if resolved is None else _carrier_to_python(resolved)


def _signature_type_variables(annotations):
    """The type variables of a signature, in order of first appearance across
    ``annotations`` (parameters first, then the return annotation); one
    collector for every decorator kind (RFC 0033)."""
    from .._types import _type_variables_of

    found = {}
    for annotation in annotations:
        for variable in _type_variables_of(annotation):
            found.setdefault(_type_var_name(variable), variable)
    return tuple(found.values())


def _pin_type_arguments(variables, items, *, default_var, owner):
    """The one subscript rule (RFC 0033): ``fn[VAR: X]`` pins the named
    variable (declared or not); bare items fill the ``DEFAULT[...]`` variable first, then the
    remaining variables in order of first appearance. A single bare item with
    no ``DEFAULT`` and more than one remaining variable, or more bare items
    than remaining variables, is a ``WiringError``. Returns ``{name: value}``
    in the order the variables were pinned. ``default_var`` is a name: the
    marked variable may appear only in a default value (``= DEFAULT[OUT]``
    on a ``type[...]`` parameter), so it need not be one of ``variables``."""
    from ._core import WiringError

    ordered = [_type_var_name(variable) for variable in variables]
    pins, bare = {}, []
    for entry in (items if isinstance(items, tuple) else (items,)):
        if isinstance(entry, slice):
            if entry.step is not None or not isinstance(
                    entry.start, (_TypeVarSentinel, typing.TypeVar)):
                raise WiringError(
                    f"{owner}: subscript entries are TYPEVAR: type, got {entry!r}")
            # A named entry for a variable the signature does not declare
            # seeds the scope and binds nothing the signature uses (the 0.5
            # reference accepts ``extract_tsd[TIME_SERIES_TYPE: TS[int]]``).
            pins[_type_var_name(entry.start)] = entry.stop
        else:
            bare.append(entry)
    if not bare:
        return pins
    remaining = [name for name in ordered if name not in pins]
    default = default_var if default_var is not None and default_var not in pins else None
    targets = ([default] if default is not None else []) + [
        name for name in remaining if name != default]
    rendered = ", ".join(remaining) or "none"
    if len(bare) == 1 and default is None and len(remaining) != 1:
        raise WiringError(
            f"{owner}: can not figure out which type parameter to assign "
            f"{bare[0]!r} to (unbound type variables in this signature: {rendered}). "
            f"Name it explicitly, as in {owner}[TYPE_VAR: {bare[0]!r}], "
            f"or mark one with DEFAULT[...].")
    if len(bare) > len(targets):
        raise WiringError(
            f"{owner}: {len(bare)} type arguments for {len(targets)} unbound type "
            f"variable(s) ({rendered})")
    pins.update(zip(targets, bare))
    return pins


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
