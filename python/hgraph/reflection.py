"""Lightweight type-reflection helpers.

hg_cpp does not provide upstream's ``Hg*TypeMetaData`` reflection family (see
``docs/source/developer_guide/type_reflection.rst``). The load-bearing needs
already work on the shipped surface — type annotations are first-class
comparable values (``TS[int] == TS[int]``) and a wired node exposes
``signature.input_types`` / ``signature.output_type`` directly. This module
covers the remaining, lighter need: **structurally decomposing a time-series
type** into its parts, each returned as a plain comparable python/TS type.

It is a thin wrapper over primitives the C++ bridge already exposes
(``_hgraph`` ``TsType`` predicates + ``tsd_key_vt`` / ``tsd_element_ts`` /
``tsl_element_ts`` / ``ts_value_vt`` / ``ts_field_types`` / ``ref_target``);
there is no parallel metadata hierarchy. Every accessor returns something you
can compare directly (``value_type(TSD[str, TS[int]]) == TS[int]``).

Migration reference:

===============================================  ==================================
upstream ``Hg*TypeMetaData``                     ``hgraph.reflection``
===============================================  ==================================
``m.value_scalar_tp`` (TS / TSS payload)         ``scalar_type(t)``
``m.key_tp`` / ``m.value_tp`` (TSD)              ``key_type(t)`` / ``value_type(t)``
``m.element_type`` / ``m.size`` (TSL)            ``element_type(t)`` / ``size(t)``
``m.meta_data_schema`` (TSB / compound scalar)   ``fields(t)``
``m.py_type`` (resolved TS binding)              ``resolved_type(t)``
``m.dereference()`` / ``m.has_references``       ``dereference(t)`` / ``is_reference(t)``
``isinstance(m, HgTSDTypeMetaData)``             ``is_tsd(t)`` (and siblings)
===============================================  ==================================
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
from datetime import date, datetime, time, timedelta

import _hgraph as _m

from ._types import (
    _TSB_SCHEMA_CLASSES,
    _TS_SCALAR_TYPES,
    _VALUE_SCALAR_TYPES,
    _FrameType,
    _TsExpr,
    _is_python_object_class,
    _structured_specialized_field_types,
    _structured_python_field_types,
    _value_type,
)
from ._compat import CompoundScalar

__all__ = (
    "scalar_type",
    "resolved_type",
    "key_type",
    "value_type",
    "element_type",
    "size",
    "fields",
    "bundle_schema_type",
    "dereference",
    "is_reference",
    "is_ts",
    "is_tsd",
    "is_tsl",
    "is_tss",
    "is_bundle",
    "is_compound_scalar",
    "is_frame",
    "frame_schema",
    "operator_overloads",
)

# Atomic python scalar types, mapped from their value-type back to the python
# type. Built once, defensively (a type without a value-type is skipped).
_ATOMIC = (bool, int, float, str, bytes, date, datetime, time, timedelta)


def _atomic_reverse():
    rev = {}
    for pt in _ATOMIC:
        try:
            rev[_value_type(pt)] = pt
        except Exception:  # pragma: no cover - a scalar without a value-type
            pass
    return rev


_VT_TO_PY = _atomic_reverse()


def _handle(t):
    """The ``_hgraph.TsType`` for a time-series type expression."""
    output_type = getattr(t, "output_type", None)
    if output_type is not None:
        t = output_type
    if isinstance(t, _m.TsType):
        return t
    h = getattr(t, "handle", None)
    if h is None:
        raise TypeError(f"{t!r} is not a time-series type expression")
    return h


def resolved_type(t):
    """Return a resolved time-series binding as a public type expression.

    This is the implementation-neutral replacement for reading ``.py_type``
    from a resolution-map metadata object. Public annotations are returned
    unchanged; C++ resolution handles are wrapped as comparable annotations.
    """
    output_type = getattr(t, "output_type", None)
    if output_type is not None:
        t = output_type
    if isinstance(t, _TsExpr):
        return t
    if isinstance(t, _m.TsType):
        return _wrap(t)
    if isinstance(t, _m.ValueType):
        return _m.python_type_for_value(t)
    raise TypeError(f"{t!r} is not a resolved time-series type")


def _wrap(handle):
    """Wrap a bridge ``TsType`` back into a comparable type expression."""
    return _TsExpr(handle, repr(handle))


def _vt_to_py(vt):
    py = _VALUE_SCALAR_TYPES.get(vt, _VT_TO_PY.get(vt))
    if py is None:
        reflected = _m.python_type_for_value(vt)
        if isinstance(reflected, type):
            py = reflected
    if py is None:
        raise TypeError(
            f"scalar type is not a plain atomic python type (kind {_m.vt_kind(vt)}); "
            "decompose it further or read it from the schema"
        )
    return py


def scalar_type(t):
    """The payload scalar of a ``TS[S]`` or the element scalar of a ``TSS[S]``.

    ``scalar_type(TS[int]) == int``; ``scalar_type(TSS[str]) == str``. For a
    ``TS[CompoundScalar]`` the compound-scalar class itself is returned.
    """
    h = _handle(t)
    if h.is_tss:
        return _vt_to_py(_m.vt_element(_m.ts_value_vt(h)))
    if h.is_ts:
        declared = _TS_SCALAR_TYPES.get(h)
        if declared is not None:
            return declared
        structured_schema = getattr(t, "_structured_schema", None)
        if structured_schema is not None:
            return structured_schema
        cs = getattr(t, "_cs_class", None)
        if cs is not None:
            return cs
        return _vt_to_py(_m.ts_value_vt(h))
    raise TypeError(f"scalar_type expects a TS or TSS type, got {t!r}")


def key_type(t):
    """The key scalar of a ``TSD[K, V]``. ``key_type(TSD[str, TS[int]]) == str``."""
    h = _handle(t)
    if not h.is_tsd:
        raise TypeError(f"key_type expects a TSD type, got {t!r}")
    return _vt_to_py(_m.tsd_key_vt(h))


def value_type(t):
    """The value time-series of a ``TSD[K, V]``.

    ``value_type(TSD[str, TS[int]]) == TS[int]``. The value is returned as
    resolved: if the engine wraps it in ``REF[...]`` that wrapping is preserved
    (apply :func:`dereference` for the logical shape).
    """
    h = _handle(t)
    if not h.is_tsd:
        raise TypeError(f"value_type expects a TSD type, got {t!r}")
    return _wrap(_m.tsd_element_ts(h))


def element_type(t):
    """The element time-series of a fixed ``TSL[V, Size]``.

    ``element_type(TSL[TS[int], 3]) == TS[int]``.
    """
    h = _handle(t)
    if not h.is_tsl:
        raise TypeError(f"element_type expects a TSL type, got {t!r}")
    return _wrap(_m.tsl_element_ts(h))


def size(t):
    """The fixed size of a ``TSL[V, Size]``. ``size(TSL[TS[int], 3]) == 3``."""
    h = _handle(t)
    if not h.is_tsl:
        raise TypeError(f"size expects a TSL type, got {t!r}")
    return h.fixed_size


def fields(t):
    """Ordered ``{name: type}`` for a ``TSB`` schema or a compound scalar.

    For a ``TSB``/``TS[CompoundScalar]`` the values are the field time-series
    types (``{'a': TS[int]}``); for a compound-scalar *class* they are the field
    scalar types (``{'a': int}``).
    """
    if isinstance(t, Mapping):
        return {name: resolved_type(value) for name, value in t.items()}
    import typing

    origin = typing.get_origin(t)
    if _is_python_object_class(origin):
        return dict(_structured_specialized_field_types(t))
    # A structured scalar class passed directly: its scalar fields.
    if isinstance(t, type) and (
        dataclasses.is_dataclass(t) or _is_python_object_class(t)
    ):
        return dict(_structured_python_field_types(t))
    if isinstance(t, _m.ValueType):
        python_type = _m.python_type_for_value(t)
        if python_type is not t and isinstance(python_type, type):
            return fields(python_type)
        value_fields = tuple(t.fields)
        if value_fields:
            return {
                name: _m.python_type_for_value(field_type)
                for name, field_type in value_fields
            }
        raise TypeError(f"fields expects a bundle value type, got {t!r}")
    h = _handle(t)
    if h.is_tsb:
        return {name: _wrap(field) for name, field in _m.ts_field_types(h)}
    cs = getattr(t, "_cs_class", None)
    if cs is None and h.is_ts:
        declared = _TS_SCALAR_TYPES.get(h)
        declared_origin = typing.get_origin(declared) or declared
        if isinstance(declared_origin, type) and (
            (dataclasses.is_dataclass(declared_origin)
             and issubclass(declared_origin, CompoundScalar))
            or _is_python_object_class(declared_origin)
        ):
            cs = declared_origin
            t = declared
    if cs is not None:
        schema = getattr(t, "_structured_schema", cs)
        if not hasattr(t, "_structured_schema"):
            schema = t
        return dict(_structured_specialized_field_types(schema))
    raise TypeError(f"fields expects a TSB or compound-scalar type, got {t!r}")


def bundle_schema_type(t):
    """Return the concrete Python schema class associated with a ``TSB``.

    This preserves nominal schema identity when reflecting a bundle; callers
    that need its scalar representation can use
    ``bundle_schema_type(t).to_scalar_schema()``.
    """
    h = _handle(t)
    if not h.is_tsb:
        raise TypeError(f"bundle_schema_type expects a TSB type, got {t!r}")
    schema = _TSB_SCHEMA_CLASSES.get(h)
    if schema is None:
        raise TypeError(f"TSB type {t!r} has no registered Python schema class")
    return schema


def dereference(t):
    """Strip one ``REF[...]`` wrapper. ``dereference(REF[TS[int]]) == TS[int]``.

    A non-reference type is returned unchanged.
    """
    h = _handle(t)
    if not h.is_ref:
        return t
    return _wrap(_m.ref_target(h))


def is_reference(t):
    """``True`` if ``t`` is a ``REF[...]`` type."""
    return _handle(t).is_ref


def is_ts(t):
    """``True`` if ``t`` is a scalar ``TS[...]`` type."""
    return _handle(t).is_ts


def is_tsd(t):
    """``True`` if ``t`` is a ``TSD[...]`` type."""
    return _handle(t).is_tsd


def is_tsl(t):
    """``True`` if ``t`` is a ``TSL[...]`` type."""
    return _handle(t).is_tsl


def is_tss(t):
    """``True`` if ``t`` is a ``TSS[...]`` type."""
    return _handle(t).is_tss


def is_bundle(t):
    """``True`` if ``t`` is a ``TSB`` bundle type."""
    return _handle(t).is_tsb


def is_compound_scalar(t):
    """``True`` for a compound-scalar class or a ``TS`` over either structured scalar policy."""
    import typing

    def structured(value):
        origin = typing.get_origin(value) or value
        return isinstance(origin, type) and (
            (dataclasses.is_dataclass(origin) and issubclass(origin, CompoundScalar))
            or _is_python_object_class(origin)
        )

    if structured(t):
        return True
    try:
        return is_ts(t) and structured(scalar_type(t))
    except TypeError:
        return False


def is_frame(t):
    """``True`` for ``Frame[Row]`` or ``TS[Frame[Row]]``."""
    if isinstance(t, _FrameType):
        return True
    try:
        return is_ts(t) and isinstance(scalar_type(t), _FrameType)
    except TypeError:
        return False


def frame_schema(t):
    """Return the row schema declared by ``Frame[Row]`` or ``TS[Frame[Row]]``."""
    scalar = scalar_type(t) if not isinstance(t, _FrameType) else t
    if not isinstance(scalar, _FrameType):
        raise TypeError(f"frame_schema expects Frame[Row] or TS[Frame[Row]], got {t!r}")
    return scalar.schema


def operator_overloads(operator):
    """Return the decorated implementations registered for an operator.

    The returned callables expose ordinary ``inspect.signature`` annotations.
    A ``@dispatch`` definition's generic fallback is not an overload and is
    therefore excluded.
    """
    entries = getattr(operator, "_overloads", None)
    if entries is None:
        raise TypeError(f"{operator!r} is not an hgraph operator")
    fallback = getattr(operator, "_dispatch_fallback", None)
    return tuple(implementation for implementation, _ in entries
                 if implementation is not fallback)
